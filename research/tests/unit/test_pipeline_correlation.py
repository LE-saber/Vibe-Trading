from datetime import date, datetime
import json

import pandas as pd
import pytest

from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.correlation import build_return_correlation
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext


REFERENCE = date(2026, 9, 1)
CONTEXT = ResearchContext(
    research_date=date(2026, 9, 2),
    phase="intraday",
    market_data_reference_date=REFERENCE,
    as_of=datetime.fromisoformat("2026-09-02T10:00:00+08:00"),
)
CONFIG = load_pipeline_config()
SESSIONS = pd.bdate_range(end=REFERENCE.isoformat(), periods=61).strftime("%Y-%m-%d").tolist()


def _closes(returns: list[float], start: float = 100.0) -> list[float]:
    values = [start]
    for value in returns:
        values.append(values[-1] * (1.0 + value))
    return values


def _panel(closes_by_symbol: dict[str, list[float]], *, raw_close: float = 999_999.0) -> pd.DataFrame:
    rows = []
    for symbol, closes in closes_by_symbol.items():
        assert len(closes) == len(SESSIONS)
        rows.extend(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "close_qfq": close,
                "close": raw_close,
            }
            for trade_date, close in zip(SESSIONS, closes)
        )
    return pd.DataFrame(rows)


def _build(panel: pd.DataFrame, symbols: list[str]):
    return build_return_correlation(panel, symbols=symbols, sessions=SESSIONS, context=CONTEXT, config=CONFIG)


def _expected_spearman(left: list[float], right: list[float]) -> float:
    left_rank = pd.Series(left, dtype=float).rank(method="average")
    right_rank = pd.Series(right, dtype=float).rank(method="average")
    return float(left_rank.corr(right_rank))


def test_monotonic_negative_spearman_is_a_redundancy_edge_and_window_is_exact() -> None:
    increasing = [0.001 + index * 0.0001 for index in range(60)]
    decreasing = list(reversed(increasing))
    left, right = "600000.SH", "300750.SZ"
    result = _build(_panel({left: _closes(increasing), right: _closes(decreasing)}), [left, right])

    assert result["symbols"] == [right, left]
    assert result["method"] == "spearman_pairwise_complete"
    assert result["window_sessions"] == 60
    assert result["reference_date"] == REFERENCE.isoformat()
    assert result["window_start_date"] == SESSIONS[0]
    assert result["window_end_date"] == SESSIONS[-1]
    assert result["return_dates"] == SESSIONS[1:]
    assert result["observation_counts"][left][right] == 60
    assert result["correlation_matrix"][left][right] == pytest.approx(-1.0)
    assert result["high_correlation_pairs"] == [
        {"left": right, "right": left, "correlation": pytest.approx(-1.0), "observations": 60}
    ]
    assert result["clusters"] == [{"cluster_id": 1, "symbols": [right, left]}]
    json.dumps(result, allow_nan=False)


def test_spearman_uses_average_ties_and_not_raw_return_pearson() -> None:
    left_pattern = [0.01, 0.01, 0.02, 0.03, 0.03, 0.04]
    right_pattern = [0.04, 0.01, 0.02, 0.03, 0.04, 0.04]
    left_returns = left_pattern * 10
    right_returns = right_pattern * 10
    left, right = "600000.SH", "300750.SZ"
    result = _build(_panel({left: _closes(left_returns), right: _closes(right_returns)}), [left, right])

    expected = _expected_spearman(left_returns, right_returns)
    pearson = float(pd.Series(left_returns).corr(pd.Series(right_returns)))
    actual = result["correlation_matrix"][left][right]
    assert actual == pytest.approx(expected)
    assert actual != pytest.approx(pearson)


def test_missing_or_nonpositive_close_does_not_bridge_returns() -> None:
    returns = [0.001 + index * 0.0001 for index in range(60)]
    left, right = "600000.SH", "300750.SZ"
    left_closes = _closes(returns)
    left_closes[30] = 0.0
    result = _build(_panel({left: left_closes, right: _closes(returns)}), [left, right])

    assert result["observed_return_counts"][left] == 58
    assert result["observation_counts"][left][right] == 58
    assert result["correlation_matrix"][left][right] == pytest.approx(1.0)


def test_constant_returns_are_unknown_and_do_not_form_an_edge() -> None:
    left, right = "300750.SZ", "600000.SH"
    returns = [0.001 + index * 0.0001 for index in range(60)]
    result = _build(_panel({left: [100.0] * 61, right: _closes(returns)}), [right, left])

    assert result["correlation_matrix"][left][left] is None
    assert result["correlation_matrix"][left][right] is None
    assert result["observation_counts"][left][right] == 60
    assert result["missing_pairs"] == [
        {"left": left, "right": right, "reason": "constant_returns", "observations": 60}
    ]
    assert result["clusters"] == [
        {"cluster_id": 1, "symbols": [left]},
        {"cluster_id": 2, "symbols": [right]},
    ]


def test_insufficient_pair_overlap_is_unknown_not_zero() -> None:
    left, right = "300750.SZ", "600000.SH"
    returns = [0.001 + index * 0.0001 for index in range(60)]
    right_closes = _closes(returns)[21:]
    rows = _panel({left: _closes(returns)})
    rows = pd.concat([rows, pd.DataFrame(
        {
            "symbol": right,
            "trade_date": SESSIONS[21:],
            "close_qfq": right_closes,
            "close": 999_999.0,
        }
    )], ignore_index=True)
    result = _build(rows, [left, right])

    assert result["observed_return_counts"][right] == 39
    assert result["observation_counts"][left][right] == 39
    assert result["correlation_matrix"][left][right] is None
    assert result["missing_pairs"] == [
        {"left": left, "right": right, "reason": "insufficient_observations", "observations": 39}
    ]
    assert all(value is None for value in result["correlation_matrix"][right].values())


def test_full_canonical_panel_is_filtered_after_validating_other_symbols() -> None:
    returns = [0.001 + index * 0.0001 for index in range(60)]
    requested = ["600000.SH", "300750.SZ"]
    extra = "600519.SH"
    result = _build(
        _panel({requested[0]: _closes(returns), requested[1]: _closes(returns), extra: _closes(returns)}),
        requested,
    )

    assert result["symbols"] == sorted(requested)
    assert extra not in result["correlation_matrix"]
    assert result["clusters"] == [{"cluster_id": 1, "symbols": sorted(requested)}]


def test_future_and_duplicate_inputs_fail_closed() -> None:
    returns = [0.001 + index * 0.0001 for index in range(60)]
    left, right = "600000.SH", "300750.SZ"
    panel = _panel({left: _closes(returns), right: _closes(returns)})

    future = panel.copy()
    future.loc[0, "trade_date"] = "2026-09-02"
    with pytest.raises(PipelineError, match="future"):
        _build(future, [left, right])

    duplicate = pd.concat([panel, panel.iloc[[0]]], ignore_index=True)
    with pytest.raises(PipelineError, match="duplicate"):
        _build(duplicate, [left, right])

    with pytest.raises(PipelineError, match="duplicates"):
        _build(panel, [left, left])


def test_panel_and_symbol_reordering_has_identical_output() -> None:
    returns = [0.001 + index * 0.0001 for index in range(60)]
    symbols = ["600000.SH", "300750.SZ", "600519.SH"]
    panel = _panel({symbol: _closes(returns) for symbol in symbols})
    baseline = _build(panel, symbols)
    reordered = _build(panel.sample(frac=1.0, random_state=11).reset_index(drop=True), list(reversed(symbols)))

    assert reordered == baseline

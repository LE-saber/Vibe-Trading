"""Deterministic scoring tests on explicitly synthetic market inputs."""

import numpy as np
import pandas as pd
import pytest

from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.features import base_features, supplemental_features
from competition_core.pipeline.scoring import percentile, score_stage, feature_groups
from tests.pipeline_fixtures import context, small_config, synthetic_panel, technical_rows


def features(panel=None):
    original, sessions, symbols = synthetic_panel()
    panel = original if panel is None else panel
    return base_features(panel, context=context(), sessions=sessions, symbols=symbols,
                         technical_rows=technical_rows(panel, sessions, symbols))


def test_percentile_ties_missing_neutral_and_direction():
    result = percentile(pd.Series([10., 20., 20., np.nan, 30.]))
    assert result.dropna().tolist() == [0, 50, 50, 100]
    assert pd.isna(result.iloc[3])
    assert percentile(pd.Series([3., 3., np.nan])).dropna().tolist() == [50, 50]
    assert percentile(pd.Series([np.nan, 3.])).dropna().tolist() == [50]
    assert percentile(pd.Series([1., 2.]), -1).tolist() == [100, 0]
    assert percentile(pd.Series([np.nan, np.inf])).isna().all()


def test_qfq_features_cross_exdate_preserve_actual_volume_amount():
    panel, sessions, _ = synthetic_panel()
    before = panel.copy(deep=True)
    row = features(panel).loc["300750.SZ"]
    stock = panel.loc[panel.symbol == "300750.SZ"].set_index("trade_date")
    assert stock.adj_factor.nunique() == 2
    assert row.momentum_5d == pytest.approx(stock.close_qfq.iloc[-1] / stock.close_qfq.iloc[-6] - 1)
    assert abs(row.momentum_5d - (stock.close_raw.iloc[-1] / stock.close_raw.iloc[-6] - 1)) > .4
    assert row.latest_volume_raw == stock.volume_raw.iloc[-1]
    assert row.latest_volume_shares == stock.volume_raw.iloc[-1] * 100
    assert row.latest_amount_cny == stock.amount_raw.iloc[-1] * 1000
    pd.testing.assert_frame_equal(panel, before)


def test_suspension_gap_is_not_compressed_or_forward_filled():
    panel, sessions, _ = synthetic_panel()
    panel = panel.loc[~((panel.symbol == "300750.SZ") & (panel.trade_date == sessions[-7]))]
    row = features(panel).loc["300750.SZ"]
    stock = panel.loc[panel.symbol == "300750.SZ"].set_index("trade_date").close_qfq.reindex(sessions)
    assert row.history_sessions == 119
    assert row.momentum_20d == pytest.approx(stock.iloc[-1] / stock.iloc[-21] - 1)
    assert pd.isna(row.volatility_20d) and pd.isna(row.atr14_ratio) and pd.isna(row.sma_20)
    assert row.technical_contiguous_sessions == 6


def test_missing_reference_factor_price_never_falls_back_to_raw():
    panel, sessions, _ = synthetic_panel()
    mask = (panel.symbol == "300750.SZ") & (panel.trade_date == sessions[-1])
    panel.loc[mask, ["adj_factor", "open_qfq", "high_qfq", "low_qfq", "close_qfq"]] = np.nan
    scored = score_stage(features(panel), small_config(), stage="S1")
    row = next(row for row in scored["rows"] if row["symbol"] == "300750.SZ")
    assert not row["selected"] and "missing_reference_qfq_close" in row["reject_reasons"]
    assert row["quant_score"] is None and row["features"]["latest_close_raw"] > 0


def test_missing_components_renormalize_and_high_rsi_does_not_delete():
    data = features()
    for name in feature_groups("S1")["liquidity"]:
        data[name] = np.nan
    result = score_stage(data, small_config(), stage="S1")
    assert len(result["output_symbols"]) == 12
    for row in result["rows"]:
        assert row["liquidity_score"] is None
        assert row["effective_weights"]["liquidity"] is None
        assert row["effective_weights"]["trend"] == pytest.approx(.25 / .85)
        assert row["features"]["rsi_14"] == 95
        assert row["overheat_score"] is not None
        assert sum(value for value in row["effective_weights"].values() if value is not None) == pytest.approx(1)


def test_score_ties_and_shuffled_inputs_are_exactly_deterministic():
    data = features()
    for fields in feature_groups("S1").values():
        for name in fields:
            data[name] = 1.0
    original = score_stage(data, small_config(), stage="S1")
    repeated = score_stage(data.sample(frac=1, random_state=7), small_config(), stage="S1")
    assert original == repeated
    assert original["output_symbols"] == sorted(data.symbol)[:12]
    assert all(row["quant_score"] == pytest.approx(50) for row in original["rows"])


def test_missing_technical_for_one_symbol_does_not_remove_other_candidates():
    data = features()
    for fields in (feature_groups("S1")["trend"],):
        for field in fields:
            data.loc["300750.SZ", field] = np.nan
    result = score_stage(data, small_config(), stage="S1")
    catl = next(row for row in result["rows"] if row["symbol"] == "300750.SZ")
    assert catl["trend_score"] is None and catl["quant_score"] is not None
    assert catl["effective_weights"]["momentum"] == pytest.approx(.25 / .75)
    assert result["summary"]["scorable_count"] == 15


@pytest.mark.parametrize("fault", ["duplicate", "future", "wrong_identity"])
def test_invalid_panel_rejected(fault):
    panel, sessions, _ = synthetic_panel()
    if fault == "duplicate":
        panel = pd.concat([panel, panel.iloc[:1]], ignore_index=True)
    elif fault == "future":
        panel.loc[0, "trade_date"] = "2026-09-02"
    else:
        panel.loc[0, "symbol"] = "300750.SH"
    with pytest.raises((PipelineError, ValueError)):
        features(panel)


def supplements():
    panel, sessions, symbols = synthetic_panel()
    data = features(panel)
    benchmark = {"status": "SUCCESS", "data": [dict(ts_code="000300.SH", trade_date=day.replace("-", ""), close=100 + i)
                                                  for i, day in enumerate(sessions)]}
    limits = []
    raw = panel.set_index(["symbol", "trade_date"])
    for day in sessions[-5:]:
        rows = []
        for symbol in symbols:
            price = raw.loc[(symbol, day), "close_raw"]
            rows.append(dict(ts_code=symbol, trade_date=day.replace("-", ""), up_limit=price if day == sessions[-1] else price * 1.2,
                             down_limit=price * .8))
        limits.append({"status": "SUCCESS", "data": rows})
    return panel, sessions, data, benchmark, limits


def test_s2_uses_dated_benchmark_and_actual_limit_prices_without_industry_guess():
    panel, sessions, data, benchmark, limits = supplements()
    expanded = supplemental_features(data, panel, context=context(), sessions=sessions, benchmark_symbol="000300.SH",
                                     benchmark=benchmark, limit_batches=limits, limit_sessions=5)
    catl = expanded.loc["300750.SZ"]
    assert catl.recent_limit_up_count == 1 and catl.latest_limit_up == 1
    assert catl.benchmark_return_20d == pytest.approx(219 / 199 - 1)
    assert catl.benchmark_relative_20d == pytest.approx((1 + catl.momentum_20d) / (219 / 199) - 1)
    result = score_stage(expanded, small_config(), stage="S2")
    assert len(result["output_symbols"]) == 12
    assert all("sector_relative_20d" in row["missing_fields"] for row in result["rows"])


def test_missing_benchmark_limit_query_remains_unknown_not_false_zero():
    panel, sessions, data, _, _ = supplements()
    expanded = supplemental_features(data, panel, context=context(), sessions=sessions, benchmark_symbol="000300.SH",
                                     benchmark={"status": "MISSING"}, limit_batches=[{"status": "MISSING"}], limit_sessions=5)
    assert expanded.benchmark_relative_20d.isna().all()
    assert expanded.latest_limit_up.isna().all() and expanded.recent_limit_up_count.isna().all()
    assert score_stage(expanded, small_config(), stage="S2")["summary"]["selected_count"] == 12


@pytest.mark.parametrize("key,value", [("benchmark_symbol", "600519.SH"), ("limit_lookback_sessions", 0),
                                      ("technical_timeout_seconds", 999), ("minimum_available_weight", float("nan"))])
def test_screening_config_rejects_invalid_safety_parameters(key, value):
    payload = small_config().to_dict()
    payload["screening"][key] = value
    with pytest.raises(PipelineError):
        PipelineConfig.from_dict(payload)

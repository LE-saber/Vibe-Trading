from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
from typing import Any

import pandas as pd
import pytest

from adapters.vibe import technical
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext


CORE_ROOT = Path(__file__).resolve().parents[2]
VIBE_ROOT = CORE_ROOT
REFERENCE = date(2026, 9, 1)
CONTEXT = ResearchContext(
    research_date=date(2026, 9, 2),
    phase="intraday",
    market_data_reference_date=REFERENCE,
    as_of=datetime.fromisoformat("2026-09-02T10:00:00+08:00"),
)
SYMBOLS = ["600000.SH", "300750.SZ"]
SESSIONS = [
    (REFERENCE - timedelta(days=offset)).isoformat()
    for offset in range(39, -1, -1)
]
TECHNICAL_FIELDS = (
    "sma_5",
    "sma_10",
    "sma_20",
    "sma_20_lag5",
    "rsi_14",
    "macd_histogram",
    "bollinger_upper",
    "bollinger_middle",
    "bollinger_lower",
)


def _panel(
    symbols: list[str] = SYMBOLS,
    *,
    missing: set[tuple[str, str]] | None = None,
    raw_offset: float = 10_000.0,
) -> pd.DataFrame:
    missing = missing or set()
    rows: list[dict[str, Any]] = []
    for symbol_index, symbol in enumerate(symbols):
        for session_index, trade_date in enumerate(SESSIONS):
            if (symbol, trade_date) in missing:
                continue
            close_qfq = float(10 + symbol_index * 100 + session_index)
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "close_qfq": close_qfq,
                    "rawclose": close_qfq + raw_offset,
                }
            )
    return pd.DataFrame(rows)


def _compute_with_adapter(
    panel: pd.DataFrame,
    *,
    symbols: list[str] = SYMBOLS,
    work_dir: Path,
) -> dict[str, Any]:
    return technical.compute_technical(
        panel,
        context=CONTEXT,
        sessions=SESSIONS,
        symbols=symbols,
        work_dir=work_dir,
        vibe_root=VIBE_ROOT,
    )


def _vibe_helper_values(values: list[float]) -> dict[str, Any]:
    from competition_core.technical_indicators import (
        _compute_bollinger,
        _compute_macd,
        _compute_rsi,
        _compute_sma,
    )
    close = pd.Series(values, dtype=float)
    bands = _compute_bollinger(close.iloc[-20:], 20)
    return {
        "sma_5": _compute_sma(close.iloc[-5:], 5),
        "sma_10": _compute_sma(close.iloc[-10:], 10),
        "sma_20": _compute_sma(close.iloc[-20:], 20),
        "sma_20_lag5": _compute_sma(close.iloc[-25:-5], 20),
        "rsi_14": _compute_rsi(close),
        "macd_histogram": _compute_macd(close)["histogram"],
        "bollinger_upper": bands["upper"],
        "bollinger_middle": bands["middle"],
        "bollinger_lower": bands["lower"],
    }


def _worker_module() -> Any:
    path = CORE_ROOT / "scripts" / "vibe-technical-worker.py"
    spec = importlib.util.spec_from_file_location("p10c_vibe_technical_worker", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_worker_direct(
    tmp_path: Path,
    *,
    csv_body: str,
    input_hash: str | None = None,
    symbols: list[str] = SYMBOLS,
    sessions: list[str] = SESSIONS,
) -> tuple[int, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    input_path = tmp_path / "input.csv"
    input_path.write_text(csv_body, encoding="utf-8", newline="")
    actual_hash = hashlib.sha256(input_path.read_bytes()).hexdigest()
    request = {
        "context": CONTEXT.to_dict(),
        "sessions": sessions,
        "symbols": symbols,
        "input_csv": input_path.name,
        "input_sha256": actual_hash if input_hash is None else input_hash,
    }
    request_path = tmp_path / "request.json"
    output_path = tmp_path / "output.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    worker = _worker_module()
    return (
        worker.main(
            [
                "--request",
                str(request_path),
                "--output",
                str(output_path),
                "--vibe-root",
                str(VIBE_ROOT),
            ]
        ),
        output_path,
    )


def test_real_vibe_worker_matches_original_helpers_and_sorts_symbols(tmp_path: Path) -> None:
    panel = _panel()
    result = _compute_with_adapter(panel, work_dir=tmp_path)
    rows = {row["symbol"]: row for row in result["rows"]}
    assert [row["symbol"] for row in result["rows"]] == sorted(SYMBOLS)
    assert rows[SYMBOLS[0]]["contiguous_sessions"] == len(SESSIONS)
    expected = _vibe_helper_values(
        [float(10 + session_index) for session_index in range(len(SESSIONS))]
    )
    for field in TECHNICAL_FIELDS:
        assert rows[SYMBOLS[0]][field] == pytest.approx(expected[field])
    assert rows[SYMBOLS[0]]["missing_fields"] == []
    assert len(result["provenance"]["worker_sha256"]) == 64
    assert len(result["provenance"]["helper_sha256"]) == 64
    assert result["provenance"]["context"] == CONTEXT.to_dict()


def test_qfq_is_the_only_price_column_used(tmp_path: Path) -> None:
    panel = _panel(symbols=[SYMBOLS[0]], raw_offset=1_000_000.0)
    result = _compute_with_adapter(panel, symbols=[SYMBOLS[0]], work_dir=tmp_path)
    row = result["rows"][0]
    expected = _vibe_helper_values(
        [float(10 + session_index) for session_index in range(len(SESSIONS))]
    )
    assert row["sma_20"] == pytest.approx(expected["sma_20"])
    assert row["bollinger_middle"] == pytest.approx(expected["bollinger_middle"])
    assert row["sma_20"] != pytest.approx(1_000_000.0)


def test_negative_and_zero_indicators_are_valid_not_missing(tmp_path: Path) -> None:
    panel = _panel()
    for symbol, closing in ((SYMBOLS[0], [100. - i for i in range(len(SESSIONS))]),
                            (SYMBOLS[1], [10.] * len(SESSIONS))):
        panel.loc[panel.symbol == symbol, "close_qfq"] = closing
    result = _compute_with_adapter(panel, work_dir=tmp_path)
    rows = {row["symbol"]: row for row in result["rows"]}
    expected = _vibe_helper_values([100. - i for i in range(len(SESSIONS))])
    assert expected["macd_histogram"] < 0
    assert rows[SYMBOLS[0]]["macd_histogram"] == expected["macd_histogram"]
    assert rows[SYMBOLS[0]]["rsi_14"] == 0
    assert rows[SYMBOLS[1]]["macd_histogram"] == 0
    assert rows[SYMBOLS[0]]["missing_fields"] == rows[SYMBOLS[1]]["missing_fields"] == []


def test_gap_is_not_compressed_and_only_affected_stock_is_missing(tmp_path: Path) -> None:
    gap_date = SESSIONS[-8]
    panel = _panel(missing={(SYMBOLS[0], gap_date)})
    result = _compute_with_adapter(panel, work_dir=tmp_path)
    rows = {row["symbol"]: row for row in result["rows"]}
    broken = rows[SYMBOLS[0]]
    healthy = rows[SYMBOLS[1]]
    assert broken["contiguous_sessions"] == 7
    assert broken["sma_5"] is not None
    assert broken["sma_10"] is None
    assert broken["sma_20"] is None
    assert broken["bollinger_middle"] is None
    assert broken["rsi_14"] is None
    assert broken["macd_histogram"] is None
    assert {"sma_10", "sma_20", "rsi_14"} <= set(broken["missing_fields"])
    assert "missing_close_window" in result["provenance"]["missing_reasons"][SYMBOLS[0]].values()
    assert healthy["missing_fields"] == []


def test_one_stock_without_any_rows_gets_missing_values_only_for_that_stock(tmp_path: Path) -> None:
    result = _compute_with_adapter(_panel(symbols=[SYMBOLS[0]]), work_dir=tmp_path)
    rows = {row["symbol"]: row for row in result["rows"]}
    assert rows[SYMBOLS[0]]["missing_fields"] == []
    assert rows[SYMBOLS[1]]["contiguous_sessions"] == 0
    assert all(rows[SYMBOLS[1]][field] is None for field in TECHNICAL_FIELDS)
    assert set(rows[SYMBOLS[1]]["missing_fields"]) == set(TECHNICAL_FIELDS)
    assert rows[SYMBOLS[0]]["sma_20"] is not None


def test_core_rejects_future_and_duplicate_panel_rows_before_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        technical.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("worker must not start"),
    )
    future = _panel()
    future.loc[0, "trade_date"] = "2026-09-02"
    with pytest.raises(PipelineError, match="future"):
        _compute_with_adapter(future, work_dir=tmp_path)

    duplicate = pd.concat([_panel(), _panel().iloc[[0]]], ignore_index=True)
    with pytest.raises(PipelineError, match="duplicate"):
        _compute_with_adapter(duplicate, work_dir=tmp_path)

    outside_sessions = _panel()
    outside_sessions.loc[0, "trade_date"] = "2026-01-01"
    with pytest.raises(PipelineError, match="outside sessions"):
        _compute_with_adapter(outside_sessions, work_dir=tmp_path)

    with pytest.raises(PipelineError, match="invalid"):
        _compute_with_adapter(_panel(), symbols=["not-a-symbol"], work_dir=tmp_path)


def test_worker_rejects_invalid_hash_future_date_and_duplicate(tmp_path: Path) -> None:
    valid_row = f"{SYMBOLS[0]},{SESSIONS[-1]},10\n"
    status, output = _run_worker_direct(
        tmp_path / "hash",
        csv_body="symbol,trade_date,close_qfq\n" + valid_row,
        input_hash="0" * 64,
        symbols=[SYMBOLS[0]],
    )
    assert status != 0
    assert not output.exists()

    future_dir = tmp_path / "future"
    future_dir.mkdir()
    status, _ = _run_worker_direct(
        future_dir,
        csv_body=f"symbol,trade_date,close_qfq\n{SYMBOLS[0]},2026-09-02,10\n",
        symbols=[SYMBOLS[0]],
    )
    assert status != 0

    duplicate_dir = tmp_path / "duplicate"
    duplicate_dir.mkdir()
    status, _ = _run_worker_direct(
        duplicate_dir,
        csv_body=f"symbol,trade_date,close_qfq\n{valid_row}{valid_row}",
        symbols=[SYMBOLS[0]],
    )
    assert status != 0

    outside_dir = tmp_path / "outside-sessions"
    outside_dir.mkdir()
    status, _ = _run_worker_direct(
        outside_dir,
        csv_body=f"symbol,trade_date,close_qfq\n{SYMBOLS[0]},2026-01-01,10\n",
        symbols=[SYMBOLS[0]],
    )
    assert status != 0


def test_non_positive_qfq_is_missing_in_core_and_worker(tmp_path: Path) -> None:
    panel = _panel(symbols=[SYMBOLS[0]])
    panel.loc[panel["trade_date"] == SESSIONS[-1], "close_qfq"] = 0
    normalized = technical._validate_panel(panel, [SYMBOLS[0]], SESSIONS, CONTEXT)
    assert normalized[-1][2] is None

    rows = "\n".join(
        f"{SYMBOLS[0]},{trade_date},{0 if trade_date == SESSIONS[-1] else 10}"
        for trade_date in SESSIONS
    )
    status, output = _run_worker_direct(
        tmp_path,
        csv_body="symbol,trade_date,close_qfq\n" + rows + "\n",
        symbols=[SYMBOLS[0]],
    )
    assert status == 0
    row = json.loads(output.read_text(encoding="utf-8"))["rows"][0]
    assert row["sma_5"] is None
    assert row["sma_5"] != 0


def test_timeout_is_structured_without_stdout_or_exception_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def timed_out(*_args: Any, **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired("vibe-technical-worker", kwargs["timeout"])

    monkeypatch.setattr(technical.subprocess, "run", timed_out)
    with pytest.raises(PipelineError, match="timed out") as error:
        _compute_with_adapter(_panel(), work_dir=tmp_path)
    assert error.value.code == "TIMEOUT"

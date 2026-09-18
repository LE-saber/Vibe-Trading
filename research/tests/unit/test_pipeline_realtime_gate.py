from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
import math
from zoneinfo import ZoneInfo

import pytest

from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.realtime_gate import evaluate_realtime_quotes
from competition_core.pipeline.time_context import ResearchContext


SHANGHAI = ZoneInfo("Asia/Shanghai")
RESEARCH_DATE = date(2026, 9, 2)
OBSERVED = datetime(2026, 9, 2, 10, 0, 5, tzinfo=SHANGHAI)
SYMBOLS = ["600519.SH", "300750.SZ"]
CONFIG = load_pipeline_config()
_AUTO_AMOUNT = object()


def context(
    *,
    phase: str = "intraday",
    as_of: datetime | None = None,
) -> ResearchContext:
    if phase == "postclose":
        default_as_of = datetime(2026, 9, 2, 15, tzinfo=SHANGHAI)
        reference = RESEARCH_DATE
    else:
        default_as_of = datetime(2026, 9, 2, 9, tzinfo=SHANGHAI)
        reference = date(2026, 9, 1)
    return ResearchContext(
        research_date=RESEARCH_DATE,
        phase=phase,
        market_data_reference_date=reference,
        as_of=as_of or default_as_of,
    )


def quote(
    symbol: str = SYMBOLS[0],
    *,
    server_time: datetime | str = datetime(2026, 9, 2, 10, tzinfo=SHANGHAI),
    trade_date: str | date | None = "2026-09-02",
    timestamp_quality: str | None = "verified",
    price: float = 10.0,
    prev_close: float = 9.5,
    opening: float = 9.8,
    high: float = 10.2,
    low: float = 9.7,
    bid: float | None = 9.99,
    ask: float | None = 10.01,
    volume: float | None = 10.0,
    volume_unit: str | None = "lots",
    amount: float | None | object = _AUTO_AMOUNT,
    amount_unit: str | None = "CNY",
    source: str | None = "fixture",
    received_at: datetime | str | None = datetime(2026, 9, 2, 10, 0, 1, tzinfo=SHANGHAI),
    **extra,
) -> dict:
    result = {
        "symbol": symbol,
        "server_time": server_time,
        "trade_date": trade_date,
        "timestamp_quality": timestamp_quality,
        "timestamp_source": "fixture.server_time",
        "received_at": received_at,
        "price": price,
        "prev_close": prev_close,
        "open": opening,
        "high": high,
        "low": low,
        "bid1": bid,
        "ask1": ask,
        "volume": volume,
        "volume_unit": volume_unit,
        "amount": (
            amount if amount is not _AUTO_AMOUNT else (
            volume * (100 if volume_unit == "lots" else 1) * price
            if volume is not None and volume_unit in {"lots", "shares"}
            else None
            )
        ),
        "amount_unit": amount_unit,
        "source": source,
        "stale": False,
        "fallback_used": False,
    }
    result.update(extra)
    return result


def evaluate(
    quotes: list[dict],
    *,
    symbols: list[str] = SYMBOLS,
    observed_at: datetime = OBSERVED,
    phase: str = "intraday",
    benchmark_quote: dict | None = None,
) -> dict:
    return evaluate_realtime_quotes(
        symbols=symbols,
        quotes=quotes,
        context=context(phase=phase),
        config=CONFIG,
        observed_at=observed_at,
        snapshot_id="snapshot-1",
        benchmark_quote=benchmark_quote,
    )


def test_normal_quote_preserves_order_computes_metrics_and_vwap() -> None:
    first = quote(SYMBOLS[0])
    second = quote(SYMBOLS[1], price=20, prev_close=19, opening=19.5, high=20.5, low=19.2)
    result = evaluate([second, first])

    assert [row["symbol"] for row in result["rows"]] == SYMBOLS
    row = result["rows"][0]
    assert row["realtime_gate"] == "PASS"
    assert row["execution_eligible"] is True
    assert result["output_symbols"] == SYMBOLS
    assert row["metrics"]["volume_shares"] == 1000
    assert row["metrics"]["session_vwap"] == 10
    assert row["metrics"]["change_vs_prev_close"] == pytest.approx(10 / 9.5 - 1)
    assert row["metrics"]["change_vs_open"] == pytest.approx(10 / 9.8 - 1)
    assert row["metrics"]["gap"] == pytest.approx(9.8 / 9.5 - 1)
    assert row["metrics"]["position_in_intraday_range"] == pytest.approx((10 - 9.7) / (10.2 - 9.7))
    assert row["metrics"]["drawdown_from_high"] == pytest.approx((10.2 - 10) / 10.2)
    assert row["metrics"]["recovery_from_low"] == pytest.approx(10 / 9.7 - 1)
    assert row["quote"]["price"] == 10.0
    assert row["quote"]["bid1"] == 9.99
    assert row["meta"]["research_date"] == "2026-09-02"
    assert row["meta"]["fallback_used"] is False
    assert result["stage"] == "S5_5"
    assert row["selected"] is True
    assert row["meta"]["market_data_reference_date"] == "2026-09-01"
    assert result["status"] == "SUCCESS"
    assert result["counts"]["PASS"] == 2


def test_benchmark_relative_strength_is_optional_and_requires_same_verified_time() -> None:
    benchmark = quote("000300.SH", price=1.01, prev_close=1, opening=1, high=1.02, low=0.99)
    result = evaluate([quote()], benchmark_quote=benchmark)
    metric = result["rows"][0]["metrics"]["intraday_relative_strength"]
    assert metric == pytest.approx((10 / 9.5 - 1) - (1.01 - 1))
    assert "intraday_relative_strength" not in result["rows"][0]["missing_fields"]

    without = evaluate([quote()])
    assert without["rows"][0]["metrics"]["intraday_relative_strength"] is None
    assert "intraday_relative_strength" in without["missing_fields"]

    mismatched = quote("benchmark", server_time="2026-09-02T09:59:59+08:00",
                       price=1.01, prev_close=1, opening=1, high=1.02, low=0.99)
    result = evaluate([quote()], benchmark_quote=mismatched)
    assert result["rows"][0]["metrics"]["intraday_relative_strength"] is None
    assert "intraday_relative_strength" in result["rows"][0]["missing_fields"]


def test_stale_and_future_quotes_fail_closed_without_using_context_as_of() -> None:
    stale = quote(server_time="2026-09-02T09:58:00+08:00",
                  received_at="2026-09-02T09:58:01+08:00")
    result = evaluate([stale])
    assert result["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert result["rows"][0]["metrics"]["quote_age_seconds"] == pytest.approx(125)
    assert result["rows"][0]["execution_eligible"] is False

    small_clock_skew = quote(server_time="2026-09-02T10:00:12+08:00",
                             received_at="2026-09-02T10:00:05+08:00")
    result = evaluate([small_clock_skew])
    assert result["rows"][0]["realtime_gate"] == "PASS"
    assert result["rows"][0]["metrics"]["quote_age_seconds"] == 0
    assert result["rows"][0]["execution_eligible"] is True

    future = quote(server_time="2026-09-02T10:00:16+08:00",
                   received_at="2026-09-02T10:00:05+08:00")
    result = evaluate([future])
    assert result["rows"][0]["realtime_gate"] == "REJECT"
    assert result["rows"][0]["execution_eligible"] is False
    assert "future_server_time" in result["rows"][0]["reject_reasons"]


@pytest.mark.parametrize(
    "server_time,trade_date,quality",
    [
        ("09:30:00", "2026-09-02", "time_only"),
        ("2026-09-01T10:00:00+08:00", "2026-09-01", "verified"),
        ("2026-09-02T10:00:00+08:00", "2026-09-01", "verified"),
        ("2026-09-02T10:00:00", "2026-09-02", "verified"),
    ],
)
def test_time_date_and_quality_mismatches_are_missing_realtime(
    server_time: str, trade_date: str, quality: str
) -> None:
    result = evaluate([quote(server_time=server_time, trade_date=trade_date,
                             timestamp_quality=quality)])
    row = result["rows"][0]
    assert row["realtime_gate"] == "MISSING_REALTIME"
    assert row["execution_eligible"] is False
    assert row["metrics"]["change_vs_prev_close"] == pytest.approx(10 / 9.5 - 1)


@pytest.mark.parametrize(
    "clock,expected_gate,eligible",
    [
        ("09:29:59", "DOWNGRADE", False),
        ("09:30:00", "PASS", True),
        ("11:29:59", "PASS", True),
        ("11:30:00", "DOWNGRADE", False),
        ("13:00:00", "PASS", True),
        ("14:56:59", "PASS", True),
        ("14:57:00", "DOWNGRADE", False),
    ],
)
def test_continuous_auction_boundaries_are_precise(
    clock: str, expected_gate: str, eligible: bool
) -> None:
    observed = datetime.fromisoformat(f"2026-09-02T{clock}+08:00")
    result = evaluate([quote(server_time=observed, received_at=observed)], observed_at=observed)
    row = result["rows"][0]
    assert row["realtime_gate"] == expected_gate
    assert row["execution_eligible"] is eligible


def test_preopen_phase_is_data_only() -> None:
    result = evaluate([quote()], phase="preopen")
    assert result["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert result["rows"][0]["execution_eligible"] is False
    assert "outside_continuous_auction" in result["rows"][0]["reasons"]


def test_postclose_phase_is_data_only() -> None:
    observed = datetime(2026, 9, 2, 15, 0, tzinfo=SHANGHAI)
    result = evaluate(
        [quote(server_time=observed, received_at=observed)],
        phase="postclose",
        observed_at=observed,
    )
    assert result["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert result["rows"][0]["execution_eligible"] is False
    assert "outside_continuous_auction" in result["rows"][0]["reasons"]


def test_zero_or_missing_one_sided_book_is_not_treated_as_safe() -> None:
    zero = evaluate([quote(bid=0)])
    assert zero["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert zero["rows"][0]["execution_eligible"] is False
    assert "zero_bid" in zero["rows"][0]["reasons"]

    missing = quote()
    del missing["bid1"]
    result = evaluate([missing])
    assert result["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert "bid" in result["rows"][0]["missing_fields"]
    assert result["rows"][0]["metrics"]["bid_ask_spread"] is None


def test_crossed_or_conflicting_book_fields_fail_closed() -> None:
    crossed = evaluate([quote(bid=10.01, ask=10.0)])
    assert crossed["rows"][0]["realtime_gate"] == "REJECT"
    assert "crossed_bid_ask" in crossed["rows"][0]["reject_reasons"]

    conflicting = quote()
    conflicting["bid"] = 9.98
    conflicting["bid1"] = 9.99
    result = evaluate([conflicting])
    assert result["rows"][0]["realtime_gate"] == "REJECT"
    assert "bid_alias_conflict" in result["rows"][0]["reject_reasons"]


def test_gap_and_drawdown_use_absolute_gap_and_strictly_exceeding_limits() -> None:
    at_limit = quote(prev_close=100, opening=108, high=108, price=101.52, low=100)
    result = evaluate([at_limit])
    assert result["rows"][0]["realtime_gate"] == "PASS"

    above_gap = quote(prev_close=100, opening=108.01, high=108.01, price=108.01, low=100)
    assert evaluate([above_gap])["rows"][0]["realtime_gate"] == "DOWNGRADE"

    above_drawdown = quote(prev_close=100, opening=100, high=100, price=93.99, low=93.99)
    assert evaluate([above_drawdown])["rows"][0]["realtime_gate"] == "DOWNGRADE"


def test_lot_normalization_and_input_are_unchanged() -> None:
    original = quote(volume=12, volume_unit="lots", amount=1_200)
    before = deepcopy(original)
    result = evaluate([original])
    assert original == before
    assert result["rows"][0]["quote"]["volume"] == 12
    assert result["rows"][0]["metrics"]["volume_shares"] == 1200

    shares = quote(volume=1200, volume_unit="shares", amount=1_200)
    result = evaluate([shares])
    assert result["rows"][0]["metrics"]["volume_shares"] == 1200


def test_missing_quote_does_not_drag_down_other_symbols() -> None:
    result = evaluate([quote(SYMBOLS[0])], symbols=SYMBOLS)
    rows = result["rows"]
    assert [row["symbol"] for row in rows] == SYMBOLS
    assert rows[0]["realtime_gate"] == "PASS"
    assert rows[1]["realtime_gate"] == "MISSING_REALTIME"
    assert result["status"] == "PARTIAL"
    assert result["output_symbols"] == [SYMBOLS[0]]


@pytest.mark.parametrize(
    "changes",
    [
        {"price": math.nan},
        {"price": True},
        {"amount": -1},
        {"volume": False},
        {"bid1": math.inf},
    ],
)
def test_nan_bool_and_invalid_numbers_are_rejected(changes: dict) -> None:
    result = evaluate([quote(**changes)])
    row = result["rows"][0]
    assert row["realtime_gate"] == "REJECT"
    assert row["execution_eligible"] is False


def test_duplicate_and_unknown_identity_fail_without_quote_payload_in_error() -> None:
    duplicate = quote()
    with pytest.raises(PipelineError, match="duplicate") as error:
        evaluate([duplicate, deepcopy(duplicate)], symbols=[SYMBOLS[0]])
    assert "price" not in str(error.value)

    with pytest.raises(PipelineError, match="unknown"):
        evaluate([quote("600036.SH")], symbols=[SYMBOLS[0]])


def test_missing_units_is_missing_realtime_and_incomplete_vwap_is_not_fabricated() -> None:
    no_units = quote()
    del no_units["volume_unit"]
    result = evaluate([no_units])
    assert result["rows"][0]["realtime_gate"] == "MISSING_REALTIME"

    no_amount = quote(amount=None)
    result = evaluate([no_amount])
    row = result["rows"][0]
    assert row["realtime_gate"] == "DOWNGRADE"
    assert row["metrics"]["session_vwap"] is None
    assert "session_vwap" in row["missing_fields"]


def test_inconsistent_reported_zero_is_preserved_but_not_usable_vwap_or_execution() -> None:
    zero_amount = evaluate([quote(amount=0, volume=10)])
    row = zero_amount["rows"][0]
    assert row["realtime_gate"] == "REJECT"
    assert row["quote"]["amount"] == 0  # Preserve raw zero, do not reinterpret it as missing.
    assert row["metrics"]["amount_cny"] == 0
    assert row["metrics"]["session_vwap"] is None

    no_volume = evaluate([quote(amount=10_000, volume=0)])
    assert no_volume["rows"][0]["realtime_gate"] == "REJECT"
    assert no_volume["rows"][0]["metrics"]["session_vwap"] is None

    bad_unit = evaluate([quote(amount=10_000, volume=10, amount_unit="THOUSAND_CNY")])
    row = bad_unit["rows"][0]
    assert row["realtime_gate"] == "REJECT"
    assert row["metrics"]["session_vwap"] is None

    inconsistent = evaluate([quote(amount=1, volume=10)])
    row = inconsistent["rows"][0]
    assert row["realtime_gate"] == "REJECT"
    assert "trade_totals_price_inconsistent" in row["reject_reasons"]

    overflow = evaluate([quote(amount=0, volume=1e308)])
    assert overflow["rows"][0]["realtime_gate"] == "REJECT"
    assert overflow["rows"][0]["metrics"]["session_vwap"] is None


@pytest.mark.parametrize(
    "flags,expected_gate,reason",
    [
        ({"tradeable": False}, "REJECT", "not_tradeable"),
        ({"is_suspended": True}, "REJECT", "suspended_flag"),
        ({"suspended": True}, "REJECT", "suspended_flag"),
        ({"is_limit_up": True}, "DOWNGRADE", "limit_up_flag"),
        ({"is_limit_down": True}, "DOWNGRADE", "limit_down_flag"),
    ],
)
def test_explicit_tradeability_suspension_and_limit_flags_are_respected(
    flags: dict, expected_gate: str, reason: str
) -> None:
    result = evaluate([quote(**flags)])
    row = result["rows"][0]
    assert row["realtime_gate"] == expected_gate
    assert reason in row["reasons"]
    assert row["execution_eligible"] is False


def test_benchmark_identity_is_required_before_relative_strength() -> None:
    valid = quote("000300.SH", price=1.01, prev_close=1, opening=1, high=1.02, low=0.99)
    result = evaluate([quote()], benchmark_quote=valid)
    assert result["rows"][0]["metrics"]["intraday_relative_strength"] is not None

    wrong = quote("399001.SZ", price=1.01, prev_close=1, opening=1, high=1.02, low=0.99)
    result = evaluate([quote()], benchmark_quote=wrong)
    row = result["rows"][0]
    assert row["metrics"]["intraday_relative_strength"] is None
    assert "intraday_relative_strength" in row["missing_fields"]
    assert result["meta"]["benchmark_reason"] == "benchmark_identity_mismatch"


def test_observed_at_must_be_aware_and_same_local_research_date() -> None:
    with pytest.raises(PipelineError):
        evaluate([quote()], observed_at=datetime(2026, 9, 2, 10, 0, 5))
    with pytest.raises(PipelineError):
        evaluate([quote()], observed_at=datetime(2026, 9, 3, 10, tzinfo=SHANGHAI))
    with pytest.raises(PipelineError):
        evaluate([quote()], observed_at=datetime(2026, 9, 2, 8, 59, tzinfo=SHANGHAI))


def test_received_at_must_not_precede_server_or_be_future() -> None:
    before = quote(received_at="2026-09-02T09:59:59+08:00")
    assert evaluate([before])["rows"][0]["realtime_gate"] == "REJECT"
    future = quote(received_at="2026-09-02T10:00:06+08:00")
    assert evaluate([future])["rows"][0]["realtime_gate"] == "REJECT"
    assert evaluate([quote(received_at=None)])["rows"][0]["realtime_gate"] == "REJECT"
    assert evaluate([quote(received_at="")])["rows"][0]["realtime_gate"] == "REJECT"


def test_stale_and_fallback_markers_are_not_silent_passes() -> None:
    stale = evaluate([quote(stale=True)])
    assert stale["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert stale["meta"]["stale"] is True

    fallback = evaluate([quote(fallback_used=True)])
    assert fallback["rows"][0]["realtime_gate"] == "DOWNGRADE"
    assert fallback["meta"]["fallback_used"] is True


def test_malformed_unit_only_rejects_its_own_row_and_context_is_canonical() -> None:
    malformed = quote(SYMBOLS[0])
    malformed["volume_unit"] = ["lots"]
    result = evaluate([malformed, quote(SYMBOLS[1])])
    assert result["rows"][0]["realtime_gate"] == "REJECT"
    assert result["rows"][1]["realtime_gate"] == "PASS"
    assert "market_data_reference_date" in result["meta"]
    assert all(row["meta"]["market_data_reference_date"] == result["meta"]["market_data_reference_date"]
               for row in result["rows"])

from __future__ import annotations

import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.evidence_fields import supplement_sections


SYMBOL = "600000.SH"
SESSIONS = [
    "2026-08-28",
    "2026-08-31",
    "2026-09-01",
    "2026-09-02",
    "2026-09-03",
]
LATEST = SESSIONS[-1]
SOURCE = "tushare"
OBSERVED_AT = "2026-09-03T18:00:00+08:00"


def _compact(day: str) -> str:
    return day.replace("-", "")


def _batch(
    endpoint: str,
    data: list[dict],
    *,
    status: str = "SUCCESS",
    start: str | None = None,
    end: str | None = None,
    request_symbol: bool = True,
    source: str = SOURCE,
) -> dict:
    start = start or (SESSIONS[0] if data else LATEST)
    end = end or (SESSIONS[-1] if data else start)
    request: dict[str, object] = {
        "start_date": _compact(start),
        "end_date": _compact(end),
        "fields": "ts_code,trade_date",
    }
    if start == end:
        request.pop("start_date")
        request.pop("end_date")
        request["trade_date"] = _compact(start)
    if request_symbol:
        request["ts_code"] = SYMBOL
    if endpoint == "moneyflow":
        request["fields"] = (
            "ts_code,trade_date,buy_lg_amount,sell_lg_amount,"
            "buy_elg_amount,sell_elg_amount,net_mf_amount"
        )
    elif endpoint == "margin_detail":
        request["fields"] = "ts_code,trade_date,rzye,rzmre,rzche,rqye,rqyl,rzrqye"
    elif endpoint == "top_list":
        request["fields"] = "ts_code,trade_date,reason,net_amount,l_buy,l_sell"
    return {
        "status": status,
        "data": data,
        "source": source,
        "endpoint": endpoint,
        "request": request,
        "context": {"market_data_reference_date": LATEST},
        "observed_at": OBSERVED_AT,
        "integrity_hash": "fixture-hash",
        "request_id": f"fixture-{endpoint}-{start}-{end}",
    }


def _flow_row(day: str, *, buy: float = 10, sell: float = 2, net: float = 1) -> dict:
    return {
        "ts_code": SYMBOL,
        "trade_date": _compact(day),
        "buy_lg_amount": buy,
        "sell_lg_amount": sell,
        "buy_elg_amount": 5,
        "sell_elg_amount": 3,
        "net_mf_amount": net,
    }


def _margin_row(day: str, *, buy: float = 20, repay: float = 5) -> dict:
    return {
        "ts_code": SYMBOL,
        "trade_date": _compact(day),
        "rzye": 100,
        "rzmre": buy,
        "rzche": repay,
        "rqye": 30,
        "rqyl": 40,
        "rzrqye": 130,
    }


def _all_amounts(value: float = 100_000) -> dict[str, float]:
    return {day: value for day in SESSIONS}


def test_signed_and_zero_moneyflow_legs_are_valid_and_scaled_once() -> None:
    rows = [_flow_row(day) for day in SESSIONS]
    rows[0] = _flow_row(SESSIONS[0], buy=0, sell=0, net=0)
    rows[0]["buy_elg_amount"] = 0
    rows[0]["sell_elg_amount"] = 0
    rows[-1] = _flow_row(SESSIONS[-1], buy=2, sell=10, net=-2)

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", rows)],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[],
        amount_by_date=_all_amounts(),
    )

    flow = result["flow"]
    assert flow["coverage"] == 1.0
    assert flow["rows"][0]["main"] == 0.0
    assert flow["latest_main_flow_cny"] == -60_000.0
    assert flow["main_flow_5d_cny"] == 3 * 100_000 - 60_000
    assert flow["latest_net_mf_cny"] == -20_000.0
    assert flow["main_flow_ratio_5d"] == pytest.approx(flow["main_flow_5d_cny"] / 500_000)
    assert flow["metric_definition"] == (
        "large+extra_large_net_proxy; not vendor net_mf_amount or Eastmoney main"
    )


def test_missing_moneyflow_leg_never_becomes_zero() -> None:
    rows = [_flow_row(day) for day in SESSIONS]
    rows[-1]["sell_lg_amount"] = None

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", rows)],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[],
        amount_by_date=_all_amounts(),
    )

    flow = result["flow"]
    assert flow["latest_main_flow_cny"] is None
    assert flow["main_flow_5d_cny"] is None
    assert flow["coverage"] == 0.8
    assert flow["rows"][-1]["main"] is None
    assert "sell_lg_amount" in flow["rows"][-1]["missing_reason"]
    assert flow["latest_net_mf_cny"] == 10_000.0


def test_five_day_gap_stays_a_gap_and_does_not_compress_four_days() -> None:
    rows = [_flow_row(day) for day in SESSIONS if day != SESSIONS[1]]

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", rows, start=SESSIONS[0], end=SESSIONS[-1])],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[],
        amount_by_date=_all_amounts(),
    )

    flow = result["flow"]
    assert flow["rows"][1] == {
        "trade_date": SESSIONS[1],
        "main": None,
        "net": None,
        "missing_reason": "missing_row",
    }
    assert flow["coverage"] == 0.8
    assert flow["main_flow_5d_cny"] is None
    assert sum(row["main"] for row in flow["rows"] if row["main"] is not None) == 400_000.0


def test_missing_actual_amount_only_blocks_ratio() -> None:
    rows = [_flow_row(day) for day in SESSIONS]
    amounts = _all_amounts()
    amounts[SESSIONS[2]] = None

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", rows)],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[],
        amount_by_date=amounts,
    )

    flow = result["flow"]
    assert flow["main_flow_5d_cny"] == 500_000.0
    assert flow["main_flow_ratio_5d"] is None
    assert "main_flow_ratio_5d" in flow["missing"]


def test_dragon_empty_success_is_false_but_source_failure_is_unknown() -> None:
    empty = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[],
        amount_by_date={},
    )["dragon_tiger"]
    failed = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[],
        dragon_batch=_batch("top_list", [], status="MISSING", request_symbol=False),
        margin_batches=[],
        amount_by_date={},
    )["dragon_tiger"]

    assert empty["listed"] is False
    assert empty["coverage"] == 1.0
    assert empty["latest_net_buy_cny"] is None
    assert failed["listed"] is None
    assert failed["coverage"] == 0.0
    assert failed["latest_net_buy_cny"] is None


def test_dragon_multiple_reasons_are_preserved_without_aggregation() -> None:
    data = [
        {
            "ts_code": SYMBOL,
            "trade_date": _compact(LATEST),
            "reason": "window-a",
            "net_amount": 100.0,
            "l_buy": 300.0,
            "l_sell": 200.0,
        },
        {
            "ts_code": SYMBOL,
            "trade_date": _compact(LATEST),
            "reason": "window-b",
            "net_amount": 200.0,
            "l_buy": 500.0,
            "l_sell": 300.0,
        },
    ]

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[],
        dragon_batch=_batch("top_list", data, start=LATEST, end=LATEST, request_symbol=False),
        margin_batches=[],
        amount_by_date={},
    )

    dragon = result["dragon_tiger"]
    assert dragon["listed"] is True
    assert dragon["entries"] == data
    assert dragon["latest_net_buy_cny"] is None
    assert "dragon_multiple_windows_not_aggregated" in result["risk_flags"]


def test_missing_margin_rows_remain_unknown_not_not_a_margin_security() -> None:
    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[_batch("margin_detail", [], start=SESSIONS[0], end=SESSIONS[-1])],
        amount_by_date={},
    )

    margin = result["margin"]
    assert margin["coverage"] == 0.0
    assert margin["financing_net_buy_5d_cny"] is None
    assert margin["latest_financing_balance_cny"] is None
    assert margin["latest_short_shares"] is None
    assert all(row["rzye"] is None for row in margin["rows"])
    assert "is_margin_security" not in margin


def test_duplicate_and_future_rows_are_rejected() -> None:
    duplicate = _flow_row(SESSIONS[0])
    with pytest.raises(PipelineError, match="duplicate"):
        supplement_sections(
            symbol=SYMBOL,
            sessions=SESSIONS,
            flow_batches=[
                _batch("moneyflow", [duplicate], start=SESSIONS[0], end=SESSIONS[0]),
                _batch("moneyflow", [duplicate], start=SESSIONS[0], end=SESSIONS[0]),
            ],
            dragon_batch=_batch("top_list", [], request_symbol=False),
            margin_batches=[],
            amount_by_date={},
        )

    future = _flow_row(LATEST)
    future["trade_date"] = "20260904"
    with pytest.raises(PipelineError, match="future"):
        supplement_sections(
            symbol=SYMBOL,
            sessions=SESSIONS,
            flow_batches=[_batch("moneyflow", [future], start=LATEST, end=LATEST)],
            dragon_batch=_batch("top_list", [], request_symbol=False),
            margin_batches=[],
            amount_by_date={},
        )


def test_abnormal_status_and_missing_margin_fields_are_isolated_as_missing() -> None:
    incomplete = _margin_row(LATEST)
    incomplete.pop("rzmre")

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", [], status="ERROR", start=LATEST, end=LATEST)],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[_batch("margin_detail", [incomplete], start=LATEST, end=LATEST)],
        amount_by_date={},
    )

    assert result["flow"]["rows"][-1]["main"] is None
    assert "unexpected_status" in result["flow"]["reason"]
    assert result["margin"]["rows"][-1]["rzmre"] is None
    assert "rzmre" in result["margin"]["rows"][-1]["missing_reason"]


def test_whole_market_multi_stock_batches_filter_locally_and_keep_target_net() -> None:
    flow_rows = []
    margin_rows = []
    for day in SESSIONS:
        flow_rows.extend([_flow_row(day, net=2), {**_flow_row(day, net=99), "ts_code": "600001.SH"}])
        margin_rows.extend([_margin_row(day), {**_margin_row(day, buy=99), "ts_code": "600001.SH"}])

    result = supplement_sections(
        symbol=SYMBOL,
        sessions=SESSIONS,
        flow_batches=[_batch("moneyflow", flow_rows, request_symbol=False)],
        dragon_batch=_batch("top_list", [], request_symbol=False),
        margin_batches=[_batch("margin_detail", margin_rows, request_symbol=False)],
        amount_by_date=_all_amounts(),
    )

    assert result["flow"]["coverage"] == 1.0
    assert result["flow"]["latest_net_mf_cny"] == 20_000.0
    assert result["flow"]["main_flow_5d_cny"] == 500_000.0
    assert result["margin"]["coverage"] == 1.0
    assert result["margin"]["financing_net_buy_5d_cny"] == 75.0


def test_dragon_previous_session_is_not_relabeled_as_reference_date():
    with pytest.raises(PipelineError, match="reference session"):
        supplement_sections(symbol=SYMBOL, sessions=SESSIONS, flow_batches=[], margin_batches=[],
            dragon_batch=_batch("top_list", [], start=SESSIONS[-2], end=SESSIONS[-2], request_symbol=False),
            amount_by_date={})

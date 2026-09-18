"""Pure, date-aligned construction of the S3 supplemental evidence fields."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.features import finite


METRIC_DEFINITION = (
    "large+extra_large_net_proxy; not vendor net_mf_amount or Eastmoney main"
)
_FLOW_LEGS = ("buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount")
_MARGIN_FIELDS = ("rzye", "rzmre", "rzche", "rqye", "rqyl", "rzrqye")


def supplement_sections(
    *,
    symbol: str,
    sessions: list[str],
    flow_batches: list[dict],
    dragon_batch: dict,
    margin_batches: list[dict],
    amount_by_date: dict[str, float | None],
) -> dict:
    """Build the flow, dragon-tiger and margin evidence sections.

    The caller owns fetching and persistence.  This function only consumes
    already frozen SourceCache records and an already normalized actual-amount
    map.  A source failure is represented as missing evidence; malformed dates
    or duplicate identities that could make the evidence ambiguous fail closed.
    """

    symbol = _validate_symbol(symbol)
    session_dates = _validate_sessions(sessions)
    if not isinstance(flow_batches, list) or not isinstance(margin_batches, list):
        raise PipelineError("INVALID_ARGUMENTS", "evidence batches must be lists")
    if not isinstance(dragon_batch, dict):
        raise PipelineError("INVALID_ARGUMENTS", "dragon_batch must be a mapping")
    if not isinstance(amount_by_date, dict):
        raise PipelineError("INVALID_ARGUMENTS", "amount_by_date must be a mapping")

    amounts = _amounts_for_sessions(amount_by_date, session_dates)
    flow = _build_flow(symbol, session_dates, flow_batches, amounts)
    dragon = _build_dragon(symbol, session_dates, dragon_batch)
    margin = _build_margin(symbol, session_dates, margin_batches)

    risk_flags: list[str] = []
    if flow["main_flow_5d_cny"] is not None and flow["main_flow_5d_cny"] < 0:
        risk_flags.append("main_flow_5d_negative")
    if flow["main_flow_ratio_5d"] is not None and abs(flow["main_flow_ratio_5d"]) > 1:
        risk_flags.append("main_flow_ratio_5d_outlier")
    if len(dragon["entries"]) > 1:
        risk_flags.append("dragon_multiple_windows_not_aggregated")
    if margin["financing_net_buy_5d_cny"] is not None and margin["financing_net_buy_5d_cny"] < 0:
        risk_flags.append("financing_net_repayment_5d")

    return {
        "flow": flow,
        "dragon_tiger": dragon,
        "margin": margin,
        "risk_flags": risk_flags,
    }


def _validate_symbol(symbol: Any) -> str:
    if not isinstance(symbol, str) or not symbol or symbol != symbol.strip():
        raise PipelineError("INVALID_ARGUMENTS", "symbol must be a non-empty string")
    return symbol


def _validate_sessions(sessions: Any) -> list[str]:
    if not isinstance(sessions, list) or len(sessions) != 5:
        raise PipelineError("INVALID_ARGUMENTS", "sessions must contain exactly five dates")
    result: list[str] = []
    seen: set[date] = set()
    for value in sessions:
        if not isinstance(value, str) or not value or value != value.strip():
            raise PipelineError("INVALID_ARGUMENTS", "sessions contain an invalid date")
        parsed = _parse_date(value)
        if parsed is None:
            raise PipelineError("INVALID_ARGUMENTS", "sessions contain an invalid date")
        if parsed in seen:
            raise PipelineError("INVALID_ARGUMENTS", "sessions contain a duplicate date")
        seen.add(parsed)
        result.append(parsed.isoformat())
    if result != sorted(result):
        raise PipelineError("INVALID_ARGUMENTS", "sessions must be ascending")
    return result


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").date()
        except ValueError:
            return None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == text else None


def _amounts_for_sessions(values: dict[str, float | None], sessions: list[str]) -> dict[str, float | None]:
    normalized: dict[str, float | None] = {}
    for key, value in values.items():
        parsed = _parse_date(key)
        if parsed is None or parsed.isoformat() not in sessions:
            continue
        number = finite(value)
        normalized[parsed.isoformat()] = number if number is not None and number >= 0 else None
    return {day: normalized.get(day) for day in sessions}


def _source_token(batch: Any) -> str | None:
    if not isinstance(batch, Mapping):
        return None
    source = batch.get("source")
    endpoint = batch.get("endpoint")
    if not isinstance(source, str) or not source.strip() or not isinstance(endpoint, str) or not endpoint.strip():
        return None
    return f"{source.strip()}.{endpoint.strip()}"


def _section_source(batches: list[Any]) -> str | None:
    sources: list[str] = []
    for batch in batches:
        source = _source_token(batch)
        if source is not None and source not in sources:
            sources.append(source)
    return ",".join(sources) if sources else None


def _request_scope(request: Any, sessions: list[str], label: str) -> tuple[list[str] | None, str | None]:
    if not isinstance(request, Mapping):
        return None, "missing_request"
    has_trade_date = "trade_date" in request
    has_range = "start_date" in request or "end_date" in request
    if has_trade_date and has_range:
        return None, "ambiguous_request_dates"
    if has_trade_date:
        parsed = _parse_date(request.get("trade_date"))
        if parsed is None:
            return None, "missing_request_date"
        _reject_outside_window(parsed, sessions, label)
        iso = parsed.isoformat()
        if iso not in sessions:
            raise PipelineError("STALE_DATA", f"{label} request date does not align with sessions")
        return [iso], None
    if has_range:
        if "start_date" not in request or "end_date" not in request:
            return None, "incomplete_request_range"
        start = _parse_date(request.get("start_date"))
        end = _parse_date(request.get("end_date"))
        if start is None or end is None or start > end:
            return None, "invalid_request_range"
        _reject_outside_window(start, sessions, label)
        _reject_outside_window(end, sessions, label)
        if start.isoformat() not in sessions or end.isoformat() not in sessions:
            raise PipelineError("STALE_DATA", f"{label} request dates do not align with sessions")
        selected = [day for day in sessions if start <= date.fromisoformat(day) <= end]
        return selected, None
    return None, "missing_request_date"


def _reject_outside_window(value: date, sessions: list[str], label: str) -> None:
    first = date.fromisoformat(sessions[0])
    last = date.fromisoformat(sessions[-1])
    if value > last:
        raise PipelineError("STALE_DATA", f"{label} contains a future date")
    if value < first:
        raise PipelineError("STALE_DATA", f"{label} date is outside sessions")


def _requested_symbol_matches(request: Mapping[str, Any], symbol: str) -> bool:
    values = [request[key] for key in ("ts_code", "symbol", "code") if key in request]
    values = [value for value in values if value is not None and str(value).strip()]
    return bool(values) and all(value == symbol for value in values)


def _has_symbol_filter(request: Mapping[str, Any]) -> bool:
    return any(
        key in request and request[key] is not None and str(request[key]).strip()
        for key in ("ts_code", "symbol", "code")
    )


def _inspect_batch(
    batch: Any,
    *,
    symbol: str,
    sessions: list[str],
    endpoint: str,
    full_market: bool = False,
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "status": None,
        "data": [],
        "scope": None,
        "reasons": [],
        "source": _source_token(batch),
    }
    if not isinstance(batch, Mapping):
        info["reasons"] = ["missing_batch"]
        return info

    status = batch.get("status")
    info["status"] = status
    reasons: list[str] = []
    if status not in {"SUCCESS", "MISSING"}:
        reasons.append("unexpected_status")
    if _source_token(batch) is None:
        reasons.append("missing_source")
    if batch.get("endpoint") != endpoint:
        reasons.append("unexpected_endpoint")

    request = batch.get("request")
    scope, request_reason = _request_scope(request, sessions, endpoint)
    info["scope"] = scope
    if request_reason is not None:
        reasons.append(request_reason)
    elif isinstance(request, Mapping):
        if full_market and _has_symbol_filter(request):
            reasons.append("dragon_request_must_be_full_market")
        elif full_market and scope != [sessions[-1]]:
            reasons.append("dragon_request_must_be_latest_date")
        elif not full_market and _has_symbol_filter(request) and not _requested_symbol_matches(request, symbol):
            reasons.append("request_symbol_mismatch")

    data = batch.get("data")
    if status == "SUCCESS":
        if not isinstance(data, list):
            reasons.append("missing_data")
        else:
            info["data"] = data
    elif isinstance(data, list):
        info["data"] = data
    info["reasons"] = _unique(reasons)
    return info


def _scope_for(info: dict[str, Any], sessions: list[str]) -> list[str]:
    scope = info.get("scope")
    return list(scope) if isinstance(scope, list) and scope else list(sessions)


def _claim_scope(claimed: set[str], scope: list[str], label: str) -> None:
    overlap = claimed.intersection(scope)
    if overlap:
        raise PipelineError("STALE_DATA", f"duplicate {label} symbol/date request")
    claimed.update(scope)


def _check_returned_date(parsed: date, *, scope: list[str], sessions: list[str], label: str) -> str:
    if parsed > date.fromisoformat(sessions[-1]):
        raise PipelineError("STALE_DATA", f"{label} returned a future date")
    iso = parsed.isoformat()
    if iso not in sessions:
        raise PipelineError("STALE_DATA", f"{label} returned a date outside sessions")
    if iso not in scope:
        raise PipelineError("STALE_DATA", f"{label} returned a date outside requested dates")
    return iso


def _build_flow(
    symbol: str,
    sessions: list[str],
    batches: list[dict],
    amounts: dict[str, float | None],
) -> dict[str, Any]:
    sources = _section_source(batches)
    day_reasons = {day: [] for day in sessions}
    values: dict[str, dict[str, Any]] = {}
    claimed: set[str] = set()

    for batch in batches:
        info = _inspect_batch(batch, symbol=symbol, sessions=sessions, endpoint="moneyflow")
        scope = _scope_for(info, sessions)
        if info.get("scope"):
            _claim_scope(claimed, scope, "flow")
        if info["reasons"]:
            for day in scope:
                _note(day_reasons, day, *info["reasons"])
            continue
        if info["status"] == "MISSING":
            for day in scope:
                _note(day_reasons, day, "source_missing")
            continue

        data = info["data"]
        if not data:
            for day in scope:
                _note(day_reasons, day, "empty_success")
        found: set[str] = set()
        for raw in data:
            if not isinstance(raw, Mapping):
                for day in scope:
                    _note(day_reasons, day, "invalid_row")
                continue
            raw_date = raw.get("trade_date")
            parsed = _parse_date(raw_date)
            if parsed is not None:
                _check_returned_date(parsed, scope=scope, sessions=sessions, label="moneyflow")
            raw_symbol = raw.get("ts_code")
            if raw_symbol != symbol:
                if raw_symbol is None:
                    for day in scope:
                        _note(day_reasons, day, "missing_symbol")
                continue
            if parsed is None:
                for day in scope:
                    _note(day_reasons, day, "missing_trade_date")
                continue
            day = parsed.isoformat()
            if day in values:
                raise PipelineError("STALE_DATA", "duplicate flow symbol/date row")
            main, net, row_reason = _flow_values(raw)
            values[day] = {"main": main, "net": net, "row_reason": row_reason}
            found.add(day)
        for day in scope:
            if day not in found:
                _note(day_reasons, day, "missing_row")

    rows: list[dict[str, Any]] = []
    for day in sessions:
        value = values.get(day)
        if value is None:
            main = None
            net = None
            reason = _join_reasons(day_reasons[day] or ["missing_row"])
        else:
            main = value["main"]
            net = value["net"]
            reason = _join_reasons([*day_reasons[day], value["row_reason"]])
        rows.append({"trade_date": day, "main": main, "net": net, "missing_reason": reason})

    valid_main = [row["main"] for row in rows if row["main"] is not None]
    latest_main = rows[-1]["main"]
    main_5d = sum(valid_main, 0.0) if len(valid_main) == len(sessions) else None
    amount_values = [amounts[day] for day in sessions]
    amount_complete = all(value is not None for value in amount_values)
    amount_total = sum(amount_values, 0.0) if amount_complete else None
    ratio = (
        main_5d / amount_total
        if main_5d is not None and amount_total is not None and amount_total > 0
        else None
    )

    missing: list[str] = []
    for row in rows:
        if row["main"] is None:
            missing.append(f"rows.{row['trade_date']}.main")
        if row["net"] is None:
            missing.append(f"rows.{row['trade_date']}.net")
    for field, value in (
        ("latest_main_flow_cny", latest_main),
        ("main_flow_5d_cny", main_5d),
        ("main_flow_ratio_5d", ratio),
        ("latest_net_mf_cny", rows[-1]["net"]),
    ):
        if value is None:
            missing.append(field)
    reasons = _unique([reason for day in sessions for reason in day_reasons[day]])
    if main_5d is None:
        reasons.append("main_flow_5d_incomplete")
    if not amount_complete:
        reasons.append("amount_by_date_incomplete")
    elif amount_total is not None and amount_total <= 0:
        reasons.append("amount_by_date_nonpositive")
    return {
        "as_of": sessions[-1],
        "source": sources,
        "coverage": len(valid_main) / len(sessions),
        "metric_definition": METRIC_DEFINITION,
        "latest_main_flow_cny": latest_main,
        "main_flow_5d_cny": main_5d,
        "main_flow_ratio_5d": ratio,
        "latest_net_mf_cny": rows[-1]["net"],
        "rows": rows,
        "missing": missing,
        "reason": _unique(reasons),
    }


def _flow_values(row: Mapping[str, Any]) -> tuple[float | None, float | None, str | None]:
    reasons: list[str] = []
    legs: list[float] = []
    for field in _FLOW_LEGS:
        value = finite(row.get(field))
        if value is None:
            reasons.append(f"missing_{field}")
        elif value < 0:
            reasons.append(f"invalid_{field}")
        else:
            legs.append(value)
    main: float | None
    if len(legs) != len(_FLOW_LEGS):
        main = None
    else:
        main = finite((legs[0] + legs[2] - legs[1] - legs[3]) * 10_000)
        if main is None:
            reasons.append("invalid_main_flow")
    net_value = finite(row.get("net_mf_amount"))
    net = finite(net_value * 10_000) if net_value is not None else None
    if net is None:
        reasons.append("missing_net_mf_amount")
    return main, net, _join_reasons(reasons)


def _build_dragon(symbol: str, sessions: list[str], batch: dict) -> dict[str, Any]:
    info = _inspect_batch(
        batch,
        symbol=symbol,
        sessions=sessions,
        endpoint="top_list",
        full_market=True,
    )
    source = _section_source([batch])
    scope = _scope_for(info, sessions)
    if info.get("scope") and scope != [sessions[-1]]:
        raise PipelineError("STALE_DATA", "dragon snapshot must use the reference session only")
    entries: list[dict[str, Any]] = []
    reasons = list(info["reasons"])
    missing: list[str] = []

    if info["reasons"] or info["status"] == "MISSING":
        if info["status"] == "MISSING":
            reasons.append("source_missing")
        listed = None
        coverage = 0.0
        missing.extend(("listed", "latest_net_buy_cny"))
    else:
        invalid_target_row = False
        seen: set[tuple[str, str]] = set()
        for raw in info["data"]:
            if not isinstance(raw, Mapping):
                invalid_target_row = True
                reasons.append("invalid_row")
                continue
            raw_date = raw.get("trade_date")
            parsed = _parse_date(raw_date)
            if parsed is not None:
                _check_returned_date(parsed, scope=scope, sessions=sessions, label="top_list")
            raw_symbol = raw.get("ts_code")
            if raw_symbol != symbol:
                continue
            if parsed is None:
                invalid_target_row = True
                reasons.append("missing_trade_date")
                continue
            reason_key = repr(raw.get("reason"))
            key = (parsed.isoformat(), reason_key)
            if key in seen:
                raise PipelineError("STALE_DATA", "duplicate dragon symbol/date/reason row")
            seen.add(key)
            entries.append(
                {
                    "ts_code": raw.get("ts_code"),
                    "trade_date": raw.get("trade_date"),
                    "reason": raw.get("reason"),
                    "net_amount": raw.get("net_amount"),
                    "l_buy": raw.get("l_buy"),
                    "l_sell": raw.get("l_sell"),
                }
            )
        if entries:
            listed = True
        elif invalid_target_row:
            listed = None
            missing.append("listed")
        else:
            listed = False
            reasons.append("no_symbol_row")
        coverage = 1.0
        if not entries:
            missing.append("latest_net_buy_cny")

    latest = entries[0]["net_amount"] if len(entries) == 1 else None
    if len(entries) != 1:
        missing.append("latest_net_buy_cny")
    return {
        "as_of": sessions[-1],
        "source": source,
        "coverage": coverage,
        "listed": listed,
        "entries": entries,
        "latest_net_buy_cny": latest,
        "missing": _unique(missing),
        "reason": _unique(reasons),
    }


def _build_margin(symbol: str, sessions: list[str], batches: list[dict]) -> dict[str, Any]:
    sources = _section_source(batches)
    day_reasons = {day: [] for day in sessions}
    values: dict[str, dict[str, Any]] = {}
    claimed: set[str] = set()

    for batch in batches:
        info = _inspect_batch(batch, symbol=symbol, sessions=sessions, endpoint="margin_detail")
        scope = _scope_for(info, sessions)
        if info.get("scope"):
            _claim_scope(claimed, scope, "margin")
        if info["reasons"]:
            for day in scope:
                _note(day_reasons, day, *info["reasons"])
            continue
        if info["status"] == "MISSING":
            for day in scope:
                _note(day_reasons, day, "source_missing")
            continue

        data = info["data"]
        if not data:
            for day in scope:
                _note(day_reasons, day, "empty_success")
        found: set[str] = set()
        for raw in data:
            if not isinstance(raw, Mapping):
                for day in scope:
                    _note(day_reasons, day, "invalid_row")
                continue
            raw_date = raw.get("trade_date")
            parsed = _parse_date(raw_date)
            if parsed is not None:
                _check_returned_date(parsed, scope=scope, sessions=sessions, label="margin_detail")
            raw_symbol = raw.get("ts_code")
            if raw_symbol != symbol:
                if raw_symbol is None:
                    for day in scope:
                        _note(day_reasons, day, "missing_symbol")
                continue
            if parsed is None:
                for day in scope:
                    _note(day_reasons, day, "missing_trade_date")
                continue
            day = parsed.isoformat()
            if day in values:
                raise PipelineError("STALE_DATA", "duplicate margin symbol/date row")
            row_values, row_reason = _margin_values(raw)
            values[day] = {**row_values, "row_reason": row_reason}
            found.add(day)
        for day in scope:
            if day not in found:
                _note(day_reasons, day, "missing_row")

    rows: list[dict[str, Any]] = []
    for day in sessions:
        value = values.get(day)
        if value is None:
            field_values = {field: None for field in _MARGIN_FIELDS}
            reason = _join_reasons(day_reasons[day] or ["missing_row"])
        else:
            field_values = {field: value[field] for field in _MARGIN_FIELDS}
            reason = _join_reasons([*day_reasons[day], value["row_reason"]])
        rows.append({"trade_date": day, **field_values, "missing_reason": reason})

    latest = rows[-1]
    complete_dates = sum(
        all(rows[index][field] is not None for field in ("rzye", "rzmre", "rzche"))
        for index in range(len(rows))
    )
    buy_repay_complete = all(
        row["rzmre"] is not None and row["rzche"] is not None for row in rows
    )
    financing_net = (
        sum((row["rzmre"] - row["rzche"]) for row in rows)
        if buy_repay_complete
        else None
    )
    top_fields = {
        "latest_financing_balance_cny": latest["rzye"],
        "latest_financing_buy_cny": latest["rzmre"],
        "latest_financing_repay_cny": latest["rzche"],
        "latest_short_balance_cny": latest["rqye"],
        "latest_short_shares": latest["rqyl"],
        "latest_total_balance_cny": latest["rzrqye"],
        "financing_net_buy_5d_cny": financing_net,
    }
    missing: list[str] = []
    for row in rows:
        for field in _MARGIN_FIELDS:
            if row[field] is None:
                missing.append(f"rows.{row['trade_date']}.{field}")
    missing.extend(field for field, value in top_fields.items() if value is None)
    reasons = _unique([reason for day in sessions for reason in day_reasons[day]])
    if financing_net is None:
        reasons.append("financing_net_buy_5d_incomplete")
    return {
        "as_of": sessions[-1],
        "source": sources,
        "coverage": complete_dates / len(sessions),
        **top_fields,
        "rows": rows,
        "missing": missing,
        "reason": _unique(reasons),
    }


def _margin_values(row: Mapping[str, Any]) -> tuple[dict[str, float | None], str | None]:
    values: dict[str, float | None] = {}
    reasons: list[str] = []
    for field in _MARGIN_FIELDS:
        value = finite(row.get(field))
        if value is None:
            reasons.append(f"missing_{field}")
        elif value < 0:
            reasons.append(f"invalid_{field}")
        else:
            values[field] = value
            continue
        values[field] = None
    return values, _join_reasons(reasons)


def _note(target: dict[str, list[str]], day: str, *reasons: str) -> None:
    for reason in reasons:
        if reason not in target[day]:
            target[day].append(reason)


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _join_reasons(values: list[str | None]) -> str | None:
    reasons = _unique([value for value in values if value is not None])
    return ";".join(reasons) if reasons else None


__all__ = ["METRIC_DEFINITION", "supplement_sections"]

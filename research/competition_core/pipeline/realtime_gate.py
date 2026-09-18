"""Pure, fail-closed validation for current-session quote snapshots.

The gate validates data already supplied by a provider.  It never fetches a
quote, chooses a clock, infers a trading limit, sizes an order, or changes the
canonical symbol contract.

Execution windows are Shanghai-time half-open intervals:
``[09:30, 11:30)`` and ``[13:00, 14:57)``.  Their starts are inclusive and
their ends are exclusive; pre-open, the lunch break, the closing auction, and
post-close are therefore data-only windows.  An optional benchmark contributes
relative strength only when its canonical configured identity is present and
its normalized ``server_time`` exactly matches the stock quote timestamp.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
import math
from typing import Any
from zoneinfo import ZoneInfo

from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import ResearchContext
from contracts.symbol import CanonicalSymbol, SymbolContractError


SHANGHAI = ZoneInfo("Asia/Shanghai")
# Quote volume normalization is a market-data unit rule, not an order-lot rule.
_VOLUME_LOT_SHARES = 100
_MAX_PROVIDER_CLOCK_SKEW = timedelta(seconds=10)
_PRICE_FIELDS = ("price", "prev_close", "open", "high", "low")
_GATE_VALUES = ("PASS", "DOWNGRADE", "REJECT", "MISSING_REALTIME")
_MISSING = object()
_EXECUTION_WINDOWS = (
    (time(9, 30), time(11, 30)),
    (time(13, 0), time(14, 57)),
)


@dataclass(slots=True)
class _QuoteView:
    quote: dict[str, Any]
    source: str | None = None
    fallback_used: bool = False
    stale: bool = False
    server_time: datetime | None = None
    trade_date: date | None = None
    received_at: datetime | None = None
    timestamp_valid: bool = False
    core_missing: bool = False
    core_ready: bool = False
    hard_invalid: bool = False
    future: bool = False
    prices: dict[str, float] = field(default_factory=dict)
    bid: float | None = None
    ask: float | None = None
    amount: float | None = None
    volume: float | None = None
    amount_unit: str | None = None
    volume_unit: str | None = None
    missing_fields: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    reject_reasons: list[str] = field(default_factory=list)
    downgrade_reasons: list[str] = field(default_factory=list)


def evaluate_realtime_quotes(
    *,
    symbols: list[str],
    quotes: list[dict],
    context: ResearchContext,
    config: PipelineConfig,
    observed_at: datetime,
    snapshot_id: str,
    benchmark_quote: dict | None = None,
) -> dict[str, Any]:
    """Evaluate provider quote rows against an explicit research context.

    ``rows`` always follows ``symbols`` order.  A missing or ordinary bad row
    is isolated to that symbol; batch identity violations (duplicate or
    unknown rows) raise :class:`PipelineError` before any result is returned.
    ``output_symbols`` contains only rows whose gate is PASS and whose quote
    is inside the continuous-auction execution window.
    """

    observed_local = _validate_arguments(
        symbols=symbols,
        quotes=quotes,
        context=context,
        config=config,
        observed_at=observed_at,
        snapshot_id=snapshot_id,
        benchmark_quote=benchmark_quote,
    )
    requested_symbols = list(symbols)
    quote_by_symbol = _index_quotes(quotes, requested_symbols)
    max_age = int(config.section("realtime")["max_quote_age_seconds"])
    realtime_config = config.section("realtime")
    execution_window_open = (
        context.phase == "intraday"
        and _in_continuous_auction(observed_local.time())
    )

    benchmark_view = None
    benchmark_reason = None
    if benchmark_quote is not None:
        expected_benchmark = config.section("screening")["benchmark_symbol"]
        if benchmark_quote.get("symbol") != expected_benchmark:
            benchmark_reason = (
                "benchmark_identity_missing"
                if "symbol" not in benchmark_quote
                else "benchmark_identity_mismatch"
            )
        else:
            benchmark_view = _inspect_quote(
                benchmark_quote,
                research_date=context.research_date,
                observed_at=observed_local,
                max_age=max_age,
                execution_window_open=True,
                check_execution_fields=False,
                realtime_config=realtime_config,
            )
            if not _benchmark_usable(benchmark_view):
                benchmark_reason = "benchmark_quote_invalid"

    rows: list[dict[str, Any]] = []
    source_provenance: list[dict[str, Any]] = []
    for symbol in requested_symbols:
        view = quote_by_symbol.get(symbol)
        if view is None:
            row = _missing_quote_row(
                symbol,
                context=context,
                observed_at=observed_local,
                snapshot_id=snapshot_id,
                benchmark_reason=benchmark_reason,
            )
        else:
            view = _inspect_quote(
                view,
                research_date=context.research_date,
                observed_at=observed_local,
                max_age=max_age,
                execution_window_open=execution_window_open,
                check_execution_fields=True,
                realtime_config=realtime_config,
            )
            row = _build_row(
                symbol,
                view,
                context=context,
                observed_at=observed_local,
                snapshot_id=snapshot_id,
                benchmark_view=benchmark_view,
                benchmark_reason=benchmark_reason,
            )
        rows.append(row)
        source_provenance.append(_provenance(symbol, row))

    counts = {gate: sum(row["realtime_gate"] == gate for row in rows)
              for gate in _GATE_VALUES}
    output_symbols = [
        row["symbol"] for row in rows if row["execution_eligible"]
    ]
    missing_fields = sorted({
        field_name
        for row in rows
        for field_name in row["missing_fields"]
    })
    reject_reasons = sorted({
        reason
        for row in rows
        for reason in row["reject_reasons"]
    })
    missing_counts = {
        field_name: sum(field_name in row["missing_fields"] for row in rows)
        for field_name in missing_fields
    }
    reject_counts = {
        reason: sum(reason in row["reject_reasons"] for row in rows)
        for reason in reject_reasons
    }
    source_values = sorted({
        row["meta"]["source"]
        for row in rows
        if row["meta"]["source"] is not None
    })
    source = source_values[0] if len(source_values) == 1 else source_values
    fallback_used = any(row["meta"]["fallback_used"] for row in rows)
    stale = any(row["meta"]["stale"] for row in rows)

    meta = {
        "research_date": context.research_date.isoformat(),
        "phase": context.phase,
        "market_data_reference_date": context.market_data_reference_date.isoformat(),
        "as_of": context.as_of.isoformat(),
        "source": source,
        "fallback_used": fallback_used,
        "stale": stale,
        "snapshot_id": snapshot_id,
        "observed_at": observed_local.isoformat(),
        "timezone": "Asia/Shanghai",
        "execution_window": {
            "timezone": "Asia/Shanghai",
            "phase_required": "intraday",
            "intervals": [
                {"start": "09:30:00", "end": "11:30:00",
                 "start_inclusive": True, "end_inclusive": False},
                {"start": "13:00:00", "end": "14:57:00",
                 "start_inclusive": True, "end_inclusive": False},
            ],
            "open_at_observed_at": execution_window_open,
        },
    }
    if benchmark_view is not None:
        meta["benchmark_source"] = benchmark_view.source
        meta["benchmark_server_time"] = (
            benchmark_view.server_time.isoformat()
            if benchmark_view.server_time is not None else None
        )
    elif benchmark_reason is not None:
        meta["benchmark_reason"] = benchmark_reason

    summary = {
        "input_count": len(requested_symbols),
        "output_count": len(output_symbols),
        "execution_eligible_count": len(output_symbols),
        "counts": dict(counts),
        "missing_counts": missing_counts,
        "reject_counts": reject_counts,
        "missing_fields": missing_fields,
        "reject_reasons": reject_reasons,
    }
    return {
        "stage": "S5_5",
        "status": "SUCCESS" if counts["PASS"] == len(rows) else "PARTIAL",
        "input_symbols": requested_symbols,
        "output_symbols": output_symbols,
        "rows": rows,
        "summary": summary,
        "counts": counts,
        "missing_fields": missing_fields,
        "reject_reasons": reject_reasons,
        "source_provenance": source_provenance,
        "meta": meta,
    }


def _validate_arguments(
    *,
    symbols: list[str],
    quotes: list[dict],
    context: ResearchContext,
    config: PipelineConfig,
    observed_at: datetime,
    snapshot_id: str,
    benchmark_quote: dict | None,
) -> datetime:
    if not isinstance(context, ResearchContext):
        raise PipelineError("INVALID_ARGUMENTS", "realtime context must be a ResearchContext")
    if not isinstance(config, PipelineConfig):
        raise PipelineError("INVALID_ARGUMENTS", "realtime config must be a PipelineConfig")
    if not isinstance(symbols, list) or not symbols:
        raise PipelineError("IDENTITY_CONFLICT", "realtime symbols must be a nonempty list")
    if any(not isinstance(symbol, str) for symbol in symbols):
        raise PipelineError("IDENTITY_CONFLICT", "realtime symbols must be canonical strings")
    if len(symbols) != len(set(symbols)):
        raise PipelineError("IDENTITY_CONFLICT", "realtime symbols contain duplicates")
    for symbol in symbols:
        try:
            CanonicalSymbol.from_core(symbol)
        except SymbolContractError as exc:
            raise PipelineError("IDENTITY_CONFLICT", "realtime symbol is not canonical") from exc
    if not isinstance(quotes, list):
        raise PipelineError("INVALID_ARGUMENTS", "realtime quotes must be a list")
    if any(not isinstance(quote, dict) for quote in quotes):
        raise PipelineError("IDENTITY_CONFLICT", "realtime quote rows must be mappings")
    if benchmark_quote is not None and not isinstance(benchmark_quote, dict):
        raise PipelineError("INVALID_ARGUMENTS", "benchmark quote must be a mapping or None")
    if not isinstance(observed_at, datetime):
        raise PipelineError("NOT_READY", "observed_at must be timezone-aware")
    observed_local = _parse_full_aware_datetime(observed_at)
    if observed_local is None:
        raise PipelineError("NOT_READY", "observed_at must be timezone-aware")
    if observed_local.date() != context.research_date:
        raise PipelineError("NOT_READY", "observed_at date must equal research_date")
    context_as_of = _parse_full_aware_datetime(context.as_of)
    if context_as_of is None or context_as_of > observed_local:
        raise PipelineError("NOT_READY", "research cutoff exceeds observed_at")
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise PipelineError("INVALID_ARGUMENTS", "snapshot_id must be nonempty")
    return observed_local


def _index_quotes(
    quotes: list[dict], requested_symbols: list[str]
) -> dict[str, dict[str, Any]]:
    requested = set(requested_symbols)
    indexed: dict[str, dict[str, Any]] = {}
    for quote in quotes:
        symbol = quote.get("symbol", _MISSING)
        if not isinstance(symbol, str):
            raise PipelineError("IDENTITY_CONFLICT", "realtime quote identity is missing")
        try:
            CanonicalSymbol.from_core(symbol)
        except SymbolContractError as exc:
            raise PipelineError("IDENTITY_CONFLICT", "realtime quote identity is not canonical") from exc
        if symbol not in requested:
            raise PipelineError("IDENTITY_CONFLICT", "realtime quote contains an unknown symbol")
        if symbol in indexed:
            raise PipelineError("IDENTITY_CONFLICT", "realtime quote contains duplicate symbols")
        indexed[symbol] = deepcopy(quote)
    return indexed


def _inspect_quote(
    quote: dict[str, Any],
    *,
    research_date: date,
    observed_at: datetime,
    max_age: int,
    execution_window_open: bool,
    check_execution_fields: bool,
    realtime_config: dict[str, Any],
) -> _QuoteView:
    view = _QuoteView(deepcopy(quote))
    view.fallback_used = _active_marker(quote, ("fallback_used", "fallback"))
    view.stale = _active_marker(quote, ("stale", "is_stale"))
    if view.fallback_used:
        _downgrade(view, "fallback_quote")
    if view.stale:
        _downgrade(view, "stale_marker")

    source = quote.get("source", _MISSING)
    if _is_absent(source):
        _missing(view, "source", "missing_source")
    elif not isinstance(source, str) or not source.strip():
        _reject(view, "invalid_source")
    else:
        view.source = source.strip()
    _validate_status_flags(view, quote)

    raw_server_time = quote.get("server_time", _MISSING)
    if _is_absent(raw_server_time):
        _missing(view, "server_time", "missing_server_time")
    else:
        view.server_time = _parse_full_aware_datetime(raw_server_time)
        if view.server_time is None:
            _missing(view, "server_time", "server_time_not_full_aware")
        else:
            if view.server_time.date() != research_date:
                _missing(view, "server_time", "server_time_date_mismatch_research_date")
            if view.server_time > observed_at + _MAX_PROVIDER_CLOCK_SKEW:
                view.future = True
                _reject(view, "future_server_time")

    raw_trade_date = quote.get("trade_date", _MISSING)
    if _is_absent(raw_trade_date):
        _missing(view, "trade_date", "missing_trade_date")
    else:
        view.trade_date = _parse_trade_date(raw_trade_date)
        if view.trade_date is None:
            _missing(view, "trade_date", "invalid_trade_date")
        elif view.trade_date != research_date:
            _missing(view, "trade_date", "trade_date_mismatch_research_date")
    if (
        view.server_time is not None
        and view.trade_date is not None
        and view.server_time.date() != view.trade_date
    ):
        _missing(view, "server_time", "server_time_trade_date_conflict")
        _missing(view, "trade_date", "server_time_trade_date_conflict")

    timestamp_quality = quote.get("timestamp_quality", _MISSING)
    if timestamp_quality != "verified":
        _missing(view, "timestamp_quality", "timestamp_quality_not_verified")

    if view.server_time is not None and not view.future:
        if view.server_time.date() == research_date:
            view.timestamp_valid = (
                view.trade_date == research_date
                and timestamp_quality == "verified"
            )
    if view.server_time is not None and not view.future:
        age = max(0.0, (observed_at - view.server_time).total_seconds())
        if age > max_age:
            view.stale = True
            _downgrade(view, "quote_age_exceeds_limit")

    raw_received_at = quote.get("received_at", _MISSING)
    if raw_received_at is not _MISSING:
        if _is_absent(raw_received_at):
            _reject(view, "invalid_received_at")
        else:
            view.received_at = _parse_full_aware_datetime(raw_received_at)
            if view.received_at is None:
                _reject(view, "invalid_received_at")
            else:
                if view.server_time is not None and view.received_at < view.server_time:
                    provider_ahead_within_tolerance = (
                        observed_at < view.server_time <= observed_at + _MAX_PROVIDER_CLOCK_SKEW
                        and view.received_at + _MAX_PROVIDER_CLOCK_SKEW >= view.server_time
                    )
                    if not provider_ahead_within_tolerance:
                        _reject(view, "received_at_before_server_time")
                if view.received_at > observed_at:
                    _reject(view, "received_at_after_observed_at")

    for field_name in _PRICE_FIELDS:
        raw_value = quote.get(field_name, _MISSING)
        if _is_absent(raw_value):
            _missing(view, field_name, f"missing_{field_name}")
            continue
        number = _finite_number(raw_value)
        if number is None or number <= 0:
            _reject(view, f"invalid_{field_name}")
            continue
        view.prices[field_name] = number

    raw_amount_unit = quote.get("amount_unit", _MISSING)
    if _is_absent(raw_amount_unit):
        _missing(view, "amount_unit", "missing_amount_unit")
    elif raw_amount_unit != "CNY":
        _reject(view, "invalid_amount_unit")
    else:
        view.amount_unit = "CNY"

    raw_volume_unit = quote.get("volume_unit", _MISSING)
    if _is_absent(raw_volume_unit):
        _missing(view, "volume_unit", "missing_volume_unit")
    elif not isinstance(raw_volume_unit, str) or raw_volume_unit not in {"lots", "shares"}:
        _reject(view, "invalid_volume_unit")
    else:
        view.volume_unit = raw_volume_unit

    view.amount = _optional_nonnegative(view, quote, "amount")
    view.volume = _optional_nonnegative(view, quote, "volume")

    view.bid = _optional_quote_number(view, quote, ("bid", "bid1"), "bid")
    view.ask = _optional_quote_number(view, quote, ("ask", "ask1"), "ask")
    if view.bid is not None and view.ask is not None and view.ask < view.bid:
        _reject(view, "crossed_bid_ask")
    if check_execution_fields:
        if view.bid is None:
            _downgrade(view, "missing_bid")
            _missing(view, "bid", "missing_bid", core=False)
        elif view.bid == 0:
            _downgrade(view, "zero_bid")
        if view.ask is None:
            _downgrade(view, "missing_ask")
            _missing(view, "ask", "missing_ask", core=False)
        elif view.ask == 0:
            _downgrade(view, "zero_ask")

    if all(field_name in view.prices for field_name in _PRICE_FIELDS):
        price = view.prices["price"]
        opening = view.prices["open"]
        high = view.prices["high"]
        low = view.prices["low"]
        if high < max(price, opening, low) or low > min(price, opening, high):
            _reject(view, "ohlc_price_inconsistent")

    view.core_ready = (
        not view.core_missing
        and not view.hard_invalid
        and view.timestamp_valid
        and view.source is not None
        and view.amount_unit == "CNY"
        and view.volume_unit in {"lots", "shares"}
        and all(field_name in view.prices for field_name in _PRICE_FIELDS)
        and not _has_reason(view, "ohlc_price_inconsistent")
    )

    if check_execution_fields:
        if not execution_window_open:
            _downgrade(view, "outside_continuous_auction")
        elif view.server_time is not None and not _in_continuous_auction(view.server_time.time()):
            _downgrade(view, "quote_outside_continuous_auction")

    metrics = _metrics_for_view(view, observed_at)
    _validate_trade_totals(view, metrics)
    gap = metrics["gap"]
    if gap is not None and _exceeds_limit(abs(gap), float(realtime_config["max_gap"])):
        _downgrade(view, "gap_exceeds_limit")
    drawdown = metrics["drawdown_from_high"]
    if (
        drawdown is not None
        and _exceeds_limit(drawdown, float(realtime_config["max_drawdown_from_high"]))
    ):
        _downgrade(view, "drawdown_exceeds_limit")
    if check_execution_fields:
        if view.amount is None or view.volume is None:
            _missing(view, "session_vwap", "session_vwap_incomplete_trade_totals", core=False)
            _downgrade(view, "session_vwap_unavailable")
        elif metrics["session_vwap"] is None:
            _missing(view, "session_vwap", "session_vwap_unavailable", core=False)
            _downgrade(view, "session_vwap_unavailable")
    return view


def _build_row(
    symbol: str,
    view: _QuoteView,
    *,
    context: ResearchContext,
    observed_at: datetime,
    snapshot_id: str,
    benchmark_view: _QuoteView | None,
    benchmark_reason: str | None,
) -> dict[str, Any]:
    metrics = _metrics_for_view(view, observed_at)
    if benchmark_reason is not None:
        _missing(view, "intraday_relative_strength", benchmark_reason, core=False)
    elif benchmark_view is None:
        _missing(view, "intraday_relative_strength", "benchmark_quote_unavailable", core=False)
    elif not _benchmark_usable(benchmark_view):
        _missing(view, "intraday_relative_strength", "benchmark_quote_invalid", core=False)
    elif not view.core_ready or view.server_time != benchmark_view.server_time:
        _missing(view, "intraday_relative_strength", "benchmark_timestamp_mismatch", core=False)
    else:
        benchmark_metrics = _metrics_for_view(benchmark_view, observed_at)
        stock_change = metrics["change_vs_prev_close"]
        benchmark_change = benchmark_metrics["change_vs_prev_close"]
        if stock_change is None or benchmark_change is None:
            _missing(view, "intraday_relative_strength", "benchmark_relative_strength_unavailable", core=False)
        else:
            metrics["intraday_relative_strength"] = stock_change - benchmark_change

    if view.hard_invalid:
        gate = "REJECT"
    elif not view.core_ready:
        gate = "MISSING_REALTIME"
    elif view.downgrade_reasons:
        gate = "DOWNGRADE"
    else:
        gate = "PASS"
    execution_eligible = gate == "PASS"
    row_meta = _row_meta(view, context=context, observed_at=observed_at, snapshot_id=snapshot_id)
    row: dict[str, Any] = {
        "symbol": symbol,
        "realtime_gate": gate,
        "quote": deepcopy(view.quote),
        "metrics": metrics,
        "missing_fields": sorted(set(view.missing_fields)),
        "reasons": sorted(set(view.reasons)),
        "reject_reasons": sorted(set(view.reject_reasons)),
        "execution_eligible": execution_eligible,
        "selected": execution_eligible,
        "meta": row_meta,
    }
    return row


def _missing_quote_row(
    symbol: str,
    *,
    context: ResearchContext,
    observed_at: datetime,
    snapshot_id: str,
    benchmark_reason: str | None,
) -> dict[str, Any]:
    missing = ["quote", "server_time", "trade_date", "timestamp_quality", *_PRICE_FIELDS,
               "amount_unit", "volume_unit"]
    reasons = ["missing_quote"]
    missing.append("intraday_relative_strength")
    reasons.append(benchmark_reason or "benchmark_quote_unavailable")
    row_meta = {
        "research_date": context.research_date.isoformat(),
        "phase": context.phase,
        "market_data_reference_date": context.market_data_reference_date.isoformat(),
        "as_of": context.as_of.isoformat(),
        "source": None,
        "fallback_used": False,
        "stale": False,
        "snapshot_id": snapshot_id,
        "observed_at": observed_at.isoformat(),
        "server_time": None,
        "trade_date": None,
        "received_at": None,
        "timestamp_quality": None,
        "timestamp_source": None,
    }
    return {
        "symbol": symbol,
        "realtime_gate": "MISSING_REALTIME",
        "quote": None,
        "metrics": _empty_metrics(),
        "missing_fields": sorted(set(missing)),
        "reasons": reasons,
        "reject_reasons": [],
        "execution_eligible": False,
        "selected": False,
        "meta": row_meta,
    }


def _metrics_for_view(view: _QuoteView, observed_at: datetime) -> dict[str, float | None]:
    metrics = _empty_metrics()
    prices = view.prices
    if all(field_name in prices for field_name in _PRICE_FIELDS):
        price = prices["price"]
        prev_close = prices["prev_close"]
        opening = prices["open"]
        high = prices["high"]
        low = prices["low"]
        price_range = high - low
        metrics["change_vs_prev_close"] = _safe_ratio(price, prev_close, subtract_one=True)
        metrics["change_vs_open"] = _safe_ratio(price, opening, subtract_one=True)
        metrics["gap"] = _safe_ratio(opening, prev_close, subtract_one=True)
        metrics["range"] = price_range if math.isfinite(price_range) else None
        if price_range != 0 and math.isfinite(price_range):
            metrics["position_in_intraday_range"] = (price - low) / price_range
        metrics["drawdown_from_high"] = 1.0 - price / high
        metrics["recovery_from_low"] = price / low - 1.0
        for key in (
            "position_in_intraday_range",
            "drawdown_from_high",
            "recovery_from_low",
        ):
            if metrics[key] is not None and not math.isfinite(metrics[key]):
                metrics[key] = None

    if view.server_time is not None and not view.future:
        age = max(0.0, (observed_at - view.server_time).total_seconds())
        metrics["quote_age_seconds"] = age if math.isfinite(age) else None
    if view.bid is not None:
        metrics["bid"] = view.bid
    if view.ask is not None:
        metrics["ask"] = view.ask
    if view.bid is not None and view.ask is not None:
        spread = view.ask - view.bid
        metrics["bid_ask_spread"] = spread if math.isfinite(spread) else None
    if view.volume is not None and view.volume_unit in {"lots", "shares"}:
        multiplier = _VOLUME_LOT_SHARES if view.volume_unit == "lots" else 1
        volume_shares = view.volume * multiplier
        metrics["volume_shares"] = (
            volume_shares if math.isfinite(volume_shares) else None
        )
    if view.amount is not None and view.amount_unit == "CNY":
        metrics["amount_cny"] = view.amount
    if (
        view.amount is not None and view.amount > 0
        and view.volume is not None
        and view.amount_unit == "CNY"
        and metrics["volume_shares"] is not None
        and metrics["volume_shares"] > 0
    ):
        vwap = view.amount / metrics["volume_shares"]
        # Raw reported totals remain in quote/amount_cny. An inconsistent
        # implied average is not a usable VWAP, even when division is finite.
        valid_range = ("low" not in prices or "high" not in prices
                       or not _outside_range(vwap, prices["low"], prices["high"]))
        metrics["session_vwap"] = vwap if math.isfinite(vwap) and valid_range else None
    return metrics


def _validate_trade_totals(
    view: _QuoteView, metrics: dict[str, float | None]
) -> None:
    """Reject derived totals that overflow or imply an impossible VWAP."""

    if (
        view.amount is None
        or view.volume is None
        or view.amount_unit != "CNY"
        or view.volume_unit not in {"lots", "shares"}
    ):
        return
    if view.volume <= 0:
        if view.amount > 0:
            _reject(view, "trade_totals_price_inconsistent")
        return
    volume_shares = metrics["volume_shares"]
    session_vwap = metrics["session_vwap"]
    if volume_shares is None:
        _reject(view, "trade_totals_overflow")
        return
    if view.amount == 0:
        _reject(view, "trade_totals_price_inconsistent")
        return
    if session_vwap is None:
        _reject(view, "trade_totals_price_inconsistent")
        return
    if all(field_name in view.prices for field_name in _PRICE_FIELDS):
        low = view.prices["low"]
        high = view.prices["high"]
        if _outside_range(session_vwap, low, high):
            _reject(view, "trade_totals_price_inconsistent")


def _outside_range(value: float, low: float, high: float) -> bool:
    tolerance = max(abs(low), abs(high), 1.0) * 1e-12
    return value < low - tolerance or value > high + tolerance


def _empty_metrics() -> dict[str, float | None]:
    return {
        "change_vs_prev_close": None,
        "change_vs_open": None,
        "gap": None,
        "range": None,
        "position_in_intraday_range": None,
        "drawdown_from_high": None,
        "recovery_from_low": None,
        "bid": None,
        "ask": None,
        "bid_ask_spread": None,
        "amount_cny": None,
        "volume_shares": None,
        "session_vwap": None,
        "quote_age_seconds": None,
        "intraday_relative_strength": None,
    }


def _benchmark_usable(view: _QuoteView) -> bool:
    return (
        view.core_ready
        and not view.hard_invalid
        and not view.future
        and not view.stale
        and not view.downgrade_reasons
    )


def _row_meta(
    view: _QuoteView,
    *,
    context: ResearchContext,
    observed_at: datetime,
    snapshot_id: str,
) -> dict[str, Any]:
    return {
        "research_date": context.research_date.isoformat(),
        "phase": context.phase,
        "market_data_reference_date": context.market_data_reference_date.isoformat(),
        "as_of": context.as_of.isoformat(),
        "source": view.source,
        "fallback_used": view.fallback_used,
        "stale": view.stale,
        "snapshot_id": snapshot_id,
        "observed_at": observed_at.isoformat(),
        "server_time": view.server_time.isoformat() if view.server_time is not None else None,
        "trade_date": view.trade_date.isoformat() if view.trade_date is not None else None,
        "received_at": view.received_at.isoformat() if view.received_at is not None else None,
        "timestamp_quality": view.quote.get("timestamp_quality"),
        "timestamp_source": view.quote.get("timestamp_source"),
    }


def _provenance(symbol: str, row: dict[str, Any]) -> dict[str, Any]:
    row_meta = row["meta"]
    return {
        "symbol": symbol,
        "source": row_meta["source"],
        "timestamp_quality": row_meta["timestamp_quality"],
        "timestamp_source": row_meta["timestamp_source"],
        "server_time": row_meta["server_time"],
        "trade_date": row_meta["trade_date"],
        "received_at": row_meta["received_at"],
        "fallback_used": row_meta["fallback_used"],
        "stale": row_meta["stale"],
        "realtime_gate": row["realtime_gate"],
    }


def _validate_status_flags(view: _QuoteView, quote: dict[str, Any]) -> None:
    tradeable = _explicit_bool_flag(view, quote, "tradeable")
    if tradeable is False:
        _reject(view, "not_tradeable")

    suspension_values = [
        (field_name, _explicit_bool_flag(view, quote, field_name))
        for field_name in ("is_suspended", "suspended")
        if field_name in quote
    ]
    known_suspension_values = [value for _name, value in suspension_values if value is not None]
    if len(known_suspension_values) == 2 and known_suspension_values[0] != known_suspension_values[1]:
        _reject(view, "suspension_flag_conflict")
    if any(value is True for value in known_suspension_values):
        _reject(view, "suspended_flag")

    limit_up = _explicit_bool_flag(view, quote, "is_limit_up")
    limit_down = _explicit_bool_flag(view, quote, "is_limit_down")
    if limit_up is True and limit_down is True:
        _reject(view, "limit_flag_conflict")
    if limit_up is True:
        _downgrade(view, "limit_up_flag")
    if limit_down is True:
        _downgrade(view, "limit_down_flag")


def _explicit_bool_flag(
    view: _QuoteView, quote: dict[str, Any], field_name: str
) -> bool | None:
    if field_name not in quote:
        return None
    value = quote[field_name]
    if type(value) is not bool:
        _reject(view, f"invalid_{field_name}_flag")
        return None
    return value


def _optional_nonnegative(
    view: _QuoteView, quote: dict[str, Any], field_name: str
) -> float | None:
    raw_value = quote.get(field_name, _MISSING)
    if _is_absent(raw_value):
        _missing(view, field_name, f"missing_{field_name}", core=False)
        return None
    number = _finite_number(raw_value)
    if number is None or number < 0:
        _reject(view, f"invalid_{field_name}")
        return None
    return number


def _optional_quote_number(
    view: _QuoteView,
    quote: dict[str, Any],
    aliases: tuple[str, ...],
    normalized_name: str,
) -> float | None:
    values: list[float] = []
    for alias in aliases:
        if alias not in quote or _is_absent(quote[alias]):
            continue
        number = _finite_number(quote[alias])
        if number is None or number < 0:
            _reject(view, f"invalid_{normalized_name}")
            return None
        values.append(number)
    if not values:
        return None
    if len(values) > 1 and values[0] != values[1]:
        _reject(view, f"{normalized_name}_alias_conflict")
        return None
    return values[0]


def _missing(
    view: _QuoteView, field_name: str, reason: str, *, core: bool = True
) -> None:
    if field_name not in view.missing_fields:
        view.missing_fields.append(field_name)
    if core:
        view.core_missing = True
    _reason(view, reason)


def _reject(view: _QuoteView, reason: str) -> None:
    view.hard_invalid = True
    if reason not in view.reject_reasons:
        view.reject_reasons.append(reason)
    _reason(view, reason)


def _downgrade(view: _QuoteView, reason: str) -> None:
    if reason not in view.downgrade_reasons:
        view.downgrade_reasons.append(reason)
    _reason(view, reason)


def _reason(view: _QuoteView, reason: str) -> None:
    if reason not in view.reasons:
        view.reasons.append(reason)


def _has_reason(view: _QuoteView, reason: str) -> bool:
    return reason in view.reasons


def _active_marker(quote: dict[str, Any], fields: tuple[str, ...]) -> bool:
    for field_name in fields:
        if field_name not in quote:
            continue
        value = quote[field_name]
        if type(value) is bool:
            if value:
                return True
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                numeric = float(value)
            except (OverflowError, TypeError, ValueError):
                return True
            if not math.isfinite(numeric) or value != 0:
                return True
            continue
        if not _is_absent(value):
            # Non-boolean marker values are treated as active, never as a
            # silently cleared safety flag.
            return True
    return False


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _safe_ratio(numerator: float, denominator: float, *, subtract_one: bool) -> float | None:
    try:
        value = numerator / denominator
        if subtract_one:
            value -= 1.0
    except (OverflowError, ZeroDivisionError):
        return None
    return value if math.isfinite(value) else None


def _exceeds_limit(value: float, limit: float) -> bool:
    """Compare configured decimal limits without rejecting an exact boundary."""

    return value > limit and not math.isclose(value, limit, rel_tol=1e-12, abs_tol=1e-12)


def _parse_full_aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        candidate = value
    elif isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if not any(separator in text for separator in ("T", "t", " ")):
            return None
        try:
            candidate = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    try:
        if candidate.tzinfo is None or candidate.utcoffset() is None:
            return None
        return candidate.astimezone(SHANGHAI)
    except (OverflowError, ValueError):
        return None


def _parse_trade_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError:
        return None


def _is_absent(value: Any) -> bool:
    if value is _MISSING or value is None:
        return True
    return isinstance(value, str) and not value.strip()


def _in_continuous_auction(value: time) -> bool:
    return any(start <= value < end for start, end in _EXECUTION_WINDOWS)

"""Deterministic whole-market candidate ranking over one Universe snapshot."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from competition_core.config import CompetitionConfig
from competition_core.universe.service import CompetitionUniverse

SORT_KEYS = frozenset(
    {"amount", "turnover", "volume_ratio", "momentum_5d", "momentum_10d", "momentum_20d"}
)
_MOMENTUM_WINDOWS = {"momentum_5d": 5, "momentum_10d": 10, "momentum_20d": 20}


def screen_competition_universe(
    *,
    client: Any,
    config: CompetitionConfig,
    as_of: str,
    phase: str,
    sort_by: str,
    limit: int,
) -> dict[str, Any]:
    if sort_by not in SORT_KEYS:
        raise ValueError(f"sort_by must be one of {sorted(SORT_KEYS)}")
    maximum = int(config.candidate_limits["shortlist"])
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= maximum:
        raise ValueError(f"limit must be an integer between 1 and {maximum}")

    snapshot = CompetitionUniverse(client=client, config=config).build(as_of, phase)
    reference_date = snapshot.market_data_reference_date
    trade_date = reference_date.strftime("%Y%m%d")
    basic = client.daily_basic(
        trade_date=trade_date,
        fields="ts_code,trade_date,turnover_rate,volume_ratio",
    )
    basic_by_symbol = _rows_by_symbol(basic)
    max_window = _MOMENTUM_WINDOWS.get(sort_by, 0)
    momentum = (
        _momentum_by_symbol(client, reference_date, max_window)
        if max_window
        else {}
    )

    rows: list[dict[str, Any]] = []
    for member in snapshot.eligible_members:
        daily_basic = basic_by_symbol.get(member.ts_code, {})
        rows.append(
            {
                "symbol": member.ts_code,
                "name": member.name,
                "board": member.board.value,
                "amount_cny": member.recent_amount_cny,
                "turnover_rate": _finite_float(daily_basic.get("turnover_rate")),
                "volume_ratio": _finite_float(daily_basic.get("volume_ratio")),
                **momentum.get(member.ts_code, {}),
            }
        )

    metric_key = {"amount": "amount_cny", "turnover": "turnover_rate"}.get(
        sort_by, sort_by
    )
    warnings = list(snapshot.warnings)
    if sort_by in _MOMENTUM_WINDOWS:
        missing_momentum = sum(row.get(metric_key) is None for row in rows)
        warnings.append(f"missing_momentum={missing_momentum}")
        rows = [row for row in rows if row.get(metric_key) is not None]
    rows.sort(
        key=lambda row: (
            row.get(metric_key) is not None,
            row.get(metric_key) if row.get(metric_key) is not None else float("-inf"),
            row["symbol"],
        ),
        reverse=True,
    )
    selected = rows[:limit]
    return {
        "date": snapshot.as_of.isoformat(),
        "phase": snapshot.snapshot_phase,
        "market_data_reference_date": reference_date.isoformat(),
        "snapshot_id": snapshot.snapshot_id,
        "counts": snapshot.counts(),
        "sort_by": sort_by,
        "sort_order": "descending",
        "limit": limit,
        "returned": len(selected),
        "rows": selected,
        "warnings": warnings,
    }


def _open_dates(client: Any, end_date: date, sessions: int) -> list[str]:
    calendar = client.trade_cal(
        exchange="SSE",
        start_date=(end_date - timedelta(days=max(45, sessions * 3))).strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    if calendar is None or calendar.empty:
        raise RuntimeError("tushare.trade_cal returned no rows for momentum")
    dates = sorted(
        {
            parsed.strftime("%Y%m%d")
            for row in calendar.to_dict("records")
            if _is_open(row.get("is_open"))
            if (parsed := _parse_trade_date(row.get("cal_date"))) is not None
            if parsed <= end_date
        }
    )
    if len(dates) < sessions:
        raise RuntimeError(f"only {len(dates)} open sessions available; need {sessions}")
    if dates[-1] != end_date.strftime("%Y%m%d"):
        raise RuntimeError("trade calendar does not include the momentum as-of date")
    return dates[-sessions:]


def _momentum_by_symbol(
    client: Any, reference_date: date, max_window: int
) -> dict[str, dict[str, float | None]]:
    if max_window <= 0:
        return {}

    dates = _open_dates(client, reference_date, max_window + 1)
    closes, observed_symbols = _daily_closes_by_symbol(client, dates)
    factors, invalid_factor_symbols = _adj_factors_by_symbol(
        client, dates, observed_symbols, reference_date
    )
    labels = tuple(
        label for label, window in _MOMENTUM_WINDOWS.items() if window <= max_window
    )

    result: dict[str, dict[str, float | None]] = {}
    for symbol in sorted(observed_symbols):
        metrics = {label: None for label in labels}
        history = closes.get(symbol, {})
        factor_history = factors.get(symbol, {})
        if symbol in invalid_factor_symbols:
            result[symbol] = metrics
            continue
        if any(trade_date not in history for trade_date in dates):
            result[symbol] = metrics
            continue
        if any(trade_date not in factor_history for trade_date in dates):
            result[symbol] = metrics
            continue

        anchor_factor = factor_history[dates[-1]]
        qfq_history: dict[str, float] = {}
        for trade_date in dates:
            qfq_price = _qfq_price(
                history[trade_date], factor_history[trade_date], anchor_factor
            )
            if qfq_price is None:
                qfq_history = {}
                break
            qfq_history[trade_date] = qfq_price

        if len(qfq_history) == len(dates):
            current = qfq_history[dates[-1]]
            for label, window in _MOMENTUM_WINDOWS.items():
                if window <= max_window:
                    previous = qfq_history[dates[-(window + 1)]]
                    momentum = current / previous - 1.0
                    metrics[label] = momentum if math.isfinite(momentum) else None
        result[symbol] = metrics
    return result


def _daily_closes_by_symbol(
    client: Any, dates: list[str]
) -> tuple[dict[str, dict[str, float]], set[str]]:
    closes: dict[str, dict[str, float]] = {}
    observed_symbols: set[str] = set()
    invalid_keys: set[tuple[str, str]] = set()

    for trade_date in dates:
        try:
            frame = client.daily(
                trade_date=trade_date, fields="ts_code,trade_date,close"
            )
        except Exception:  # noqa: BLE001 - missing history is not raw-price fallback
            continue
        if frame is None or frame.empty or not {"ts_code", "trade_date", "close"}.issubset(
            frame.columns
        ):
            continue

        seen: set[tuple[str, str]] = set()
        for row in frame.to_dict("records"):
            symbol = _symbol_key(row.get("ts_code"))
            if symbol is None:
                continue
            observed_symbols.add(symbol)
            if _date_key(row.get("trade_date")) != trade_date:
                continue
            key = (symbol, trade_date)
            if key in seen:
                invalid_keys.add(key)
                continue
            seen.add(key)
            close = _positive_finite_float(row.get("close"))
            if close is None:
                invalid_keys.add(key)
                continue
            closes.setdefault(symbol, {})[trade_date] = close

    for symbol, trade_date in invalid_keys:
        closes.get(symbol, {}).pop(trade_date, None)
    return closes, observed_symbols


def _adj_factors_by_symbol(
    client: Any,
    dates: list[str],
    observed_symbols: set[str],
    as_of: date,
) -> tuple[dict[str, dict[str, float]], set[str]]:
    factors: dict[str, dict[str, float]] = {}
    invalid_symbols: set[str] = set()
    adj_factor = getattr(client, "adj_factor", None)
    if not callable(adj_factor):
        return factors, invalid_symbols

    for trade_date in dates:
        try:
            frame = adj_factor(
                trade_date=trade_date,
                fields="ts_code,trade_date,adj_factor",
            )
        except Exception:  # noqa: BLE001 - qfq must fail closed on provider errors
            continue
        if frame is None or frame.empty or not {
            "ts_code", "trade_date", "adj_factor"
        }.issubset(frame.columns):
            continue

        seen: set[tuple[str, str]] = set()
        for row in frame.to_dict("records"):
            symbol = _symbol_key(row.get("ts_code"))
            if symbol is None or symbol not in observed_symbols:
                continue
            parsed_date = _parse_trade_date(row.get("trade_date"))
            if parsed_date is None or parsed_date > as_of:
                continue
            returned_date = parsed_date.strftime("%Y%m%d")
            if returned_date != trade_date:
                continue
            key = (symbol, returned_date)
            if key in seen or returned_date in factors.get(symbol, {}):
                invalid_symbols.add(symbol)
                continue
            seen.add(key)
            factor = _positive_finite_float(row.get("adj_factor"))
            if factor is None:
                invalid_symbols.add(symbol)
                continue
            factors.setdefault(symbol, {})[returned_date] = factor

    return factors, invalid_symbols


def _qfq_price(close: float, factor: float, anchor_factor: float) -> float | None:
    if not all(math.isfinite(value) and value > 0 for value in (close, factor, anchor_factor)):
        return None
    adjusted = close * (factor / anchor_factor)
    return adjusted if math.isfinite(adjusted) and adjusted > 0 else None


def _rows_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty or "ts_code" not in frame.columns:
        return {}
    return {
        str(row["ts_code"]).upper(): row
        for row in frame.to_dict("records")
        if row.get("ts_code")
    }


def _symbol_key(value: Any) -> str | None:
    if value is None:
        return None
    symbol = str(value).strip().upper()
    return symbol if symbol and symbol not in {"NAN", "NAT", "NONE"} else None


def _parse_trade_date(value: Any) -> date | None:
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None
        value = int(value)
    text = str(value).strip().replace("-", "")
    if not text or text.upper() in {"NAN", "NAT", "NONE"}:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _date_key(value: Any) -> str | None:
    parsed = _parse_trade_date(value)
    return parsed.strftime("%Y%m%d") if parsed is not None else None


def _is_open(value: Any) -> bool:
    try:
        return int(value) == 1
    except (TypeError, ValueError):
        return False


def _positive_finite_float(value: Any) -> float | None:
    number = _finite_float(value)
    return number if number is not None and number > 0 else None


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


__all__ = ["SORT_KEYS", "screen_competition_universe"]

"""Compact mootdx quote and intraday-bar adapter."""

from __future__ import annotations

import math
from datetime import date, datetime, time
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd

from market_runtime.symbols import MarketSymbol

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
FREQUENCIES = {"1m": 8, "5m": 0, "15m": 1, "30m": 2, "1h": 3}


class MootdxProvider:
    def __init__(
        self,
        client: Any | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if client is None:
            from mootdx.quotes import Quotes

            client = Quotes.factory(market="std")
        self.client = client
        self._clock = clock or (lambda: datetime.now(SHANGHAI_TZ))

    def quotes(self, symbols: list[MarketSymbol]) -> list[dict[str, Any]]:
        frame = self.client.quotes(symbol=[item.code for item in symbols])
        if frame is None or frame.empty:
            raise RuntimeError("mootdx returned no quotes")
        received_at = _local_datetime(self._clock())
        requested = {item.code: item for item in symbols}
        results: list[dict[str, Any]] = []
        for row in frame.to_dict("records"):
            code = str(row.get("code", ""))
            identity = requested.get(code)
            if identity is None:
                continue
            market = int(row.get("market", -1))
            if market != identity.mootdx_market:
                raise RuntimeError(f"mootdx exchange conflict for {identity.canonical}")
            timestamp = _quote_timestamp(row, received_at=received_at)
            results.append(
                {
                    "symbol": identity.canonical,
                    # Keep the legacy field, but never promote an unverified
                    # timestamp to an apparently fresh observation.
                    "as_of": timestamp["as_of"],
                    "raw_server_time": timestamp["raw_server_time"],
                    "server_time": timestamp["server_time"],
                    "trade_date": timestamp["trade_date"],
                    "timestamp_quality": timestamp["timestamp_quality"],
                    "timestamp_source": timestamp["timestamp_source"],
                    "received_at": timestamp["received_at"],
                    "price": _number(row.get("price")),
                    "prev_close": _number(row.get("last_close")),
                    "open": _number(row.get("open")),
                    "high": _number(row.get("high")),
                    "low": _number(row.get("low")),
                    "bid1": _number(row.get("bid1")),
                    "ask1": _number(row.get("ask1")),
                    "volume": _number(row.get("volume")),
                    "volume_unit": "lots",
                    "amount": _number(row.get("amount")),
                    "amount_unit": "CNY",
                    "source": "mootdx",
                }
            )
        return results

    def bars(
        self, symbol: MarketSymbol, interval: str, offset: int
    ) -> list[dict[str, Any]]:
        if interval not in FREQUENCIES:
            raise ValueError(f"unsupported interval: {interval}")
        frame = self.client.bars(
            symbol=symbol.code, frequency=FREQUENCIES[interval], start=0, offset=offset
        )
        normalized = normalize_bars(frame)
        return [
            {
                "symbol": symbol.canonical,
                "datetime": timestamp.isoformat(),
                "open": _number(row.get("open")),
                "high": _number(row.get("high")),
                "low": _number(row.get("low")),
                "close": _number(row.get("close")),
                "volume": _number(row.get("volume", row.get("vol"))),
                # mootdx quote snapshots report cumulative volume in lots, while
                # its intraday bars report per-bar volume in shares.
                "volume_unit": "shares",
                "amount": _number(row.get("amount")),
                "amount_unit": "CNY",
                "source": "mootdx",
            }
            for timestamp, row in normalized.iterrows()
        ]


def normalize_bars(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    result = frame.copy()
    if "datetime" in result.columns:
        timestamps = pd.to_datetime(result["datetime"], errors="raise")
    else:
        timestamps = pd.to_datetime(result.index, errors="raise")
    result.index = pd.DatetimeIndex(timestamps)
    result.index.name = "datetime"
    result = result.sort_index()
    result.index = _normalize_noon_boundary(result.index)
    return result[~result.index.duplicated(keep="last")].sort_index()


def _normalize_noon_boundary(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    adjusted = list(index)
    dates = sorted({item.date() for item in index})
    for day in dates:
        positions = [position for position, item in enumerate(index) if item.date() == day]
        day_times = {index[position].time() for position in positions}
        has_morning = any(item < time(12, 0) for item in day_times)
        if not has_morning or time(11, 30) in day_times or time(13, 0) not in day_times:
            continue
        for position in positions:
            if index[position].time() == time(13, 0):
                adjusted[position] = index[position].replace(hour=11, minute=30)
                break
    return pd.DatetimeIndex(adjusted, name=index.name)


def _quote_timestamp(
    row: dict[str, Any], *, received_at: datetime
) -> dict[str, str | None]:
    """Describe a quote timestamp without inventing a trading date.

    mootdx 0.11.7 exposes ``servertime`` as ``HH:MM:SS.sss`` from the
    TongDaXin quote packet.  Unlike its bar parser, that field carries no
    calendar date.  A time-only quote is retained for diagnostics, but its
    legacy ``as_of`` remains null so consumers can reject it as non-realtime.
    """

    source = "mootdx.servertime"
    raw_value = row.get("servertime")
    raw_server_time = _raw_server_time(raw_value)
    row_date = _row_trade_date(row)
    received_at = _local_datetime(received_at)

    if _is_missing(raw_value):
        return _timestamp_result(
            raw_server_time=raw_server_time,
            server_time=None,
            trade_date=row_date,
            quality="missing",
            source=source,
            received_at=received_at,
        )

    parsed_datetime = _parse_datetime(raw_value)
    if parsed_datetime is not None:
        local_datetime = _local_datetime(parsed_datetime)
        if row_date is not None and row_date != local_datetime.date():
            return _timestamp_result(
                raw_server_time=raw_server_time,
                server_time=local_datetime.isoformat(),
                trade_date=local_datetime.date(),
                quality="date_conflict",
                source=source,
                received_at=received_at,
            )
        return _timestamp_result(
            raw_server_time=raw_server_time,
            server_time=local_datetime.isoformat(),
            trade_date=local_datetime.date(),
            quality=_timestamp_quality(local_datetime, received_at),
            source=source,
            received_at=received_at,
        )

    parsed_time = _parse_time(raw_value)
    if parsed_time is not None:
        if row_date is None:
            return _timestamp_result(
                raw_server_time=raw_server_time,
                server_time=parsed_time.isoformat(),
                trade_date=None,
                quality="time_only",
                source=source,
                received_at=received_at,
            )
        local_datetime = _local_datetime(datetime.combine(row_date, parsed_time))
        return _timestamp_result(
            raw_server_time=raw_server_time,
            server_time=local_datetime.isoformat(),
            trade_date=local_datetime.date(),
            quality=_timestamp_quality(local_datetime, received_at),
            source="mootdx.servertime+row.trade_date",
            received_at=received_at,
        )

    return _timestamp_result(
        raw_server_time=raw_server_time,
        server_time=None,
        trade_date=row_date,
        quality="invalid",
        source=source,
        received_at=received_at,
    )


def _timestamp_result(
    *,
    raw_server_time: str | None,
    server_time: str | None,
    trade_date: date | None,
    quality: str,
    source: str,
    received_at: datetime,
) -> dict[str, str | None]:
    # Only a verified, non-future timestamp is safe for the old as_of field.
    as_of = server_time if quality == "verified" else None
    return {
        "as_of": as_of,
        "raw_server_time": raw_server_time,
        "server_time": server_time,
        "trade_date": trade_date.isoformat() if trade_date is not None else None,
        "timestamp_quality": quality,
        "timestamp_source": source,
        "received_at": received_at.isoformat(),
    }


def _timestamp_quality(value: datetime, received_at: datetime) -> str:
    return "future" if value > received_at else "verified"


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    if not any(separator in text for separator in ("T", "t", " ")):
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if text == parsed.date().isoformat():
        return None
    return parsed


def _parse_time(value: Any) -> time | None:
    if isinstance(value, time) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    # The quote protocol uses HH:MM:SS. Do not interpret a date-shaped
    # compact string such as 20260902 as the time 20:26:09.02.
    if len(text) < 5 or text[2] != ":":
        return None
    try:
        return time.fromisoformat(text)
    except ValueError:
        return None


def _row_trade_date(row: dict[str, Any]) -> date | None:
    for field in ("trade_date", "date"):
        value = row.get(field)
        if _is_missing(value):
            continue
        parsed = _parse_date(value)
        if parsed is not None:
            return parsed
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError:
        return None


def _raw_server_time(value: Any) -> str | None:
    if _is_missing(value):
        return None
    if isinstance(value, (datetime, time)):
        return value.isoformat()
    return str(value)


def _local_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ)


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NaT or value is pd.NA:
        return True
    if isinstance(value, str):
        return not value.strip() or value.strip() in {"0", "0.0"}
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(missing) if not hasattr(missing, "__len__") else False


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None

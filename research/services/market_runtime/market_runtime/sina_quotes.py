"""Bounded, dated Sina quote adapter for the isolated market runtime."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from datetime import date, datetime, time
from typing import Any, Callable
from zoneinfo import ZoneInfo

import httpx

from market_runtime.symbols import MarketSymbol

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SINA_QUOTES_URL = "https://hq.sinajs.cn/list="
SINA_REFERER = "https://finance.sina.com.cn/"
MAX_SYMBOLS = 50
MAX_RESPONSE_BYTES = 1024 * 1024
SINA_FIELD_COUNT = 33

_LINE_PATTERN = re.compile(
    r'^var\s+hq_str_(?P<identity>(?:sh|sz)\d{6})\s*=\s*"(?P<body>[^"]*)"\s*;\s*$'
)
_DATE_PATTERN = re.compile(r"(?:\d{8}|\d{4}-\d{2}-\d{2})\Z")
_TIME_PATTERN = re.compile(r"\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\Z")


class SinaQuoteError(RuntimeError):
    """Raised when Sina data or its bounded transport contract is invalid."""


def sina_identity(symbol: MarketSymbol) -> str:
    """Return the exact lower-case Sina identity for a Core symbol."""

    if not isinstance(symbol, MarketSymbol):
        raise SinaQuoteError("Sina quote identity must be a MarketSymbol")
    prefix = "sh" if symbol.exchange == "SSE" else "sz"
    return f"{prefix}{symbol.code}"


def parse_sina_quote_body(
    body: str | bytes, symbols: Sequence[MarketSymbol]
) -> dict[str, tuple[str, ...]]:
    """Parse and identity-check Sina assignments without executing JavaScript.

    The returned mapping is keyed by the complete Sina variable identity.  A
    valid assignment with an empty value is intentionally omitted so callers
    can preserve that symbol as missing.  Only the first 33 comma-separated
    fields are retained; Sina may append fields after the status field.
    """

    requested = _requested_identities(symbols)
    if isinstance(body, bytes):
        body = _decode_body(body)
    if not isinstance(body, str):
        raise SinaQuoteError("Sina response body is not text")
    body = body.lstrip("\ufeff")
    if not body.strip():
        return {}

    parsed: dict[str, tuple[str, ...]] = {}
    seen: set[str] = set()
    for line_number, line in enumerate(body.splitlines(), start=1):
        if not line.strip():
            continue
        match = _LINE_PATTERN.fullmatch(line.strip())
        if match is None:
            raise SinaQuoteError(f"malformed Sina quote line {line_number}")
        identity = match.group("identity")
        if identity not in requested:
            raise SinaQuoteError(f"unexpected Sina quote identity: {identity}")
        if identity in seen:
            raise SinaQuoteError(f"duplicate Sina quote identity: {identity}")
        seen.add(identity)
        value = match.group("body")
        if value == "":
            continue
        fields = tuple(value.split(","))
        if len(fields) < SINA_FIELD_COUNT:
            raise SinaQuoteError(
                f"Sina quote {identity} has {len(fields)} fields; "
                f"expected at least {SINA_FIELD_COUNT}"
            )
        parsed[identity] = fields[:SINA_FIELD_COUNT]
    return parsed


class SinaQuotesProvider:
    """Fetch at most 50 dated Sina quotes using a bounded HTTPS request."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if client is not None and transport is not None:
            raise ValueError("provide either a client or a transport, not both")
        self._client = client
        self._transport = transport
        self._clock = clock or (lambda: datetime.now(SHANGHAI_TZ))

    def quotes(self, symbols: list[MarketSymbol]) -> list[dict[str, Any]]:
        requested = _requested_identities(symbols)
        url = SINA_QUOTES_URL + ",".join(requested)
        if self._client is None:
            client_kwargs: dict[str, Any] = {
                "timeout": 8.0,
                "trust_env": False,
                "verify": True,
                "follow_redirects": False,
            }
            if self._transport is not None:
                client_kwargs["transport"] = self._transport
            try:
                with httpx.Client(**client_kwargs) as client:
                    body = _download(client, url)
            except SinaQuoteError:
                raise
            except httpx.HTTPError as exc:
                raise SinaQuoteError("Sina quote request failed") from exc
        else:
            body = _download(self._client, url)

        fields_by_identity = parse_sina_quote_body(body, symbols)
        if not fields_by_identity:
            return []
        received_at = _local_datetime(self._clock())
        results: list[dict[str, Any]] = []
        for identity, symbol in requested.items():
            fields = fields_by_identity.get(identity)
            if fields is not None:
                results.append(_quote_row(symbol, fields, received_at))
        return results


def _requested_identities(
    symbols: Sequence[MarketSymbol],
) -> dict[str, MarketSymbol]:
    if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
        raise SinaQuoteError("Sina quote symbols must be a sequence")
    if not 1 <= len(symbols) <= MAX_SYMBOLS:
        raise SinaQuoteError("Sina quote batch must contain one to fifty symbols")
    requested: dict[str, MarketSymbol] = {}
    for symbol in symbols:
        identity = sina_identity(symbol)
        if identity in requested:
            raise SinaQuoteError(f"duplicate requested Sina identity: {identity}")
        requested[identity] = symbol
    return requested


def _download(client: Any, url: str) -> bytes:
    try:
        with client.stream(
            "GET",
            url,
            headers={"Referer": SINA_REFERER},
        ) as response:
            status_code = response.status_code
            if 300 <= status_code < 400:
                raise SinaQuoteError("Sina quote redirect refused")
            if status_code != 200:
                raise SinaQuoteError(f"Sina quote request returned HTTP {status_code}")
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_bytes():
                if not isinstance(chunk, bytes):
                    raise SinaQuoteError("Sina response yielded a non-byte chunk")
                size += len(chunk)
                if size > MAX_RESPONSE_BYTES:
                    raise SinaQuoteError("Sina response exceeds 1 MiB limit")
                chunks.append(chunk)
            return b"".join(chunks)
    except SinaQuoteError:
        raise
    except (httpx.HTTPError, OSError, TypeError) as exc:
        raise SinaQuoteError("Sina quote response unavailable") from exc


def _decode_body(body: bytes) -> str:
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return body.decode("gb18030")
        except UnicodeDecodeError as exc:
            raise SinaQuoteError("Sina response is not decodable text") from exc


def _quote_row(
    symbol: MarketSymbol,
    fields: tuple[str, ...],
    received_at: datetime,
) -> dict[str, Any]:
    timestamp = _source_timestamp(fields[30], fields[31], received_at)
    return {
        "symbol": symbol.canonical,
        "as_of": timestamp["as_of"],
        "raw_server_time": timestamp["raw_server_time"],
        "server_time": timestamp["server_time"],
        "trade_date": timestamp["trade_date"],
        "timestamp_quality": timestamp["timestamp_quality"],
        "timestamp_source": timestamp["timestamp_source"],
        "received_at": timestamp["received_at"],
        "price": _number(fields[3]),
        "prev_close": _number(fields[2]),
        "open": _number(fields[1]),
        "high": _number(fields[4]),
        "low": _number(fields[5]),
        "bid1": _number(fields[11]),
        "ask1": _number(fields[21]),
        "volume": _number(fields[8]),
        "volume_unit": "shares",
        "amount": _number(fields[9]),
        "amount_unit": "CNY",
        "source": "sina",
    }


def _source_timestamp(
    raw_date: str,
    raw_time: str,
    received_at: datetime,
) -> dict[str, str | None]:
    date_text = raw_date.strip()
    time_text = raw_time.strip()
    parsed_date = _parse_date(date_text)
    parsed_time = _parse_time(time_text)
    raw_server_time = (
        f"{date_text} {time_text}" if date_text and time_text else None
    )
    received_iso = received_at.isoformat()

    if not date_text or not time_text:
        quality = "missing"
        server_time = None
    elif parsed_date is None or parsed_time is None:
        quality = "invalid"
        server_time = None
    else:
        source_datetime = datetime.combine(
            parsed_date, parsed_time, tzinfo=SHANGHAI_TZ
        )
        server_time = source_datetime.isoformat()
        quality = "future" if source_datetime > received_at else "verified"

    trade_date = parsed_date.isoformat() if parsed_date is not None else None
    return {
        "as_of": server_time if quality == "verified" else None,
        "raw_server_time": raw_server_time,
        "server_time": server_time,
        "trade_date": trade_date,
        "timestamp_quality": quality,
        "timestamp_source": "sina.date+time",
        "received_at": received_iso,
    }


def _parse_date(value: str) -> date | None:
    if not _DATE_PATTERN.fullmatch(value):
        return None
    try:
        if len(value) == 8:
            return datetime.strptime(value, "%Y%m%d").date()
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_time(value: str) -> time | None:
    if not _TIME_PATTERN.fullmatch(value):
        return None
    try:
        return time.fromisoformat(value)
    except ValueError:
        return None


def _local_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise SinaQuoteError("quote clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ)


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


__all__ = [
    "MAX_RESPONSE_BYTES",
    "MAX_SYMBOLS",
    "SINA_QUOTES_URL",
    "SINA_REFERER",
    "SinaQuoteError",
    "SinaQuotesProvider",
    "parse_sina_quote_body",
    "sina_identity",
]

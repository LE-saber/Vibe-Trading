"""Strict adapter for the locally verified ninef simulation API.

The adapter is read-first.  It never logs response bodies or credentials,
never retries a write, preserves unknown order states, and only advertises a
write capability when an explicit local verification profile says so.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from ..models import (
    AccountSnapshot, BrokerOrder, Capabilities, FeeSchedule, InstrumentRule,
    Leaderboard, NewsItem, OrderStatus, Position, ProposedOrder, Quote,
    RankingEntry, ResearchRequest, ResearchResult, Side, TradingContext,
    now_utc,
)
from ..ports import Cancelled, Progress
from ..reconciliation import normalize_status

NINEF_ORIGIN = "https://yyapi.9fapi.com"
_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_ACTIVE_STATUS = {
    OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING, OrderStatus.UNKNOWN,
}
_FEE_COMPONENTS = ("commission", "stampDuty", "transferFee")
_TOTAL_FEE_FIELDS = ("totalFee", "total_fee", "feePaid", "fee_paid", "fee")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class NinefError(RuntimeError):
    """Sanitized provider failure; raw payloads and headers are never retained."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward a bearer credential to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Transport(Protocol):
    def request(self, method: str, path: str, *, params: Mapping[str, str] | None = None,
                body: Mapping[str, Any] | None = None) -> dict[str, Any]: ...


class ResearchBackend(Protocol):
    def research(self, request: ResearchRequest, report: Progress,
                 cancelled: Cancelled) -> ResearchResult: ...


@dataclass(frozen=True)
class NinefSettings:
    account_key: str = ""
    competition_id: str | None = None
    timeout_seconds: int = 20
    max_pages: int = 100
    market_profile: Path | None = None
    verification_ref: str = ""
    buy_verified: bool = False
    sell_verified: bool = False
    cancel_verified: bool = False

    @classmethod
    def from_env(cls) -> "NinefSettings":
        profile = os.environ.get("NINEF_MARKET_PROFILE", "").strip()
        return cls(
            account_key=os.environ.get("NINEF_ACCOUNT_KEY", "").strip(),
            competition_id=os.environ.get("NINEF_COMPETITION_ID") or None,
            timeout_seconds=int(os.environ.get("NINEF_TIMEOUT_SECONDS", "20")),
            max_pages=int(os.environ.get("NINEF_MAX_PAGES", "100")),
            market_profile=Path(profile) if profile else None,
            verification_ref=os.environ.get("NINEF_VERIFICATION_REF", "").strip(),
            buy_verified=os.environ.get("NINEF_BUY_VERIFIED") == "1",
            sell_verified=os.environ.get("NINEF_SELL_VERIFIED") == "1",
            cancel_verified=os.environ.get("NINEF_CANCEL_VERIFIED") == "1",
        )


class NinefTransport:
    """One-attempt stdlib transport.  Only the fixed verified origin is allowed."""

    def __init__(self, token: str | None = None, *, timeout_seconds: int = 20):
        token = (token or os.environ.get("NINEF_API_TOKEN", "")).strip()
        if token.lower().startswith("bearer "):
            token = token.split(None, 1)[1].strip()
        if not token:
            raise NinefError("NINEF_API_TOKEN is not configured in this process")
        self._token = token
        self.timeout_seconds = timeout_seconds
        self._opener = urllib.request.build_opener(_NoRedirect())

    def request(self, method: str, path: str, *, params: Mapping[str, str] | None = None,
                body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if method not in {"GET", "POST"} or not path.startswith("/api/v1/"):
            raise NinefError("invalid ninef request boundary")
        query = urllib.parse.urlencode(dict(params or {}))
        url = NINEF_ORIGIN + path + ("?" + query if query else "")
        payload = None if body is None else json.dumps(
            body, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": "Bearer " + self._token,
            "User-Agent": "vibe-trading-local/0.1",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                result = json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
                UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NinefError("ninef request failed; inspect credentials/network locally") from exc
        if not isinstance(result, dict) or result.get("code") not in (None, 0):
            raise NinefError("ninef returned an unsuccessful or invalid response")
        return result


def _data(response: Mapping[str, Any], label: str) -> Any:
    if "data" not in response:
        raise NinefError(f"{label} response has no data field")
    return response["data"]


def _decimal(value: Any, label: str, *, optional: bool = False) -> Decimal | None:
    if value is None and optional:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise NinefError(f"{label} is not a decimal") from exc
    if not result.is_finite():
        raise NinefError(f"{label} is not finite")
    return result


def _integer(value: Any, label: str, *, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool):
        raise NinefError(f"{label} is not an integer")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise NinefError(f"{label} is not an integer") from exc
    if result != result.to_integral_value() or result < 0:
        raise NinefError(f"{label} is not a non-negative integer")
    return int(result)


def _timestamp(value: Any, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise NinefError("provider timestamp is invalid") from exc
    elif fallback is not None:
        parsed = fallback
    else:
        raise NinefError("provider timestamp is missing")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_SHANGHAI)
    return parsed.astimezone(timezone.utc)


def _symbol(value: Any, *, optional: bool = False) -> str | None:
    text = str(value or "").strip().upper()
    if not text and optional:
        return None
    if not _SYMBOL.fullmatch(text):
        raise NinefError("provider returned a non Shanghai/Shenzhen A-share symbol")
    return text


def _side(value: Any) -> Side | None:
    text = str(value or "").strip().upper()
    if text in {"BUY", "B", "买", "买入"} or "买" in text:
        return Side.BUY
    if text in {"SELL", "S", "卖", "卖出"} or "卖" in text:
        return Side.SELL
    return None


def _first(item: Mapping[str, Any], nested: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if item.get(key) is not None:
            return item[key]
        if nested.get(key) is not None:
            return nested[key]
    return None


def _order(item: Mapping[str, Any], observed_at: datetime) -> BrokerOrder:
    meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
    raw_id = item.get("id") or item.get("orderId") or meta.get("orderId")
    if raw_id is None or not str(raw_id).strip():
        raise NinefError("order activity row has no authoritative order ID")
    quantity_raw = _first(item, meta, "quantity")
    quantity = _integer(quantity_raw, "order quantity") if quantity_raw is not None else None
    filled_raw = _first(item, meta, "filledQuantity", "tradeQuantity")
    filled = _integer(filled_raw, "filled quantity", default=0)
    price_raw = _first(item, meta, "price", "referencePrice")
    price = _decimal(price_raw, "order price", optional=True)
    fill_price_raw = _first(item, meta, "averageFillPrice", "filledPrice", "tradePrice")
    fill_price = _decimal(fill_price_raw, "average fill price", optional=True)
    breakdown = item.get("costBreakdown")
    if not isinstance(breakdown, Mapping):
        breakdown = meta.get("costBreakdown") if isinstance(meta.get("costBreakdown"), Mapping) else {}
    total_fee_raw = _first(item, meta, *_TOTAL_FEE_FIELDS)
    if total_fee_raw is None:
        total_fee_raw = _first(breakdown, {}, *_TOTAL_FEE_FIELDS)
    if total_fee_raw is not None:
        fee = _decimal(total_fee_raw, "cumulative fee")
    elif all(breakdown.get(key) is not None for key in _FEE_COMPONENTS):
        fee = sum((_decimal(breakdown[key], "cumulative fee") for key in _FEE_COMPONENTS), Decimal(0))
    else:
        fee = None
    return BrokerOrder(
        order_id=str(raw_id).strip(), symbol=_symbol(item.get("symbol"), optional=True),
        side=_side(_first(item, meta, "direction", "side")), quantity=quantity,
        filled_quantity=filled, limit_price=price, status=normalize_status(str(item.get("status") or "")),
        average_fill_price=fill_price, fee_paid=fee,
        updated_at=_timestamp(_first(item, meta, "updatedAt", "filledAt", "submittedAt", "createdAt"), observed_at),
        client_order_id=(str(_first(item, meta, "clientOrderId", "client_order_id")).strip()
                         if _first(item, meta, "clientOrderId", "client_order_id") is not None else None),
    )


def _pct(value: Any) -> Decimal:
    text = str(value or "").strip().replace("%", "")
    return _decimal(text, "ranking return") or Decimal(0)


def _participant_id(value: Any) -> str:
    return "ninef-player-" + hashlib.sha256(str(value).encode()).hexdigest()[:24]


def _market_open(day: Mapping[str, Any], observed_at: datetime) -> bool:
    if day.get("isTradingDay") is not True:
        return False
    local = observed_at.astimezone(_SHANGHAI)
    clock = local.time().replace(tzinfo=None)
    return time(9, 30) <= clock <= time(11, 30) or time(13) <= clock <= time(15)


class NinefProvider:
    """Provider implementation with separately injectable real research."""

    def __init__(self, transport: Transport, research_backend: ResearchBackend | None = None,
                 settings: NinefSettings | None = None):
        self.transport = transport
        self.research_backend = research_backend
        self.settings = settings or NinefSettings()
        self._profile = self._load_profile(self.settings.market_profile)

    @staticmethod
    def _load_profile(path: Path | None) -> dict[str, Any] | None:
        if path is None:
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise NinefError("NINEF_MARKET_PROFILE cannot be read") from exc
        if not isinstance(payload, dict):
            raise NinefError("NINEF_MARKET_PROFILE must be a JSON object")
        return payload

    def capabilities(self) -> Capabilities:
        verified = bool(self.settings.verification_ref and self.settings.account_key)
        return Capabilities(
            provider="ninef-local", environment="NINEF_SIMULATION",
            account=True, rankings=True, news=True,
            buy=verified and self.settings.buy_verified,
            sell=verified and self.settings.sell_verified,
            cancel=verified and self.settings.cancel_verified,
            verified=verified,
            verification_ref=self.settings.verification_ref,
        )

    def _pages(self, path: str, *, params: Mapping[str, str]) -> list[Mapping[str, Any]]:
        cursor: str | None = None
        seen: set[str] = set()
        rows: list[Mapping[str, Any]] = []
        expected_total: int | None = None
        for _ in range(self.settings.max_pages):
            query = dict(params)
            if cursor:
                query["cursor"] = cursor
            page = _data(self.transport.request("GET", path, params=query), path)
            if not isinstance(page, Mapping) or not isinstance(page.get("items"), list):
                raise NinefError("paginated response has invalid data/items")
            if any(not isinstance(item, Mapping) for item in page["items"]):
                raise NinefError("paginated response contains a non-object item")
            rows.extend(page["items"])
            total = page.get("total")
            if total is not None:
                parsed_total = _integer(total, "pagination total")
                if expected_total is not None and parsed_total != expected_total:
                    raise NinefError("pagination total changed between pages")
                expected_total = parsed_total
            has_more = page.get("hasMore")
            next_cursor = page.get("nextCursor")
            if has_more is False:
                if next_cursor not in (None, ""):
                    raise NinefError("pagination metadata is contradictory")
                if expected_total is not None and len(rows) != expected_total:
                    raise NinefError("pagination ended before the reported total")
                return rows
            if has_more not in (True, None):
                raise NinefError("pagination hasMore is not boolean")
            if has_more is None and next_cursor in (None, ""):
                raise NinefError("pagination completion metadata is missing")
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
                raise NinefError("pagination did not provide a new cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise NinefError("pagination exceeded the configured page bound")

    def _account(self, started: datetime) -> tuple[AccountSnapshot, dict[str, str]]:
        assets = _data(self.transport.request("GET", "/api/v1/assets/overview"), "assets")
        raw_positions = _data(self.transport.request("GET", "/api/v1/positions"), "positions")
        raw_orders = self._pages("/api/v1/record/order-activity", params={"limit": "50"})
        observed = now_utc()
        if not isinstance(assets, Mapping) or not isinstance(raw_positions, list):
            raise NinefError("account or positions response has invalid shape")
        names: dict[str, str] = {}
        positions: list[Position] = []
        for raw in raw_positions:
            if not isinstance(raw, Mapping):
                raise NinefError("position row is not an object")
            symbol = _symbol(raw.get("symbol"))
            assert symbol is not None
            name = str(raw.get("name") or "").strip()
            names[symbol] = name
            positions.append(Position(
                symbol=symbol, name=name, quantity=_integer(raw.get("qty"), "position qty"),
                sellable_quantity=_integer(raw.get("sellableQty"), "sellable qty"),
                frozen_quantity=_integer(raw.get("frozenQty"), "frozen qty"),
                market_value=_decimal(raw.get("marketValue"), "market value"),
            ))
        orders = tuple(_order(item, observed) for item in raw_orders)
        if len({order.order_id for order in orders}) != len(orders):
            raise NinefError("order pagination returned duplicate authoritative IDs")
        available = _decimal(assets.get("availableFunds"), "available funds")
        frozen = _decimal(assets.get("frozenFunds"), "frozen funds")
        total = _decimal(assets.get("totalAssets"), "total assets")
        assert available is not None and frozen is not None and total is not None
        reconciled = available + frozen + sum((position.market_value for position in positions), Decimal(0))
        # The remote account endpoints expose no stable account ID. Keep the lock
        # identity independent of rotating bearer credentials. An explicit local
        # key is required before writes can ever be advertised as verified.
        account_alias = ("ninef-account-" + hashlib.sha256(
            self.settings.account_key.encode()
        ).hexdigest()[:24] if self.settings.account_key else "ninef-unconfigured-account")
        source_as_of = _timestamp(assets.get("updatedAt"))
        snapshot = AccountSnapshot(
            account_id=account_alias, provider="ninef-local", as_of=source_as_of,
            collection_started_at=started, collected_at=observed,
            total_assets=total, available_cash=available,
            frozen_cash=frozen, positions=tuple(sorted(positions, key=lambda item: item.symbol)),
            orders=orders, orders_complete=True, consistent=abs(reconciled - total) <= Decimal("0.02"),
            source_ref="ninef:assets/overview+positions+all-order-activity-pages",
        )
        return snapshot, names

    def _trading_day(self) -> tuple[Mapping[str, Any], date]:
        raw = _data(self.transport.request("GET", "/api/v1/market/trading-day"), "trading day")
        if not isinstance(raw, Mapping):
            raise NinefError("trading-day response is not an object")
        value = str(raw.get("date") or "")
        try:
            trading_date = date.fromisoformat(value)
        except ValueError as exc:
            raise NinefError("trading-day date is missing or invalid") from exc
        return raw, trading_date

    def _fees(self, trading_date: date) -> FeeSchedule:
        raw = (self._profile or {}).get("fees")
        if not isinstance(raw, Mapping):
            return FeeSchedule(
                version="ninef-unverified-fees", valid_from=trading_date,
                valid_until=trading_date, source_ref="ninef:no verified effective fee profile",
            )
        try:
            return FeeSchedule.model_validate(dict(raw))
        except Exception as exc:
            raise NinefError("market profile fee contract is invalid") from exc

    def _rule(self, symbol: str, trading_date: date) -> InstrumentRule:
        rules = (self._profile or {}).get("instrument_rules")
        if isinstance(rules, list):
            for raw in rules:
                if not isinstance(raw, Mapping):
                    continue
                prefix = str(raw.get("prefix") or "")
                if symbol.split(".", 1)[0].startswith(prefix):
                    values = {key: value for key, value in raw.items() if key != "prefix"}
                    values["symbol"] = symbol
                    try:
                        return InstrumentRule.model_validate(values)
                    except Exception as exc:
                        raise NinefError("market profile instrument rule is invalid") from exc
        return InstrumentRule(
            symbol=symbol, allow_odd_lot_liquidation=False,
            valid_from=trading_date, valid_until=trading_date,
            source_ref="ninef:no verified lot/tick profile",
        )

    def context(self, symbols: Sequence[str]) -> TradingContext:
        started = now_utc()
        account, _ = self._account(started)
        requested = sorted(set(symbols) | {position.symbol for position in account.positions})
        day, trading_date = self._trading_day()
        observed = now_utc()
        quotes: list[Quote] = []
        for offset in range(0, len(requested), 50):
            chunk = requested[offset:offset + 50]
            if not chunk:
                continue
            raw = _data(self.transport.request(
                "GET", "/api/v1/data/quotes/batch", params={"symbols": ",".join(chunk)}
            ), "quotes")
            if not isinstance(raw, Mapping):
                raise NinefError("quotes response is not an object")
            for symbol in chunk:
                item = raw.get(symbol)
                if not isinstance(item, Mapping):
                    continue
                last = _decimal(item.get("price"), "quote price")
                down = _decimal(item.get("limit_down"), "quote limit down")
                up = _decimal(item.get("limit_up"), "quote limit up")
                assert last is not None and down is not None and up is not None
                official = item.get("data_source") == "official"
                # The verified response has no suspension/buy/sell permission field.
                # Preserve that uncertainty instead of inferring tradability from a price.
                explicit_tradable = item.get("tradable") is True and official
                quotes.append(Quote(
                    symbol=symbol, as_of=_timestamp(item.get("updated_at")),
                    last=last, limit_down=down, limit_up=up,
                    tradable=explicit_tradable,
                    can_buy=explicit_tradable and last < up,
                    can_sell=explicit_tradable and last > down,
                    source_ref="ninef:data/quotes/batch; tradability requires explicit field",
                ))
        return TradingContext(
            account=account, quotes=tuple(quotes),
            rules=tuple(self._rule(symbol, trading_date) for symbol in requested),
            fees=self._fees(trading_date), market_open=_market_open(day, observed),
            trading_date=trading_date,
        )

    def _competition_status(self) -> Mapping[str, Any]:
        competition_id = self.settings.competition_id
        if not competition_id:
            data = _data(self.transport.request(
                "GET", "/api/v1/mp/competition-gate/competitions"
            ), "competitions")
            items = data.get("items") if isinstance(data, Mapping) else None
            usable = [item for item in (items or []) if isinstance(item, Mapping) and item.get("competitionId")]
            if len(usable) != 1:
                raise NinefError("configure NINEF_COMPETITION_ID when competition selection is ambiguous")
            competition_id = str(usable[0]["competitionId"])
        status = _data(self.transport.request(
            "GET", "/api/v1/mp/competition-gate/status",
            params={"competitionId": competition_id},
        ), "competition status")
        if not isinstance(status, Mapping):
            raise NinefError("competition status is not an object")
        return status

    @staticmethod
    def _ranking(item: Mapping[str, Any]) -> RankingEntry:
        rank = _integer(item.get("rank"), "ranking rank")
        if rank <= 0:
            raise NinefError("ranking rank must be positive")
        total_assets_raw = _first(
            item, {},
            "totalAssets",
            "totalAsset",
            "assetValue",
            "total_assets",
        )
        total_assets = None if total_assets_raw is None else _decimal(
            total_assets_raw, "ranking total assets"
        )
        return RankingEntry(
            participant_id=_participant_id(item.get("userId") or item.get("displayName") or rank),
            display_name=str(item.get("displayName") or ""), rank=rank,
            return_pct=_pct(item.get("metricText")),
            total_assets=total_assets,
        )

    def leaderboard(self) -> Leaderboard:
        status = self._competition_status()
        board = status.get("leaderboard")
        if not isinstance(board, Mapping):
            raise NinefError("competition response has no leaderboard")
        raw_items = board.get("items") or board.get("top50")
        if not isinstance(raw_items, list):
            raise NinefError("leaderboard entries are missing")
        entries = tuple(self._ranking(item) for item in raw_items[:30] if isinstance(item, Mapping))
        raw_own = board.get("currentUser") or board.get("myRank")
        own = self._ranking(raw_own) if isinstance(raw_own, Mapping) else None
        competition = status.get("competitionEntry")
        competition_id = str(competition.get("competitionId")) if isinstance(competition, Mapping) else (
            self.settings.competition_id or "ninef-competition"
        )
        return Leaderboard(
            competition_id=competition_id, as_of=_timestamp(board.get("updatedAt"), now_utc()),
            entries=entries, own=own,
            source_ref="ninef:competition-gate/status leaderboard.items[:30]+currentUser",
        )

    @staticmethod
    def _news_symbols(item: Mapping[str, Any]) -> tuple[str, ...]:
        found: set[str] = set()
        def visit(value: Any) -> None:
            if isinstance(value, Mapping):
                for key, child in value.items():
                    if key.lower() in {"symbol", "ts_code", "code"}:
                        text = str(child or "").upper()
                        if _SYMBOL.fullmatch(text):
                            found.add(text)
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)
        for key in ("relatedStocks", "recommendedStocks", "portfolioMatches", "leaders", "sourceStock"):
            visit(item.get(key))
        return tuple(sorted(found))

    def news(self) -> tuple[NewsItem, ...]:
        fetched = now_utc()
        rows = self._pages(
            "/api/v1/data/hot-topics", params={"limit": "50", "lookback_hours": "48"}
        )
        result: list[NewsItem] = []
        for item in rows:
            raw_id = item.get("id")
            if raw_id is None or isinstance(raw_id, bool) or not str(raw_id).strip():
                continue
            raw_title = item.get("title")
            if not isinstance(raw_title, str) or not raw_title.strip():
                continue
            raw_body = ""
            for key in ("content", "detail", "recommendationReason"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    raw_body = value.strip()
                    break
            if not raw_body:
                continue
            source = ""
            for key in ("dataSource", "source"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    source = value.strip()
                    break
            if not source or len(source) > 120 or item.get("createdAt") is None:
                continue
            try:
                published_at = _timestamp(item.get("createdAt"))
            except NinefError:
                continue
            raw_id = str(raw_id).strip()
            raw_title = raw_title.strip()
            truncated = len(raw_body) > 12000 or len(raw_title) > 400
            news_id = "ninef-news-" + hashlib.sha256(raw_id.encode()).hexdigest()[:24]
            result.append(NewsItem(
                news_id=news_id, title=raw_title[:400], body=raw_body[:12000],
                symbols=self._news_symbols(item),
                published_at=published_at, fetched_at=fetched,
                source=source, source_ref=(f"ninef:data/hot-topics item:{news_id}"
                    + (";content-truncated-to-contract" if truncated else "")),
            ))
        return tuple(result)

    def research(self, request: ResearchRequest, report: Progress,
                 cancelled: Cancelled) -> ResearchResult:
        if self.research_backend is None:
            raise NinefError(
                "real research backend is not configured; DEMO is never used as a fallback"
            )
        return self.research_backend.research(request, report, cancelled)

    def _require(self, capability: str) -> None:
        if not getattr(self.capabilities(), capability):
            raise NinefError(f"ninef {capability} capability is not locally verified")

    def submit(self, order: ProposedOrder, client_order_id: str) -> BrokerOrder:
        capability = "buy" if order.side == Side.BUY else "sell"
        self._require(capability)
        if not order.name:
            raise NinefError("approved order is missing the instrument name required by ninef")
        path = "/api/v1/stock/buy" if order.side == Side.BUY else "/api/v1/stock/sell"
        response = _data(self.transport.request("POST", path, body={
            "code": order.symbol.split(".", 1)[0], "name": order.name,
            "quantity": order.quantity, "source": 0, "type": 1,
            "price": str(order.limit_price),
        }), "order submission")
        if not isinstance(response, Mapping) or response.get("order_id") is None:
            raise NinefError("ninef did not return an authoritative order ID")
        status = normalize_status(str(response.get("status") or ""))
        filled_raw = _first(response, {}, "filled_quantity", "filledQuantity", "tradeQuantity")
        filled = _integer(filled_raw, "submission filled quantity", default=0)
        if status in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED} and filled_raw is None:
            status = OrderStatus.UNKNOWN
        return BrokerOrder(
            order_id=str(response["order_id"]), symbol=order.symbol, side=order.side,
            quantity=order.quantity, filled_quantity=filled, limit_price=order.limit_price,
            status=status, average_fill_price=_decimal(
                response.get("trade_price"), "submission fill price", optional=True
            ), fee_paid=_decimal(response.get("fee"), "submission fee", optional=True),
            updated_at=now_utc(), client_order_id=client_order_id,
        )

    def get_order(self, order_id: str) -> BrokerOrder:
        rows = self._pages("/api/v1/record/order-activity", params={"limit": "50"})
        matches = [item for item in rows if str(item.get("id") or item.get("orderId")) == order_id]
        if len(matches) != 1:
            raise NinefError("exact authoritative order ID was not found uniquely")
        return _order(matches[0], now_utc())

    def cancel(self, order_id: str) -> None:
        self._require("cancel")
        self.transport.request(
            "POST", "/api/v1/stock/cancel-order", body={"stock_order_id": order_id}
        )


def load_research_backend(spec: str) -> ResearchBackend | None:
    if not spec:
        return None
    module, separator, attribute = spec.partition(":")
    if not separator or not module or not attribute:
        raise NinefError("VIBE_RESEARCH_PROVIDER must be module:factory")
    factory: Callable[[], ResearchBackend] = getattr(importlib.import_module(module), attribute)
    return factory()


def create_provider() -> NinefProvider:
    settings = NinefSettings.from_env()
    transport = NinefTransport(timeout_seconds=settings.timeout_seconds)
    research = load_research_backend(os.environ.get("VIBE_RESEARCH_PROVIDER", "").strip())
    return NinefProvider(transport, research, settings)


__all__ = [
    "NinefError", "NinefProvider", "NinefSettings", "NinefTransport",
    "create_provider", "load_research_backend",
]

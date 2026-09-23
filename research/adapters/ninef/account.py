"""Fetch a strict account snapshot from the fixed Ninef read-only API surface."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import ssl
from typing import TYPE_CHECKING, Any, Callable, Mapping
import urllib.error
import urllib.parse
import urllib.request

from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.pipeline.time_context import SHANGHAI

if TYPE_CHECKING:
    from competition_core.pipeline.portfolio import FeeSchedule


BASE_URL = "https://yyapi.9fapi.com"
TOKEN_ENV = "NINEF_API_TOKEN"
MAX_RESPONSE_BYTES = 1024 * 1024
ENDPOINTS = {
    "assets": ("/api/v1/assets/overview", {}),
    "positions": ("/api/v1/positions", {}),
    "orders": ("/api/v1/record/order-activity", {"limit": "50"}),
    "competitions": ("/api/v1/mp/competition-gate/competitions", {}),
    "competition_status": ("/api/v1/mp/competition-gate/status", {}),
    "hot_topics": ("/api/v1/data/hot-topics", {"limit": "50", "lookback_hours": "48"}),
}
MAX_PAGES = 20
_OPEN_STATUSES = frozenset({"PENDING", "PARTIALLY_FILLED", "CANCEL_PENDING", "UNKNOWN"})


class NinefAccountAdapterError(RuntimeError):
    """A sanitized read or contract failure; credentials are never included."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


def _clean_token(value: str) -> str:
    token = value.strip()
    if token.lower().startswith("bearer "):
        token = token.split(None, 1)[1].strip()
    if (len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}):
        token = token[1:-1].strip()
    if not token or len(token) > 8192 or any(char.isspace() for char in token):
        raise NinefAccountAdapterError("Ninef credential is missing or malformed")
    return token


def resolve_ninef_token(
    *,
    token_file: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve one process credential; never scan directories or echo its value."""
    environment = os.environ if environ is None else environ
    configured = environment.get(TOKEN_ENV, "")
    if configured.strip():
        return _clean_token(configured)
    if token_file is None:
        raise NinefAccountAdapterError(f"{TOKEN_ENV} is not configured")
    try:
        with Path(token_file).open("rb") as handle:
            body = handle.read(8193)
        if len(body) > 8192:
            raise NinefAccountAdapterError("Ninef credential file is too large")
        text = body.decode("utf-8-sig")
    except (OSError, UnicodeError) as exc:
        raise NinefAccountAdapterError("cannot read the configured Ninef credential file") from exc
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if len(lines) != 1:
        raise NinefAccountAdapterError("Ninef credential file must contain one credential")
    value = lines[0]
    if value.startswith(f"{TOKEN_ENV}="):
        value = value.split("=", 1)[1]
    elif "=" in value:
        raise NinefAccountAdapterError("Ninef credential file contains an unsupported key")
    return _clean_token(value)


def _unique_object(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _request_json(token: str, name: str, params: Mapping[str, str] | None = None,
                  *, timeout: float = 20.0) -> dict[str, Any]:
    if name not in ENDPOINTS:
        raise NinefAccountAdapterError("unsupported Ninef endpoint")
    path, defaults = ENDPOINTS[name]
    query = urllib.parse.urlencode(defaults | dict(params or {}))
    url = BASE_URL + path + (f"?{query}" if query else "")
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {token}",
            "User-Agent": "competition-core-ninef-account/1.0",
        },
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirect(),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            if response.geturl() != url:
                raise NinefAccountAdapterError("Ninef response redirected unexpectedly")
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        reason = "credential rejected" if exc.code == 401 else f"HTTP {exc.code}"
        raise NinefAccountAdapterError(f"Ninef {name} request failed: {reason}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise NinefAccountAdapterError(f"Ninef {name} request is unavailable") from exc
    if len(body) > MAX_RESPONSE_BYTES:
        raise NinefAccountAdapterError(f"Ninef {name} response exceeds the size limit")
    try:
        payload = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise NinefAccountAdapterError(f"Ninef {name} response is malformed") from exc
    if not isinstance(payload, dict) or payload.get("code") not in (None, 0) or "data" not in payload:
        raise NinefAccountAdapterError(f"Ninef {name} response rejected the request")
    return payload


def _money(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    try:
        raw = Decimal(str(value))
        result = raw.quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid") from exc
    if not result.is_finite() or result < 0 or raw != result:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    return result


def _quantity(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise NinefAccountAdapterError(f"Ninef {field} is invalid")
    return value


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _implementation_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class NinefAccountFetch:
    snapshot: AccountSnapshot
    fee_schedule: FeeSchedule | None
    receipt: dict[str, Any]


Requester = Callable[[str, str, Mapping[str, str] | None], dict[str, Any]]


def _order_status(value: Any) -> str:
    if not isinstance(value, str):
        return "UNKNOWN"
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    groups = {
        "PENDING": {"待成交", "已报", "已受理", "pending", "queued", "submitted", "accepted"},
        "PARTIALLY_FILLED": {"部分成交", "部成", "partial", "partially_filled"},
        "FILLED": {"已成交", "全部成交", "filled", "completed"},
        "CANCEL_PENDING": {"撤单处理中", "待撤", "cancel_pending", "cancelling"},
        "CANCELLED": {"已撤", "已撤单", "已撤销", "cancelled", "canceled"},
        "REJECTED": {"废单", "已拒绝", "拒绝", "rejected", "failed"},
    }
    for status, values in groups.items():
        if normalized in values:
            return status
    return "UNKNOWN"


def _order_value(row: Mapping[str, Any], *names: str):
    meta = row.get("meta") if isinstance(row.get("meta"), Mapping) else {}
    for name in names:
        if row.get(name) is not None:
            return row[name]
        if meta.get(name) is not None:
            return meta[name]
    return None


def _normalized_order(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_id = _order_value(row, "id", "orderId", "order_id")
    if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)) or not str(raw_id).strip():
        raise NinefAccountAdapterError("Ninef order identifier is missing")
    quantity = _quantity(_order_value(row, "quantity", "qty"), "order quantity")
    filled_raw = _order_value(row, "tradeQuantity", "filledQuantity", "filled_quantity")
    filled = 0 if filled_raw is None else _quantity(filled_raw, "filled order quantity")
    if filled > quantity:
        raise NinefAccountAdapterError("Ninef filled order quantity is invalid")
    status_raw = row.get("status")
    status = _order_status(status_raw)
    symbol = _order_value(row, "symbol", "tsCode")
    side_raw = _order_value(row, "direction", "side")
    side_text = str(side_raw or "").strip().lower()
    side = "BUY" if side_text in {"买入", "buy", "b"} else ("SELL" if side_text in {"卖出", "sell", "s"} else "UNKNOWN")
    order_ref = "ninef-order-" + hashlib.sha256(str(raw_id).strip().encode("utf-8")).hexdigest()[:24]
    result = {
        "order_ref": order_ref, "symbol": symbol if isinstance(symbol, str) else None,
        "side": side, "order_type": str(_order_value(row, "orderType", "type") or "UNKNOWN")[:40],
        "price": _order_value(row, "price", "orderPrice"), "quantity": quantity,
        "filled_quantity": filled, "remaining_quantity": quantity - filled,
        "status": status, "provider_status": str(status_raw or "")[:80],
        "created_at": _order_value(row, "createdAt", "createTime"),
        "updated_at": _order_value(row, "updatedAt", "updateTime", "filledAt"),
        "frozen_cash": _order_value(row, "frozenCash", "frozenFunds"),
        "fees": _order_value(row, "totalFee", "serviceFee"),
    }
    return result


def _order_pages(read: Requester, credential: str):
    cursor = None
    seen_cursors: set[str] = set()
    expected_total = None
    rows: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    for _ in range(MAX_PAGES):
        params = {"cursor": cursor} if cursor else None
        payload = read(credential, "orders", params)
        payloads.append(payload)
        page = payload.get("data")
        if not isinstance(page, dict) or not isinstance(page.get("items"), list):
            raise NinefAccountAdapterError("Ninef order page has an invalid data shape")
        if any(not isinstance(row, dict) for row in page["items"]):
            raise NinefAccountAdapterError("Ninef order row is invalid")
        rows.extend(page["items"])
        total = page.get("total")
        if total is not None:
            if type(total) is not int or total < 0 or (expected_total is not None and total != expected_total):
                raise NinefAccountAdapterError("Ninef order total changed during pagination")
            expected_total = total
        has_more, next_cursor = page.get("hasMore"), page.get("nextCursor")
        if has_more is False:
            if next_cursor not in (None, ""):
                raise NinefAccountAdapterError("Ninef order pagination metadata is contradictory")
            if expected_total is not None and len(rows) != expected_total:
                raise NinefAccountAdapterError("Ninef order count cannot be reconciled")
            normalized = [_normalized_order(row) for row in rows]
            if len({row["order_ref"] for row in normalized}) != len(normalized):
                raise NinefAccountAdapterError("Ninef order pages contain duplicate identifiers")
            return rows, normalized, payloads
        if has_more not in (True, None):
            raise NinefAccountAdapterError("Ninef order pagination flag is invalid")
        if has_more is None and next_cursor in (None, ""):
            raise NinefAccountAdapterError("Ninef order pagination completion metadata is missing")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
            raise NinefAccountAdapterError("Ninef order pagination did not provide a new cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise NinefAccountAdapterError("Ninef order pagination exceeded the page limit")


def _fee_schedule(orders: list[dict[str, Any]], observed_at: datetime):
    """Use an explicit platform feeRule only when a fill proves it on this date."""
    rules = []
    current_day_rule = None
    for row in orders:
        breakdown = row.get("costBreakdown")
        rule = breakdown.get("feeRule") if isinstance(breakdown, dict) else None
        if not isinstance(rule, dict):
            continue
        fields = {
            "commission_rate": rule.get("commissionRate"),
            "minimum_commission": rule.get("minCommission"),
            "stamp_duty_rate": rule.get("stampDutyRate"),
            "market": rule.get("market"),
            "requested_market": rule.get("requestedMarket"),
            "source": rule.get("source"),
        }
        rules.append(fields)
        filled_at = row.get("filledAt")
        if isinstance(filled_at, str) and filled_at.startswith(observed_at.date().isoformat() + " "):
            current_day_rule = fields
    if current_day_rule is None or any(rule != current_day_rule for rule in rules):
        return None
    if (current_day_rule["market"] != "cn_a_share"
            or current_day_rule["requested_market"] != "cn_a_share"
            or current_day_rule["source"] != "config"):
        return None
    from competition_core.pipeline.portfolio import FeeSchedule
    try:
        return FeeSchedule(
            source="ninef.order_activity feeRule; commission and stamp duty only",
            as_of=observed_at,
            valid_through=observed_at.date(),
            commission_rate=Decimal(str(current_day_rule["commission_rate"])),
            minimum_commission=Decimal(str(current_day_rule["minimum_commission"])),
            buy_other_rate=Decimal("0"),
            sell_other_rate=Decimal(str(current_day_rule["stamp_duty_rate"])),
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


def fetch_ninef_account(
    token: str,
    *,
    requester: Requester | None = None,
    clock: Callable[[], datetime] | None = None,
) -> NinefAccountFetch:
    """Read and reconcile assets, positions and every authoritative order page."""
    credential = _clean_token(token)
    read = requester or (lambda supplied, name, params=None: _request_json(supplied, name, params))
    implementation_hash = _implementation_hash()
    started = (clock or (lambda: datetime.now(SHANGHAI)))()
    if not isinstance(started, datetime) or started.utcoffset() is None:
        raise NinefAccountAdapterError("Ninef observation clock must be timezone-aware")
    assets_start = read(credential, "assets", None)
    positions_response = read(credential, "positions", None)
    order_rows, orders, order_payloads = _order_pages(read, credential)
    assets_end = read(credential, "assets", None)
    completed = (clock or (lambda: datetime.now(SHANGHAI)))()
    if not isinstance(completed, datetime) or completed.utcoffset() is None or completed < started:
        raise NinefAccountAdapterError("Ninef observation clock is invalid")

    assets = assets_end.get("data")
    initial_assets = assets_start.get("data")
    positions_data = positions_response.get("data")
    if not isinstance(assets, dict) or not isinstance(initial_assets, dict) or not isinstance(positions_data, list):
        raise NinefAccountAdapterError("Ninef account response has an invalid data shape")
    # ``totalAssets`` is mark-to-market and can legitimately move between the
    # opening and closing reads while the market is live.  Cash, frozen funds
    # and holding count are the transaction-sensitive reconciliation fields;
    # positions and the complete order ledger are validated independently.
    stable_fields = ("availableFunds", "frozenFunds", "holdingCount")
    if any(initial_assets.get(name) != assets.get(name) for name in stable_fields):
        raise NinefAccountAdapterError("Ninef account changed during collection")

    positions = []
    for row in positions_data:
        if not isinstance(row, dict):
            raise NinefAccountAdapterError("Ninef position row is invalid")
        positions.append(
            AccountPosition(
                symbol=row.get("symbol"),
                quantity=_quantity(row.get("qty"), "position quantity"),
                sellable_quantity=_quantity(row.get("sellableQty"), "position sellable quantity"),
                market_value=_money(row.get("marketValue"), "position market value"),
            )
        )
    holding_count = assets.get("holdingCount")
    if type(holding_count) is not int or holding_count != len(positions):
        raise NinefAccountAdapterError("Ninef holding count cannot be reconciled")
    available = _money(assets.get("availableFunds"), "available funds")
    frozen = _money(assets.get("frozenFunds"), "frozen funds")
    pending = sum(1 for row in orders if row["status"] in _OPEN_STATUSES)

    # Bind the contract to the position observation itself.  The provider's
    # separately sampled total-assets mark may move while quotes update and
    # therefore cannot be reconciled cent-for-cent with these position rows.
    reported_total_asset = _money(assets.get("totalAssets"), "total assets")
    opening_total_asset = _money(initial_assets.get("totalAssets"), "total assets")
    reconciled_total_asset = (
        available + frozen + sum(position.market_value for position in positions)
        if reported_total_asset != opening_total_asset
        else reported_total_asset
    )
    snapshot = AccountSnapshot(
        as_of=completed.astimezone(SHANGHAI),
        source="ninef.competition_api.readonly",
        source_kind="competition_api",
        currency="CNY",
        cash_balance=available + frozen,
        available_cash=available,
        current_total_asset=reconciled_total_asset,
        positions=tuple(positions),
        pending_order_count=pending,
    )
    fee_schedule = _fee_schedule(order_rows, completed.astimezone(SHANGHAI))
    if implementation_hash != _implementation_hash():
        raise NinefAccountAdapterError("Ninef adapter changed during account observation")
    receipt = {
        "schema_version": "ninef.account.fetch.v2",
        "started_at": started.astimezone(SHANGHAI).isoformat(),
        "completed_at": completed.astimezone(SHANGHAI).isoformat(),
        "source": BASE_URL,
        "method": "GET_ONLY",
        "adapter_sha256": implementation_hash,
        "endpoints": {name: ENDPOINTS[name][0] for name in ("assets", "positions", "orders")},
        "response_hashes": {
            "assets_start": _hash_payload(assets_start),
            "positions": _hash_payload(positions_response),
            "order_pages": [_hash_payload(payload) for payload in order_payloads],
            "assets_end": _hash_payload(assets_end),
        },
        "orders": {"complete": True, "count": len(orders), "open_count": pending, "items": orders},
        "collection_consistent": True,
        "account_snapshot_hash": snapshot.snapshot_hash,
        "fee_schedule_hash": _hash_payload(fee_schedule.to_dict()) if fee_schedule else None,
    }
    return NinefAccountFetch(snapshot=snapshot, fee_schedule=fee_schedule, receipt=receipt)


__all__ = [
    "NinefAccountAdapterError",
    "NinefAccountFetch",
    "fetch_ninef_account",
    "resolve_ninef_token",
]

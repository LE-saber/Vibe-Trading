"""Small, dependency-free contracts for the W7 execution ledger.

The execution package deliberately accepts one input shape only: the immutable
``p10.execution_plan.v1`` document emitted by W6.  These models do not plan,
rank, resize, or contact a provider; they validate and serialize durable
ledger values around that document.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import hashlib
import json
import re
from typing import Any, Mapping

from competition_core.pipeline.artifacts import content_hash


W6_PLAN_SCHEMA = "p10.execution_plan.v1"
_HEX64 = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}")


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


TERMINAL = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
})


class IntentState(StrEnum):
    PLANNED = "PLANNED"
    SENDING = "SENDING"
    UNKNOWN = "UNKNOWN"
    SUBMITTED = "SUBMITTED"


class BatchState(StrEnum):
    QUEUED = "QUEUED"
    WAITING_FILL = "WAITING_FILL"
    STOP_REQUESTED = "STOP_REQUESTED"
    STOPPED_WITH_OPEN_ORDERS = "STOPPED_WITH_OPEN_ORDERS"
    STOPPED = "STOPPED"
    MANUAL_RECONCILIATION = "MANUAL_RECONCILIATION"
    PAUSED = "PAUSED"
    PAUSED_PROVIDER = "PAUSED_PROVIDER"
    PAUSED_RESTART = "PAUSED_RESTART"
    CANCEL_UNKNOWN = "CANCEL_UNKNOWN"
    REPLAN_REQUIRED = "REPLAN_REQUIRED"
    EXPIRED_WITH_OPEN_ORDERS = "EXPIRED_WITH_OPEN_ORDERS"
    EXPIRED = "EXPIRED"
    COMPLETED = "COMPLETED"


def canonical(value: Any) -> str:
    """Return deterministic JSON for ledger hashes and SQLite documents."""

    def normalize(item: Any) -> Any:
        if isinstance(item, StrEnum):
            return item.value
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("non-finite decimal")
            return format(item.normalize(), "f") if item else "0"
        if isinstance(item, datetime):
            if item.tzinfo is None or item.utcoffset() is None:
                raise ValueError("naive timestamp")
            return item.astimezone(timezone.utc).isoformat()
        if isinstance(item, date):
            return item.isoformat()
        if hasattr(item, "to_dict") and callable(item.to_dict):
            return normalize(item.to_dict())
        if isinstance(item, Mapping):
            return {str(key): normalize(val) for key, val in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalize(val) for val in item]
        if isinstance(item, float):
            if not item.is_integer():
                return item
            return int(item)
        return item

    return json.dumps(normalize(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 240:
        raise ValueError(f"{field_name} is invalid")
    if any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} is invalid")
    return value.strip()


def _identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _hash(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ValueError(f"{field_name} is invalid")
    return value


def _decimal(value: Any, field_name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{field_name} is not numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not numeric") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError(f"{field_name} is outside bounds")
    return result


def _aware(value: Any, field_name: str) -> datetime:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is invalid") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return result


@dataclass(frozen=True, slots=True)
class Capabilities:
    """Provider capability proof; external writes remain opt-in."""

    provider: str = "local"
    external: bool = False
    verified: bool = False
    verification_ref: str | None = None
    buy: bool = False
    sell: bool = False
    cancel: bool = False

    @classmethod
    def from_value(cls, value: Any) -> "Capabilities":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise ValueError("provider capabilities are invalid")
        fields = {"provider", "external", "verified", "verification_ref", "buy", "sell", "cancel"}
        if set(value) - fields:
            raise ValueError("provider capabilities contain unknown fields")
        if any(type(value.get(name, False)) is not bool for name in ("external", "verified", "buy", "sell", "cancel")):
            raise ValueError("provider capability flags must be booleans")
        provider = _text(value.get("provider", "local"), "provider")
        ref = value.get("verification_ref")
        if ref is not None:
            ref = _text(ref, "verification_ref")
        return cls(provider=provider, external=value.get("external", False),
                   verified=value.get("verified", False), verification_ref=ref,
                   buy=value.get("buy", False), sell=value.get("sell", False),
                   cancel=value.get("cancel", False))

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "external": self.external,
            "verified": self.verified,
            "verification_ref": self.verification_ref,
            "buy": self.buy,
            "sell": self.sell,
            "cancel": self.cancel,
        }


@dataclass(frozen=True, slots=True)
class PositionState:
    symbol: str
    quantity: int
    sellable_quantity: int

    def __post_init__(self) -> None:
        _text(self.symbol, "position.symbol")
        if type(self.quantity) is not int or self.quantity < 0:
            raise ValueError("position.quantity is invalid")
        if type(self.sellable_quantity) is not int or not 0 <= self.sellable_quantity <= self.quantity:
            raise ValueError("position.sellable_quantity is invalid")

    @classmethod
    def from_value(cls, value: Any) -> "PositionState":
        if not isinstance(value, dict) or set(value) != {"symbol", "quantity", "sellable_quantity"}:
            raise ValueError("position schema is invalid")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "quantity": self.quantity,
                "sellable_quantity": self.sellable_quantity}


@dataclass(frozen=True, slots=True)
class QuoteState:
    symbol: str
    last: Decimal
    limit_down: Decimal
    limit_up: Decimal
    can_buy: bool
    can_sell: bool
    as_of: datetime

    def __post_init__(self) -> None:
        _text(self.symbol, "quote.symbol")
        for name in ("last", "limit_down", "limit_up"):
            value = _decimal(getattr(self, name), f"quote.{name}", positive=True)
            object.__setattr__(self, name, value)
        if self.limit_down > self.limit_up or not self.limit_down <= self.last <= self.limit_up:
            raise ValueError("quote price band is invalid")
        if type(self.can_buy) is not bool or type(self.can_sell) is not bool:
            raise ValueError("quote capability flags are invalid")
        object.__setattr__(self, "as_of", _aware(self.as_of, "quote.as_of"))

    @classmethod
    def from_value(cls, value: Any) -> "QuoteState":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise ValueError("quote is invalid")
        fields = {"symbol", "last", "limit_down", "limit_up", "can_buy", "can_sell", "as_of"}
        if set(value) != fields:
            raise ValueError("quote schema is invalid")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "last": format(self.last, "f"),
                "limit_down": format(self.limit_down, "f"), "limit_up": format(self.limit_up, "f"),
                "can_buy": self.can_buy, "can_sell": self.can_sell,
                "as_of": self.as_of.isoformat()}


@dataclass(frozen=True, slots=True)
class BrokerOrder:
    order_id: str
    symbol: str
    side: Side
    quantity: int
    filled_quantity: int
    limit_price: Decimal
    status: OrderStatus
    updated_at: datetime
    client_order_id: str | None = None
    average_fill_price: Decimal | None = None
    fee_paid: Decimal | None = None

    def __post_init__(self) -> None:
        _identifier(self.order_id, "broker order ID")
        _text(self.symbol, "broker order symbol")
        if not isinstance(self.side, Side):
            object.__setattr__(self, "side", Side(self.side))
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("broker quantity is invalid")
        if type(self.filled_quantity) is not int or not 0 <= self.filled_quantity <= self.quantity:
            raise ValueError("broker filled quantity is invalid")
        object.__setattr__(self, "limit_price", _decimal(self.limit_price, "broker limit price", positive=True))
        if not isinstance(self.status, OrderStatus):
            object.__setattr__(self, "status", OrderStatus(self.status))
        object.__setattr__(self, "updated_at", _aware(self.updated_at, "broker updated_at"))
        if self.client_order_id is not None:
            _text(self.client_order_id, "broker client order ID")
        if self.average_fill_price is not None:
            object.__setattr__(self, "average_fill_price",
                               _decimal(self.average_fill_price, "average fill price", positive=True))
        if self.fee_paid is not None:
            object.__setattr__(self, "fee_paid", _decimal(self.fee_paid, "fee paid"))
        if self.status is OrderStatus.FILLED and self.filled_quantity != self.quantity:
            raise ValueError("filled receipt quantity is incomplete")
        if self.status is OrderStatus.PARTIALLY_FILLED and not 0 < self.filled_quantity < self.quantity:
            raise ValueError("partial receipt quantity is invalid")

    @property
    def is_open(self) -> bool:
        return self.status not in TERMINAL

    @classmethod
    def from_value(cls, value: Any) -> "BrokerOrder":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise ValueError("broker receipt is invalid")
        fields = {"order_id", "symbol", "side", "quantity", "filled_quantity", "limit_price",
                  "status", "updated_at", "client_order_id", "average_fill_price", "fee_paid"}
        if set(value) != fields:
            raise ValueError("broker receipt schema is invalid")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"order_id": self.order_id, "symbol": self.symbol, "side": self.side.value,
                "quantity": self.quantity, "filled_quantity": self.filled_quantity,
                "limit_price": format(self.limit_price, "f"), "status": self.status.value,
                "updated_at": self.updated_at.isoformat(), "client_order_id": self.client_order_id,
                "average_fill_price": format(self.average_fill_price, "f") if self.average_fill_price is not None else None,
                "fee_paid": format(self.fee_paid, "f") if self.fee_paid is not None else None}


@dataclass(frozen=True, slots=True)
class AccountState:
    account_key: str
    snapshot_hash: str
    available_cash: Decimal
    positions: tuple[PositionState, ...] = ()
    orders: tuple[BrokerOrder, ...] = ()
    orders_complete: bool = True
    consistent: bool = True
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        _identifier(self.account_key, "account key")
        _hash(self.snapshot_hash, "account snapshot hash")
        object.__setattr__(self, "available_cash", _decimal(self.available_cash, "available cash"))
        if any(not isinstance(item, PositionState) for item in self.positions):
            raise ValueError("account positions are invalid")
        if len({item.symbol for item in self.positions}) != len(self.positions):
            raise ValueError("account positions contain duplicates")
        if any(not isinstance(item, BrokerOrder) for item in self.orders):
            raise ValueError("account orders are invalid")
        if len({item.order_id for item in self.orders}) != len(self.orders):
            raise ValueError("account orders contain duplicates")
        if type(self.orders_complete) is not bool or type(self.consistent) is not bool:
            raise ValueError("account reconciliation flags are invalid")
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", _aware(self.observed_at, "account observed_at"))

    @property
    def position_by_symbol(self) -> dict[str, PositionState]:
        return {item.symbol: item for item in self.positions}

    @property
    def open_orders(self) -> tuple[BrokerOrder, ...]:
        return tuple(item for item in self.orders if item.is_open)

    @classmethod
    def from_value(cls, value: Any) -> "AccountState":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise ValueError("account state is invalid")
        positions_value = value.get("positions", [])
        if isinstance(positions_value, dict):
            positions_value = [dict(item, symbol=symbol) for symbol, item in positions_value.items()]
        positions = tuple(PositionState.from_value(item) for item in positions_value)
        orders = tuple(BrokerOrder.from_value(item) for item in value.get("orders", []))
        fields = {"account_key", "snapshot_hash", "available_cash", "positions", "orders",
                  "orders_complete", "consistent", "observed_at"}
        if set(value) != fields:
            raise ValueError("account state schema is invalid")
        return cls(account_key=value["account_key"], snapshot_hash=value["snapshot_hash"],
                   available_cash=value["available_cash"], positions=positions, orders=orders,
                   orders_complete=value["orders_complete"], consistent=value["consistent"],
                   observed_at=value["observed_at"])

    def to_dict(self) -> dict[str, Any]:
        return {"account_key": self.account_key, "snapshot_hash": self.snapshot_hash,
                "available_cash": format(self.available_cash, "f"),
                "positions": [item.to_dict() for item in self.positions],
                "orders": [item.to_dict() for item in self.orders],
                "orders_complete": self.orders_complete, "consistent": self.consistent,
                "observed_at": self.observed_at.isoformat() if self.observed_at else None}


@dataclass(frozen=True, slots=True)
class ProviderContext:
    account: AccountState
    quotes: tuple[QuoteState, ...]
    fee_schedule_hash: str
    quantity_rules_hashes: dict[str, str]
    observed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.account, AccountState):
            raise ValueError("provider account context is invalid")
        if any(not isinstance(item, QuoteState) for item in self.quotes):
            raise ValueError("provider quotes are invalid")
        if len({item.symbol for item in self.quotes}) != len(self.quotes):
            raise ValueError("provider quotes contain duplicates")
        _hash(self.fee_schedule_hash, "fee schedule hash")
        if not isinstance(self.quantity_rules_hashes, dict):
            raise ValueError("quantity rule hashes are invalid")
        for symbol, rule_hash in self.quantity_rules_hashes.items():
            _text(symbol, "quantity rule symbol")
            _hash(rule_hash, "quantity rule hash")
        object.__setattr__(self, "observed_at", _aware(self.observed_at, "provider observed_at"))

    @property
    def quote_by_symbol(self) -> dict[str, QuoteState]:
        return {item.symbol: item for item in self.quotes}

    @classmethod
    def from_value(cls, value: Any) -> "ProviderContext":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict) or set(value) != {"account", "quotes", "fee_schedule_hash", "quantity_rules_hashes", "observed_at"}:
            raise ValueError("provider context schema is invalid")
        quotes = tuple(QuoteState.from_value(item) for item in value["quotes"])
        return cls(account=AccountState.from_value(value["account"]), quotes=quotes,
                   fee_schedule_hash=value["fee_schedule_hash"],
                   quantity_rules_hashes=dict(value["quantity_rules_hashes"]),
                   observed_at=value["observed_at"])

    def to_dict(self) -> dict[str, Any]:
        return {"account": self.account.to_dict(), "quotes": [item.to_dict() for item in self.quotes],
                "fee_schedule_hash": self.fee_schedule_hash,
                "quantity_rules_hashes": dict(self.quantity_rules_hashes),
                "observed_at": self.observed_at.isoformat()}


@dataclass(frozen=True, slots=True)
class ProposedOrder:
    """The exact W6 order row carried into a durable intent."""

    symbol: str
    side: Side
    quantity: int
    limit_price: Decimal
    estimated_fee: Decimal
    current_quantity: int
    target_quantity: int
    order_ref: str
    quantity_rules_hash: str
    raw: dict[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        _text(self.symbol, "order symbol")
        if not isinstance(self.side, Side):
            object.__setattr__(self, "side", Side(self.side))
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("order quantity is invalid")
        if type(self.current_quantity) is not int or self.current_quantity < 0:
            raise ValueError("order current quantity is invalid")
        if type(self.target_quantity) is not int or self.target_quantity < 0:
            raise ValueError("order target quantity is invalid")
        if self.target_quantity != self.current_quantity + (self.quantity if self.side is Side.BUY else -self.quantity):
            raise ValueError("order direction/quantity is invalid")
        object.__setattr__(self, "limit_price", _decimal(self.limit_price, "order limit price", positive=True))
        object.__setattr__(self, "estimated_fee", _decimal(self.estimated_fee, "order estimated fee"))
        _identifier(self.order_ref, "order reference")
        _hash(self.quantity_rules_hash, "order quantity rule hash")
        if not isinstance(self.raw, dict):
            raise ValueError("order source row is invalid")

    @classmethod
    def from_w6(cls, value: Any) -> "ProposedOrder":
        if not isinstance(value, dict):
            raise ValueError("W6 order row is invalid")
        required = {"symbol", "side", "quantity", "current_quantity", "target_quantity",
                    "order_type", "limit_price", "estimated_fee", "quantity_rules",
                    "quantity_rules_hash", "order_ref", "reference_price", "gross_value",
                    "price_limits"}
        if not required <= set(value) or value.get("order_type") != "LIMIT":
            raise ValueError("W6 order row is not a fixed LIMIT order")
        if not isinstance(value["quantity_rules"], dict) or content_hash(value["quantity_rules"]) != value["quantity_rules_hash"]:
            raise ValueError("W6 quantity rule proof is invalid")
        side = Side(value["side"])
        raw = deepcopy(value)
        return cls(symbol=value["symbol"], side=side, quantity=value["quantity"],
                   limit_price=value["limit_price"], estimated_fee=value["estimated_fee"],
                   current_quantity=value["current_quantity"], target_quantity=value["target_quantity"],
                   order_ref=value["order_ref"], quantity_rules_hash=value["quantity_rules_hash"], raw=raw)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)


@dataclass(frozen=True, slots=True)
class Approval:
    approval_id: str
    plan_id: str
    plan_hash: str
    run_id: str
    seal_hash: str
    actor: str
    approved_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.approval_id, "approval ID")
        _identifier(self.plan_id, "plan ID")
        _hash(self.plan_hash, "approval plan hash")
        _identifier(self.run_id, "run ID")
        _hash(self.seal_hash, "approval seal hash")
        _identifier(self.actor, "approval actor")
        object.__setattr__(self, "approved_at", _aware(self.approved_at, "approval time"))
        object.__setattr__(self, "expires_at", _aware(self.expires_at, "approval expiry"))
        if self.expires_at <= self.approved_at:
            raise ValueError("approval expiry must be after approval time")

    @classmethod
    def from_value(cls, value: Any) -> "Approval":
        if not isinstance(value, dict):
            raise ValueError("approval is invalid")
        fields = {"approval_id", "plan_id", "plan_hash", "run_id", "seal_hash", "actor", "approved_at", "expires_at"}
        if set(value) != fields:
            raise ValueError("approval schema is invalid")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"approval_id": self.approval_id, "plan_id": self.plan_id, "plan_hash": self.plan_hash,
                "run_id": self.run_id, "seal_hash": self.seal_hash, "actor": self.actor,
                "approved_at": self.approved_at.isoformat(), "expires_at": self.expires_at.isoformat()}


@dataclass(frozen=True, slots=True)
class OperatorResolution:
    resolution_id: str
    batch_id: str
    intent_id: str
    original_intent_hash: str
    broker_order_id: str
    receipt_hash: str
    evidence_ref: str
    operator: str
    observed_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.resolution_id, "resolution ID")
        _text(self.batch_id, "batch ID")
        _text(self.intent_id, "intent ID")
        _hash(self.original_intent_hash, "original intent hash")
        _identifier(self.broker_order_id, "broker order ID")
        _hash(self.receipt_hash, "receipt hash")
        _text(self.evidence_ref, "evidence reference")
        _identifier(self.operator, "operator")
        object.__setattr__(self, "observed_at", _aware(self.observed_at, "resolution time"))

    def to_dict(self) -> dict[str, Any]:
        return {"resolution_id": self.resolution_id, "batch_id": self.batch_id,
                "intent_id": self.intent_id, "original_intent_hash": self.original_intent_hash,
                "broker_order_id": self.broker_order_id, "receipt_hash": self.receipt_hash,
                "evidence_ref": self.evidence_ref, "operator": self.operator,
                "observed_at": self.observed_at.isoformat()}


def validate_w6_plan(value: Any) -> dict[str, Any]:
    """Validate W6's self-hashes and fixed-order boundary without re-planning."""

    if not isinstance(value, dict):
        raise ValueError("execution plan must be an object")
    raw = deepcopy(value)
    integrity_hash = raw.pop("integrity_hash", None)
    if not isinstance(integrity_hash, str) or content_hash(raw) != integrity_hash:
        raise ValueError("W6 plan integrity hash mismatch")
    stored_plan_hash = raw.get("plan_hash")
    if not isinstance(stored_plan_hash, str) or _HEX64.fullmatch(stored_plan_hash) is None:
        raise ValueError("W6 plan hash is invalid")
    body = deepcopy(raw)
    body.pop("plan_hash", None)
    approval = body.get("approval")
    if not isinstance(approval, dict) or approval.get("plan_hash") != stored_plan_hash:
        raise ValueError("W6 approval hash binding is invalid")
    approval_for_hash = deepcopy(approval)
    approval_for_hash["plan_hash"] = None
    body["approval"] = approval_for_hash
    if content_hash(body) != stored_plan_hash:
        raise ValueError("W6 plan hash mismatch")
    if raw.get("schema_version") != W6_PLAN_SCHEMA or raw.get("origin") != "competition_s6":
        raise ValueError("only p10.execution_plan.v1 is accepted")
    if (raw.get("research_only") is not True or raw.get("external_write_allowed") is not False
            or raw.get("production_accepted") is not False or raw.get("orders_submitted") != 0):
        raise ValueError("W6 plan write boundary is invalid")
    plan_id = _identifier(raw.get("plan_id"), "plan ID")
    _identifier(raw.get("run_id"), "run ID")
    if approval.get("state") != "NOT_APPROVED" or approval.get("required") is not True or approval.get("plan_id") != plan_id:
        raise ValueError("W6 plan must remain unapproved before ledger approval")
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("W6 source binding is missing")
    for field_name in ("portfolio_seal_hash", "s6_artifact_hash", "s6_semantic_hash"):
        _hash(source.get(field_name), f"W6 source {field_name}")
    account = raw.get("account")
    if not isinstance(account, dict):
        raise ValueError("W6 account binding is missing")
    _identifier(account.get("account_key"), "W6 account key")
    _hash(account.get("snapshot_hash"), "W6 account snapshot hash")
    _hash(account.get("source_hash"), "W6 account source hash")
    if account.get("pending_order_count") != 0:
        raise ValueError("W6 plan has pending orders")
    market = raw.get("market")
    if not isinstance(market, dict):
        raise ValueError("W6 market binding is missing")
    _hash(market.get("fee_schedule_hash"), "W6 fee schedule hash")
    orders = raw.get("orders")
    if not isinstance(orders, list) or not orders:
        raise ValueError("W6 plan has no fixed orders")
    parsed = [ProposedOrder.from_w6(item) for item in orders]
    if len({item.order_ref for item in parsed}) != len(parsed):
        raise ValueError("W6 order references are not unique")
    raw["plan_hash"] = stored_plan_hash
    raw["integrity_hash"] = integrity_hash
    return raw


__all__ = [
    "AccountState", "Approval", "BatchState", "BrokerOrder", "Capabilities",
    "IntentState", "OperatorResolution", "OrderStatus", "PositionState",
    "ProposedOrder", "ProviderContext", "QuoteState", "Side", "TERMINAL",
    "W6_PLAN_SCHEMA", "canonical", "digest", "validate_w6_plan",
]

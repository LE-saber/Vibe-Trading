"""New application contracts, not a claimed copy of any unavailable legacy schema.

Money uses decimal strings, quantities strict integers, timestamps aware UTC.
Hashing is an application encoding, NOT RFC 8785 or a digital signature.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator, AwareDatetime, BaseModel, BeforeValidator, ConfigDict,
    Field, StrictBool, StrictInt, model_validator,
)


def decimal_input(value: Any) -> Any:
    if isinstance(value, (float, bool)):
        raise ValueError("financial values must be decimal strings or integers")
    return value


def utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


SignedMoney = Annotated[Decimal, BeforeValidator(decimal_input), Field(allow_inf_nan=False)]
Money = Annotated[SignedMoney, Field(ge=0)]
Price = Annotated[SignedMoney, Field(gt=0)]
Weight = Annotated[SignedMoney, Field(ge=0, le=1)]
Qty = Annotated[StrictInt, Field(ge=0)]
PositiveQty = Annotated[StrictInt, Field(gt=0)]
Symbol = Annotated[str, Field(pattern=r"^[0-9]{6}\.(SH|SZ)$")]
Identifier = Annotated[str, Field(min_length=1, max_length=160, pattern=r"^[\w.:-]+$")]
Timestamp = Annotated[AwareDatetime, AfterValidator(utc)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def canonical(value: Any) -> str:
    def normalize(item: Any) -> Any:
        if isinstance(item, BaseModel):
            return normalize(item.model_dump(mode="python"))
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("non-finite decimal")
            return format(item.normalize(), "f") if item else "0"
        if isinstance(item, datetime):
            if item.tzinfo is None:
                raise ValueError("naive timestamp")
            return item.astimezone(timezone.utc).isoformat()
        if isinstance(item, date):
            return item.isoformat()
        if isinstance(item, dict):
            return {key: normalize(val) for key, val in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalize(val) for val in item]
        if isinstance(item, float):
            raise ValueError("binary float in financial payload")
        return item
    return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


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


TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}


class Position(Contract):
    symbol: Symbol
    name: str = Field(default="", max_length=80)
    quantity: Qty
    sellable_quantity: Qty
    frozen_quantity: Qty
    market_value: Money

    @model_validator(mode="after")
    def quantities(self) -> Position:
        if self.sellable_quantity + self.frozen_quantity > self.quantity:
            raise ValueError("sellable + frozen exceeds quantity")
        return self


class BrokerOrder(Contract):
    order_id: Identifier
    symbol: Symbol
    side: Side
    quantity: PositiveQty
    filled_quantity: Qty
    limit_price: Price
    status: OrderStatus
    average_fill_price: Price | None = None
    fee_paid: Money | None = None
    updated_at: Timestamp
    client_order_id: Identifier | None = None

    @model_validator(mode="after")
    def consistency(self) -> BrokerOrder:
        if self.filled_quantity > self.quantity:
            raise ValueError("overfill")
        if self.status == OrderStatus.FILLED and self.filled_quantity != self.quantity:
            raise ValueError("FILLED without full quantity")
        if self.status == OrderStatus.PARTIALLY_FILLED and not 0 < self.filled_quantity < self.quantity:
            raise ValueError("invalid partial fill")
        return self

    @property
    def is_open(self) -> bool:
        return self.status not in TERMINAL


class AccountSnapshot(Contract):
    schema_version: Literal["vibe.account.v1"] = "vibe.account.v1"
    account_id: Identifier
    provider: Identifier
    as_of: Timestamp
    collection_started_at: Timestamp
    total_assets: Price
    available_cash: Money
    frozen_cash: Money
    positions: tuple[Position, ...]
    orders: tuple[BrokerOrder, ...]
    orders_complete: StrictBool = False
    consistent: StrictBool = False
    source_ref: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def consistency(self) -> AccountSnapshot:
        if len({p.symbol for p in self.positions}) != len(self.positions):
            raise ValueError("duplicate position")
        if len({o.order_id for o in self.orders}) != len(self.orders):
            raise ValueError("duplicate order")
        if self.collection_started_at > self.as_of:
            raise ValueError("negative collection interval")
        total = self.available_cash + self.frozen_cash + sum((p.market_value for p in self.positions), Decimal(0))
        if abs(total - self.total_assets) > Decimal("0.02"):
            raise ValueError("NAV does not reconcile to cash plus positions")
        return self


def account_state_hash(account: AccountSnapshot) -> str:
    """Exclude volatile valuation/timestamps; retain actual financial resources."""
    return digest({
        "account": account.account_id, "provider": account.provider,
        "cash": account.available_cash, "frozen": account.frozen_cash,
        "positions": sorted((p.symbol, p.quantity, p.sellable_quantity, p.frozen_quantity)
                            for p in account.positions if p.quantity),
        "open_orders": sorted((o.order_id, o.symbol, o.side, o.quantity, o.filled_quantity,
                               o.limit_price, o.status) for o in account.orders if o.is_open),
    })


class Quote(Contract):
    symbol: Symbol
    as_of: Timestamp
    last: Price
    limit_down: Price
    limit_up: Price
    tradable: StrictBool
    can_buy: StrictBool
    can_sell: StrictBool
    source_ref: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def band(self) -> Quote:
        if not self.limit_down <= self.last <= self.limit_up:
            raise ValueError("quote outside price band")
        return self


class InstrumentRule(Contract):
    symbol: Symbol
    buy_lot: PositiveQty
    sell_lot: PositiveQty
    allow_odd_lot_liquidation: StrictBool
    tick: Price
    valid_from: date
    valid_until: date
    source_ref: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def dates(self) -> InstrumentRule:
        if self.valid_until < self.valid_from:
            raise ValueError("invalid rule interval")
        return self


class FeeSchedule(Contract):
    version: Identifier
    commission_rate: Weight
    minimum_commission: Money
    buy_tax_rate: Weight
    sell_tax_rate: Weight
    transfer_rate: Weight
    valid_from: date
    valid_until: date
    source_ref: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def dates(self) -> FeeSchedule:
        if self.valid_until < self.valid_from:
            raise ValueError("invalid fee interval")
        return self


class TradingContext(Contract):
    account: AccountSnapshot
    quotes: tuple[Quote, ...]
    rules: tuple[InstrumentRule, ...]
    fees: FeeSchedule
    market_open: StrictBool
    trading_date: date

    @model_validator(mode="after")
    def unique(self) -> TradingContext:
        for items in (self.quotes, self.rules):
            if len({item.symbol for item in items}) != len(items):
                raise ValueError("duplicate quote/rule")
        return self


class NewsItem(Contract):
    news_id: Identifier
    title: str = Field(min_length=1, max_length=400)
    body: str = Field(max_length=12000)
    symbols: tuple[Symbol, ...] = ()
    published_at: Timestamp
    fetched_at: Timestamp
    source: str = Field(min_length=1, max_length=120)
    source_ref: str = Field(min_length=1, max_length=240)


class RankingEntry(Contract):
    participant_id: Identifier
    display_name: str = Field(max_length=80)
    rank: PositiveQty
    return_pct: SignedMoney


class Leaderboard(Contract):
    competition_id: Identifier
    as_of: Timestamp
    entries: tuple[RankingEntry, ...]
    own: RankingEntry | None
    source_ref: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def people(self) -> Leaderboard:
        if len(self.entries) > 30:
            raise ValueError("top-30 contract exceeded")
        if len({e.participant_id for e in self.entries}) != len(self.entries):
            raise ValueError("duplicate participant")
        return self


class Capabilities(Contract):
    provider: Identifier
    environment: Literal["DEMO", "NINEF_SIMULATION"]
    account: StrictBool = False
    rankings: StrictBool = False
    news: StrictBool = False
    buy: StrictBool = False
    sell: StrictBool = False
    cancel: StrictBool = False
    verified: StrictBool = False
    verification_ref: str = ""


class ResearchRequest(Contract):
    request_id: Identifier
    requested_at: Timestamp
    account: AccountSnapshot
    news: tuple[NewsItem, ...]
    competition: Leaderboard | None


class Target(Contract):
    symbol: Symbol
    weight: Weight
    reason: str = Field(min_length=1, max_length=1500)
    verdict: Literal["KEEP", "REDUCE", "EXIT", "WATCH", "REJECT"]
    evidence_ids: tuple[str, ...] = ()


class ResearchResult(Contract):
    schema_version: Literal["vibe.research.v1"] = "vibe.research.v1"
    run_id: Identifier
    as_of: Timestamp
    candidates: tuple[Symbol, ...]
    targets: tuple[Target, ...]
    evidence_ids: tuple[str, ...]
    artifact_hash: Sha256
    strategy_version: str = Field(min_length=1, max_length=160)
    news_context_hash: Sha256
    demo: StrictBool

    @model_validator(mode="after")
    def allocation(self) -> ResearchResult:
        if len({t.symbol for t in self.targets}) != len(self.targets):
            raise ValueError("duplicate target")
        if sum((t.weight for t in self.targets), Decimal(0)) > 1:
            raise ValueError("overallocated targets")
        if any(set(t.evidence_ids) - set(self.evidence_ids) for t in self.targets):
            raise ValueError("invented evidence reference")
        return self


class Policy(Contract):
    cash_reserve: Weight = Decimal("0.20")
    max_position_weight: Weight = Decimal("0.30")
    max_quote_age_seconds: PositiveQty = 20
    max_snapshot_age_seconds: PositiveQty = 20
    max_collection_seconds: PositiveQty = 10
    max_research_age_seconds: PositiveQty = 1800
    plan_lifetime_seconds: PositiveQty = 300
    max_price_drift: Weight = Decimal("0.01")


class Blocker(Contract):
    code: str
    detail: str
    symbol: Symbol | None = None


class ProposedOrder(Contract):
    symbol: Symbol
    side: Side
    quantity: PositiveQty
    limit_price: Price
    estimated_fee: Money
    current_quantity: Qty
    target_quantity: Qty
    reason: str


class TargetPosition(Contract):
    symbol: Symbol
    current_quantity: Qty
    requested_quantity: Qty
    planned_quantity: Qty
    weight: Weight
    reason: str


class Plan(Contract):
    schema_version: Literal["vibe.plan.v1"] = "vibe.plan.v1"
    plan_id: Identifier
    run_id: Identifier
    created_at: Timestamp
    expires_at: Timestamp
    research_hash: Sha256
    context: TradingContext
    policy: Policy
    targets: tuple[TargetPosition, ...]
    orders: tuple[ProposedOrder, ...]
    blockers: tuple[Blocker, ...]
    warnings: tuple[str, ...]
    projected_cash: SignedMoney
    cash_floor: Money
    demo: StrictBool


class Approval(Contract):
    approval_id: Identifier
    plan_id: Identifier
    plan_hash: Sha256
    actor: Identifier
    approved_at: Timestamp
    expires_at: Timestamp

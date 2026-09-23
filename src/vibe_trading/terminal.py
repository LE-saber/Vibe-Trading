"""Read-only contracts for the competition terminal strategy.

The terminal strategy is deliberately a separate contract family from the
canonical ``ResearchResult``/``Plan`` chain.  These models describe bounded
research facts and a shadow allocation intent; they never contain broker
orders, quantities, approval data, or execution methods.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, StrictBool, StrictInt, model_validator

from .models import (
    Contract,
    Identifier,
    Money,
    Price,
    Sha256,
    SignedMoney,
    Symbol,
    Timestamp,
    Weight,
)


TerminalReason = Annotated[str, Field(min_length=1, max_length=500)]
ClockTime = Annotated[str, Field(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")]


class TerminalMode(StrEnum):
    PROTECT = "PROTECT"
    NORMAL = "NORMAL"
    ATTACK = "ATTACK"
    LAST_SHOT = "LAST_SHOT"
    UNAVAILABLE = "UNAVAILABLE"


class TerminalConfidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNAVAILABLE = "UNAVAILABLE"


TerminalStatus = Literal["COMPLETE", "BLOCKED", "UNAVAILABLE"]


class TerminalCandidateSummary(Contract):
    """One candidate's bounded, non-order terminal facts."""

    symbol: Symbol
    name: str = Field(min_length=1, max_length=80)
    score: SignedMoney | None = None
    trigger_price: Price | None = None
    invalidation_price: Price | None = None
    limit_price: Price | None = Field(
        default=None,
        description="Candidate limit rule only; never a broker order price.",
    )
    max_weight: Weight | None = None
    hard_gate_pass: StrictBool
    eligible: StrictBool
    same_day_exit_unavailable: StrictBool = False
    blockers: tuple[TerminalReason, ...] = ()
    reasons: tuple[TerminalReason, ...] = ()
    source_hash: Sha256

    @model_validator(mode="after")
    def eligibility(self) -> TerminalCandidateSummary:
        if self.eligible and not self.hard_gate_pass:
            raise ValueError("terminal candidate cannot be eligible when its hard gate fails")
        return self


class TerminalAwardLine(Contract):
    """Award-line facts and required-return inputs, without a target order."""

    award_rank_cutoff: int | None = Field(default=None, ge=1)
    current_award_line: Money | None = None
    predicted_award_line: Money | None = None
    favorable_award_line: Money | None = None
    base_award_line: Money | None = None
    adverse_award_line: Money | None = None
    safety_buffer: Weight | None = None
    target_asset: Money | None = None
    required_return: SignedMoney | None = None
    confidence: TerminalConfidence = TerminalConfidence.UNAVAILABLE
    method: str = Field(default="", max_length=120)


class TerminalAllocationSummary(Contract):
    """Budget intent only; it cannot represent a broker order."""

    core_symbols: tuple[Symbol, ...] = ()
    aggressive_symbols: tuple[Symbol, ...] = ()
    core_budget: Weight
    aggressive_budget: Weight
    cash_target: Weight
    total_position_count: StrictInt = Field(ge=0, le=8)
    min_positions: StrictInt = Field(default=5, ge=5, le=8)
    max_positions: StrictInt = Field(default=8, ge=5, le=8)
    max_single_weight: Weight = Field(default=Decimal("0.20"), le=Decimal("0.20"))
    same_day_exit_unavailable: tuple[Symbol, ...] = ()

    @model_validator(mode="after")
    def safe_budget(self) -> TerminalAllocationSummary:
        core = set(self.core_symbols)
        aggressive = set(self.aggressive_symbols)
        if len(core) != len(self.core_symbols):
            raise ValueError("terminal core symbols must be unique")
        if len(aggressive) != len(self.aggressive_symbols):
            raise ValueError("terminal aggressive symbols must be unique")
        if core & aggressive:
            raise ValueError("terminal core and aggressive symbols cannot overlap")
        if len(self.aggressive_symbols) > 3:
            raise ValueError("terminal aggressive sleeve cannot exceed three candidates")
        if self.total_position_count != len(self.core_symbols) + len(self.aggressive_symbols):
            raise ValueError("terminal position count does not match symbol sets")
        if self.max_positions < self.min_positions:
            raise ValueError("terminal position bounds are invalid")
        if not self.min_positions <= self.total_position_count <= self.max_positions:
            raise ValueError("terminal position count must remain within configured bounds")
        if self.core_budget + self.aggressive_budget + self.cash_target != 1:
            raise ValueError("terminal budgets must sum to one")
        if not set(self.same_day_exit_unavailable) <= aggressive:
            raise ValueError("same-day exit markers must belong to aggressive symbols")
        return self


class TerminalSchedule(Contract):
    """Shanghai-local decision and risk-freeze clock labels."""

    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    candidate_rank: ClockTime
    opening_observation_end: ClockTime
    midday_refresh: ClockTime
    primary_decision: ClockTime
    final_revision: ClockTime
    main_order_deadline: ClockTime
    risk_freeze: ClockTime
    risk_frozen: StrictBool = False


class TerminalSourceBindings(Contract):
    """Content hashes tying a projection to immutable upstream evidence."""

    s4_hash: Sha256 | None = None
    s5_hash: Sha256 | None = None
    s5_5_hash: Sha256 | None = None
    account_snapshot_hash: Sha256 | None = None
    leaderboard_snapshot_hash: Sha256 | None = None
    award_line_history_hash: Sha256 | None = None
    config_hash: Sha256 | None = None
    implementation_hash: Sha256 | None = None
    realtime_observation_id: Identifier | None = None


class TerminalProjection(Contract):
    """Bounded terminal research result; permanently shadow-only."""

    schema_version: Literal["vibe.terminal-projection.v1"] = "vibe.terminal-projection.v1"
    run_id: Identifier
    evaluation_id: Sha256
    as_of: Timestamp
    trade_date: date
    remaining_trade_dates: tuple[date, ...]
    status: TerminalStatus
    mode: TerminalMode
    reasons: tuple[TerminalReason, ...] = ()
    blockers: tuple[TerminalReason, ...] = ()
    award_line: TerminalAwardLine
    watchlist: tuple[TerminalCandidateSummary, ...] = ()
    focus: tuple[TerminalCandidateSummary, ...] = ()
    terminal: tuple[TerminalCandidateSummary, ...] = ()
    allocation: TerminalAllocationSummary | None = None
    schedule: TerminalSchedule
    source_bindings: TerminalSourceBindings | None = None
    shadow_only: StrictBool = True
    execution_ready: StrictBool = False
    production_accepted: StrictBool = False
    orders_submitted: StrictInt = 0

    @model_validator(mode="after")
    def integrity(self) -> TerminalProjection:
        if self.shadow_only is not True:
            raise ValueError("terminal projection is permanently shadow-only")
        if self.execution_ready is not False or self.production_accepted is not False:
            raise ValueError("terminal projection cannot be execution-ready or production-accepted")
        if self.orders_submitted != 0:
            raise ValueError("terminal projection cannot submit orders")
        dates = self.remaining_trade_dates
        if not dates or len(set(dates)) != len(dates) or tuple(sorted(dates)) != dates:
            raise ValueError("terminal remaining trade dates must be sorted and unique")
        if self.trade_date not in dates or any(item < self.trade_date for item in dates):
            raise ValueError("terminal remaining trade dates must include the trade date")
        watch = tuple(item.symbol for item in self.watchlist)
        focus = tuple(item.symbol for item in self.focus)
        terminal = tuple(item.symbol for item in self.terminal)
        if len(set(watch)) != len(watch) or len(set(focus)) != len(focus) or len(set(terminal)) != len(terminal):
            raise ValueError("terminal candidate tiers must not contain duplicate symbols")
        if len(watch) > 10 or len(focus) > 5 or len(terminal) > 3:
            raise ValueError("terminal candidate tier exceeds configured bound")
        if not set(focus) <= set(watch) or not set(terminal) <= set(focus):
            raise ValueError("terminal candidate tiers must be nested watch -> focus -> terminal")
        if self.status == "COMPLETE":
            if self.mode is TerminalMode.UNAVAILABLE:
                raise ValueError("complete terminal projection cannot have unavailable mode")
            if self.source_bindings is None:
                raise ValueError("complete terminal projection requires source bindings")
            required_hashes = (
                self.source_bindings.s4_hash,
                self.source_bindings.s5_hash,
                self.source_bindings.s5_5_hash,
                self.source_bindings.account_snapshot_hash,
                self.source_bindings.leaderboard_snapshot_hash,
                self.source_bindings.config_hash,
                self.source_bindings.implementation_hash,
            )
            if any(value is None for value in required_hashes):
                raise ValueError("complete terminal projection has incomplete source bindings")
            if len(watch) != 10 or len(focus) != 5:
                raise ValueError("complete terminal projection requires 10 watch and 5 focus candidates")
            if self.mode is TerminalMode.PROTECT:
                if terminal or self.allocation is None or self.allocation.aggressive_symbols:
                    raise ValueError("Protect terminal projection cannot add aggressive candidates")
            elif not 2 <= len(terminal) <= 3:
                raise ValueError("non-Protect terminal projection requires two to three candidates")
            if self.allocation is None:
                raise ValueError("complete terminal projection requires allocation summary")
        return self


class TerminalShadowPlan(Contract):
    """A bounded shadow allocation record, never a canonical executable Plan."""

    schema_version: Literal["vibe.terminal-shadow-plan.v1"] = "vibe.terminal-shadow-plan.v1"
    plan_id: Identifier
    run_id: Identifier
    evaluation_id: Sha256
    created_at: Timestamp
    status: TerminalStatus
    mode: TerminalMode
    allocation: TerminalAllocationSummary | None = None
    schedule: TerminalSchedule
    source_bindings: TerminalSourceBindings | None = None
    reasons: tuple[TerminalReason, ...] = ()
    blockers: tuple[TerminalReason, ...] = ()
    shadow_only: StrictBool = True
    execution_ready: StrictBool = False
    production_accepted: StrictBool = False
    orders_submitted: StrictInt = 0

    @model_validator(mode="after")
    def integrity(self) -> TerminalShadowPlan:
        if self.shadow_only is not True:
            raise ValueError("terminal shadow plan is permanently shadow-only")
        if self.execution_ready is not False or self.production_accepted is not False:
            raise ValueError("terminal shadow plan cannot be execution-ready or production-accepted")
        if self.orders_submitted != 0:
            raise ValueError("terminal shadow plan cannot submit orders")
        if self.status == "COMPLETE" and self.mode is TerminalMode.UNAVAILABLE:
            raise ValueError("complete terminal shadow plan cannot have unavailable mode")
        if self.status == "COMPLETE" and (self.allocation is None or self.source_bindings is None):
            raise ValueError("complete terminal shadow plan requires allocation and source bindings")
        return self


__all__ = [
    "TerminalAllocationSummary",
    "TerminalAwardLine",
    "TerminalCandidateSummary",
    "TerminalConfidence",
    "TerminalMode",
    "TerminalProjection",
    "TerminalSchedule",
    "TerminalShadowPlan",
    "TerminalSourceBindings",
    "TerminalStatus",
]

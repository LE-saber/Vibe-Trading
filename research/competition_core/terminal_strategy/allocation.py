"""Mode-aware, read-only terminal allocation policy.

This module deliberately stops at a strategy-intent boundary.  It describes
which existing core symbols are preserved, which already screened terminal
symbols may belong to an aggressive sleeve, and how the portfolio budget is
partitioned between core, aggressive, and cash.  It never turns a budget into
broker quantities, prices, orders, or execution calls; S6 remains the sole
authority for those concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash

from .candidate_funnel import CandidateAssessment, CandidateBlocker, FunnelResult
from .config import TerminalStrategyConfig
from .models import TerminalMode, TerminalModeDecision, TerminalModelError


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_SINGLE_NAME_CAP = Decimal("0.20")
_MIN_POSITIONS = 5
_MAX_POSITIONS = 8
_MIN_TERMINAL_CANDIDATES = 2
_MAX_TERMINAL_CANDIDATES = 3


class TerminalAllocationError(TerminalModelError):
    """Raised when a safe allocation policy cannot be produced."""

    def __init__(
        self,
        message: str,
        *,
        blockers: tuple["AllocationBlocker", ...],
        reasons: tuple[str, ...],
        input_hash: str | None = None,
    ) -> None:
        super().__init__(message)
        self.blockers = blockers
        self.reasons = reasons
        self.input_hash = input_hash


class AllocationBlocker(StrEnum):
    """Machine-readable reasons why a policy is not usable."""

    INVALID_INPUT = "INVALID_INPUT"
    MODE_UNAVAILABLE = "MODE_UNAVAILABLE"
    MODE_DECISION_BLOCKED = "MODE_DECISION_BLOCKED"
    MODE_MISMATCH = "MODE_MISMATCH"
    FINAL_DAY_REQUIRED = "FINAL_DAY_REQUIRED"
    FUNNEL_UNAVAILABLE = "FUNNEL_UNAVAILABLE"
    FUNNEL_NOT_READY = "FUNNEL_NOT_READY"
    FUNNEL_CONFIG_MISMATCH = "FUNNEL_CONFIG_MISMATCH"
    FUNNEL_DATE_MISMATCH = "FUNNEL_DATE_MISMATCH"
    TERMINAL_CANDIDATE_SHORTFALL = "TERMINAL_CANDIDATE_SHORTFALL"
    TERMINAL_CANDIDATE_BLOCKED = "TERMINAL_CANDIDATE_BLOCKED"
    TERMINAL_CANDIDATE_DUPLICATE = "TERMINAL_CANDIDATE_DUPLICATE"
    CORE_SYMBOL_INVALID = "CORE_SYMBOL_INVALID"
    CORE_HOLDINGS_DUPLICATE = "CORE_HOLDINGS_DUPLICATE"
    CORE_AGGRESSIVE_OVERLAP = "CORE_AGGRESSIVE_OVERLAP"
    POSITION_COUNT_INVALID = "POSITION_COUNT_INVALID"
    AGGRESSIVE_COUNT_INVALID = "AGGRESSIVE_COUNT_INVALID"
    SINGLE_NAME_CAP_EXCEEDED = "SINGLE_NAME_CAP_EXCEEDED"
    MODE_BUDGET_OUT_OF_RANGE = "MODE_BUDGET_OUT_OF_RANGE"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    PROTECT_RISK_INCREASE = "PROTECT_RISK_INCREASE"
    SAME_DAY_EXIT_UNAVAILABLE = "SAME_DAY_EXIT_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class TerminalAllocationInputs:
    """Typed inputs for one allocation-policy evaluation.

    ``core_symbols`` are existing holdings or already-approved core research
    symbols supplied by the caller.  They are preserved as symbols only; this
    input has no quantities or order semantics.  The optional same-day tuple
    records a fact established by an upstream adapter and is used only to
    annotate a Last-shot policy.
    """

    mode_decision: TerminalModeDecision
    funnel: FunnelResult
    core_symbols: tuple[str, ...]
    same_day_exit_unavailable_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.mode_decision, TerminalModeDecision):
            raise TerminalAllocationError(
                "mode_decision must be TerminalModeDecision",
                blockers=(AllocationBlocker.INVALID_INPUT,),
                reasons=("MODE_DECISION_TYPE_INVALID",),
            )
        if not isinstance(self.funnel, FunnelResult):
            raise TerminalAllocationError(
                "funnel must be FunnelResult",
                blockers=(AllocationBlocker.INVALID_INPUT,),
                reasons=("FUNNEL_TYPE_INVALID",),
            )
        if not isinstance(self.core_symbols, tuple):
            raise TerminalAllocationError(
                "core_symbols must be a tuple",
                blockers=(AllocationBlocker.INVALID_INPUT,),
                reasons=("CORE_SYMBOLS_TYPE_INVALID",),
            )
        if not isinstance(self.same_day_exit_unavailable_symbols, tuple):
            raise TerminalAllocationError(
                "same_day_exit_unavailable_symbols must be a tuple",
                blockers=(AllocationBlocker.INVALID_INPUT,),
                reasons=("SAME_DAY_SYMBOLS_TYPE_INVALID",),
            )
        for symbol in self.core_symbols + self.same_day_exit_unavailable_symbols:
            if not isinstance(symbol, str) or not symbol.strip():
                raise TerminalAllocationError(
                    "allocation symbols must be non-empty strings",
                    blockers=(AllocationBlocker.INVALID_INPUT,),
                    reasons=("SYMBOL_TYPE_INVALID",),
                )
        object.__setattr__(
            self,
            "core_symbols",
            tuple(symbol.strip() for symbol in self.core_symbols),
        )
        object.__setattr__(
            self,
            "same_day_exit_unavailable_symbols",
            tuple(symbol.strip() for symbol in self.same_day_exit_unavailable_symbols),
        )

    @property
    def input_hash(self) -> str:
        """Stable hash of all policy inputs, including upstream hashes."""

        return content_hash(
            {
                "schema_version": "competition.terminal-allocation-inputs.v1",
                "mode_decision": self.mode_decision.to_dict(),
                "funnel": self.funnel.to_dict(),
                "core_symbols": sorted(self.core_symbols),
                "same_day_exit_unavailable_symbols": sorted(
                    self.same_day_exit_unavailable_symbols
                ),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.terminal-allocation-inputs.v1",
            "mode_decision": self.mode_decision.to_dict(),
            "funnel_hash": self.funnel.funnel_hash,
            "core_symbols": list(sorted(self.core_symbols)),
            "same_day_exit_unavailable_symbols": list(
                sorted(self.same_day_exit_unavailable_symbols)
            ),
            "input_hash": self.input_hash,
        }


@dataclass(frozen=True, slots=True)
class TerminalAllocationPolicy:
    """Immutable budget intent for the read-only terminal strategy layer.

    The two symbol tuples are deliberately the only symbol-level outputs:
    ``core_symbols`` preserve the existing core and ``aggressive_symbols``
    contain at most three funnel-passed terminal candidates.  There are no
    quantities, prices, order instructions, or execution methods here.
    """

    mode: TerminalMode
    as_of: datetime
    trade_date: date
    core_symbols: tuple[str, ...]
    aggressive_symbols: tuple[str, ...]
    total_position_count: int
    terminal_candidate_count: int
    core_budget: Decimal
    aggressive_budget: Decimal
    cash_target: Decimal
    min_positions: int = _MIN_POSITIONS
    max_positions: int = _MAX_POSITIONS
    max_single_weight: Decimal = _SINGLE_NAME_CAP
    same_day_exit_unavailable: tuple[str, ...] = ()
    mode_decision_hash: str = ""
    funnel_hash: str = ""
    config_hash: str = ""
    input_hash: str = ""
    reasons: tuple[str, ...] = ()
    blockers: tuple[AllocationBlocker, ...] = ()
    execution_ready: bool = False

    def __post_init__(self) -> None:
        try:
            mode = self.mode if isinstance(self.mode, TerminalMode) else TerminalMode(self.mode)
        except ValueError as exc:
            raise TerminalModelError("invalid allocation mode") from exc
        object.__setattr__(self, "mode", mode)
        if not isinstance(self.as_of, datetime) or self.as_of.utcoffset() is None:
            raise TerminalModelError("allocation.as_of must be timezone-aware")
        if not isinstance(self.trade_date, date) or isinstance(self.trade_date, datetime):
            raise TerminalModelError("allocation.trade_date must be a date")
        if self.as_of.astimezone(SHANGHAI).date() != self.trade_date:
            raise TerminalModelError("allocation.as_of date must match trade_date")
        for name in ("core_symbols", "aggressive_symbols", "same_day_exit_unavailable"):
            value = getattr(self, name)
            if not isinstance(value, tuple) or not all(
                isinstance(item, str) and item.strip() for item in value
            ):
                raise TerminalModelError(f"allocation.{name} must be a tuple of symbols")
            object.__setattr__(self, name, tuple(item.strip() for item in value))
        if len(set(self.core_symbols)) != len(self.core_symbols):
            raise TerminalModelError("allocation.core_symbols must be unique")
        if len(set(self.aggressive_symbols)) != len(self.aggressive_symbols):
            raise TerminalModelError("allocation.aggressive_symbols must be unique")
        for symbol in self.core_symbols + self.aggressive_symbols:
            try:
                CanonicalSymbol.from_core(symbol)
            except (SymbolContractError, TypeError, ValueError) as exc:
                raise TerminalModelError("allocation symbols must be canonical") from exc
        if set(self.core_symbols) & set(self.aggressive_symbols):
            raise TerminalModelError("allocation core and aggressive symbols cannot overlap")
        if type(self.total_position_count) is not int or self.total_position_count < 0:
            raise TerminalModelError("allocation.total_position_count must be non-negative")
        if type(self.terminal_candidate_count) is not int or self.terminal_candidate_count < 0:
            raise TerminalModelError("allocation.terminal_candidate_count must be non-negative")
        if self.total_position_count != len(self.core_symbols) + len(self.aggressive_symbols):
            raise TerminalModelError("allocation position count does not match symbol sets")
        if self.terminal_candidate_count != len(self.aggressive_symbols):
            raise TerminalModelError("allocation terminal count does not match aggressive symbols")
        if type(self.min_positions) is not int or type(self.max_positions) is not int:
            raise TerminalModelError("allocation position bounds must be integers")
        if (self.min_positions, self.max_positions) != (_MIN_POSITIONS, _MAX_POSITIONS):
            raise TerminalModelError("allocation position bounds must remain 5 to 8")
        if not _MIN_POSITIONS <= self.total_position_count <= _MAX_POSITIONS:
            raise TerminalModelError("allocation total positions must remain 5 to 8")
        if mode is TerminalMode.UNAVAILABLE:
            raise TerminalModelError("unavailable mode cannot produce an allocation policy")
        if mode is TerminalMode.PROTECT and self.terminal_candidate_count != 0:
            raise TerminalModelError("Protect cannot add terminal aggressive candidates")
        if mode is not TerminalMode.PROTECT and not (
            _MIN_TERMINAL_CANDIDATES <= self.terminal_candidate_count <= _MAX_TERMINAL_CANDIDATES
        ):
            raise TerminalModelError("non-Protect modes require two to three terminal candidates")
        for name in ("core_budget", "aggressive_budget", "cash_target", "max_single_weight"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or not _ZERO <= value <= _ONE:
                raise TerminalModelError(f"allocation.{name} must be a rate")
        if self.max_single_weight > _SINGLE_NAME_CAP:
            raise TerminalModelError("allocation max_single_weight cannot exceed 20%")
        if self.core_budget + self.aggressive_budget + self.cash_target != _ONE:
            raise TerminalModelError("allocation budgets must sum to 100%")
        if mode is TerminalMode.PROTECT:
            safe_budget = self.aggressive_budget <= Decimal("0.10") and self.cash_target >= Decimal("0.20")
        elif mode is TerminalMode.NORMAL:
            safe_budget = (
                Decimal("0.20") <= self.aggressive_budget <= Decimal("0.40")
                and Decimal("0.15") <= self.cash_target <= Decimal("0.20")
            )
        elif mode is TerminalMode.ATTACK:
            safe_budget = (
                Decimal("0.40") <= self.aggressive_budget <= Decimal("0.60")
                and Decimal("0.10") <= self.cash_target <= Decimal("0.15")
            )
        else:  # LAST_SHOT
            safe_budget = self.aggressive_budget <= Decimal("0.60") and self.cash_target >= Decimal("0.10")
        if not safe_budget:
            raise TerminalModelError("allocation mode budget is outside its safety range")
        if not isinstance(self.same_day_exit_unavailable, tuple) or not all(
            isinstance(item, str) for item in self.same_day_exit_unavailable
        ):
            raise TerminalModelError("same-day exit markers must be typed")
        if not set(self.same_day_exit_unavailable).issubset(set(self.aggressive_symbols)):
            raise TerminalModelError("same-day exit markers must belong to aggressive symbols")
        if not isinstance(self.reasons, tuple) or not all(
            isinstance(item, str) and item for item in self.reasons
        ):
            raise TerminalModelError("allocation reasons must be non-empty strings")
        if not isinstance(self.blockers, tuple) or not all(
            isinstance(item, AllocationBlocker) for item in self.blockers
        ):
            raise TerminalModelError("allocation blockers must be typed")
        if type(self.execution_ready) is not bool or self.execution_ready:
            raise TerminalModelError("terminal allocation policy is always research-only")
        for name in ("mode_decision_hash", "funnel_hash", "config_hash", "input_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise TerminalModelError(f"allocation.{name} must be a non-empty hash")

    @property
    def policy_hash(self) -> str:
        return content_hash(self.to_dict(include_hash=False))

    @property
    def terminal_role(self) -> str:
        return "aggressive_sleeve_only"

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": "competition.terminal-allocation-policy.v1",
            "mode": self.mode.value,
            "as_of": self.as_of.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "core_symbols": list(self.core_symbols),
            "aggressive_symbols": list(self.aggressive_symbols),
            "terminal_role": self.terminal_role,
            "total_position_count": self.total_position_count,
            "terminal_candidate_count": self.terminal_candidate_count,
            "position_bounds": {
                "min": self.min_positions,
                "max": self.max_positions,
            },
            "core_budget": str(self.core_budget),
            "aggressive_budget": str(self.aggressive_budget),
            "cash_target": str(self.cash_target),
            "max_single_weight": str(self.max_single_weight),
            "same_day_exit_unavailable": list(self.same_day_exit_unavailable),
            "mode_decision_hash": self.mode_decision_hash,
            "funnel_hash": self.funnel_hash,
            "config_hash": self.config_hash,
            "input_hash": self.input_hash,
            "reasons": list(self.reasons),
            "blockers": [item.value for item in self.blockers],
            "execution_ready": False,
        }
        if include_hash:
            result["policy_hash"] = self.policy_hash
        return result


@dataclass(frozen=True, slots=True)
class TerminalAllocationEvaluation:
    """Fail-closed evaluation envelope for callers that do not want exceptions."""

    policy: TerminalAllocationPolicy | None
    blockers: tuple[AllocationBlocker, ...]
    reasons: tuple[str, ...]
    input_hash: str | None
    execution_ready: bool = False

    @property
    def available(self) -> bool:
        return self.policy is not None and not self.blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.terminal-allocation-evaluation.v1",
            "available": self.available,
            "policy": self.policy.to_dict() if self.policy is not None else None,
            "blockers": [item.value for item in self.blockers],
            "reasons": list(self.reasons),
            "input_hash": self.input_hash,
            "execution_ready": False,
        }


def build_terminal_allocation_policy(
    inputs: TerminalAllocationInputs,
    config: TerminalStrategyConfig,
) -> TerminalAllocationPolicy:
    """Build a deterministic policy or raise a fail-closed error.

    No low-quality candidate is used to fill a count shortfall.  The caller
    must provide the existing core symbol set; this function preserves it and
    only selects from the funnel's already-passed terminal tier.
    """

    if not isinstance(inputs, TerminalAllocationInputs):
        raise TerminalAllocationError(
            "inputs must be TerminalAllocationInputs",
            blockers=(AllocationBlocker.INVALID_INPUT,),
            reasons=("INPUT_TYPE_INVALID",),
        )
    if not isinstance(config, TerminalStrategyConfig):
        raise TerminalAllocationError(
            "config must be TerminalStrategyConfig",
            blockers=(AllocationBlocker.INVALID_INPUT,),
            reasons=("CONFIG_TYPE_INVALID",),
            input_hash=inputs.input_hash,
        )

    blockers, reasons, selected = _validate_and_select(inputs, config)
    if blockers:
        raise TerminalAllocationError(
            "terminal allocation policy refused",
            blockers=blockers,
            reasons=reasons,
            input_hash=inputs.input_hash,
        )

    decision = inputs.mode_decision
    aggressive_symbols, aggressive_budget, cash_target = selected
    core_symbols = tuple(sorted(inputs.core_symbols))
    core_budget = _ONE - aggressive_budget - cash_target
    same_day = tuple(sorted(set(inputs.same_day_exit_unavailable_symbols) & set(aggressive_symbols)))
    for assessment in inputs.funnel.terminal:
        if assessment.symbol in aggressive_symbols and "SAME_DAY_EXIT_UNAVAILABLE" in assessment.risk_tags:
            same_day = tuple(sorted(set(same_day) | {assessment.symbol}))
    policy_reasons = tuple(reasons)
    if decision.mode is TerminalMode.PROTECT:
        policy_reasons = policy_reasons + ("PROTECT_PRESERVES_CORE_WITHOUT_NEW_RISK",)
    if same_day:
        policy_reasons = policy_reasons + ("SAME_DAY_EXIT_UNAVAILABLE_MARKED",)
    policy = TerminalAllocationPolicy(
        mode=decision.mode,
        as_of=decision.as_of,
        trade_date=decision.trade_date,
        core_symbols=core_symbols,
        aggressive_symbols=aggressive_symbols,
        total_position_count=len(core_symbols) + len(aggressive_symbols),
        terminal_candidate_count=len(aggressive_symbols),
        core_budget=core_budget,
        aggressive_budget=aggressive_budget,
        cash_target=cash_target,
        max_single_weight=_SINGLE_NAME_CAP,
        same_day_exit_unavailable=same_day,
        mode_decision_hash=decision.decision_hash,
        funnel_hash=inputs.funnel.funnel_hash,
        config_hash=config.config_hash,
        input_hash=inputs.input_hash,
        reasons=policy_reasons,
        blockers=(),
        execution_ready=False,
    )
    return policy


def evaluate_terminal_allocation(
    inputs: TerminalAllocationInputs,
    config: TerminalStrategyConfig,
) -> TerminalAllocationEvaluation:
    """Return a typed fail-closed envelope instead of raising on refusal."""

    try:
        policy = build_terminal_allocation_policy(inputs, config)
    except TerminalAllocationError as exc:
        return TerminalAllocationEvaluation(
            policy=None,
            blockers=exc.blockers,
            reasons=exc.reasons,
            input_hash=exc.input_hash,
            execution_ready=False,
        )
    return TerminalAllocationEvaluation(
        policy=policy,
        blockers=(),
        reasons=policy.reasons,
        input_hash=policy.input_hash,
        execution_ready=False,
    )


create_terminal_allocation_policy = build_terminal_allocation_policy
plan_terminal_allocation = build_terminal_allocation_policy


def _validate_and_select(
    inputs: TerminalAllocationInputs,
    config: TerminalStrategyConfig,
) -> tuple[tuple[AllocationBlocker, ...], tuple[str, ...], tuple[tuple[str, ...], Decimal, Decimal]]:
    decision = inputs.mode_decision
    funnel = inputs.funnel
    blockers: list[AllocationBlocker] = []
    reasons: list[str] = []

    if decision.mode is TerminalMode.UNAVAILABLE:
        blockers.append(AllocationBlocker.MODE_UNAVAILABLE)
        reasons.append("MODE_UNAVAILABLE")
    if decision.blockers:
        blockers.append(AllocationBlocker.MODE_DECISION_BLOCKED)
        reasons.append("MODE_DECISION_HAS_BLOCKERS")
    if funnel.mode is TerminalMode.UNAVAILABLE or funnel.status == "UNAVAILABLE":
        blockers.append(AllocationBlocker.FUNNEL_UNAVAILABLE)
        reasons.append("FUNNEL_UNAVAILABLE")
    if decision.mode is not funnel.mode:
        blockers.append(AllocationBlocker.MODE_MISMATCH)
        reasons.append("MODE_AND_FUNNEL_MODE_MISMATCH")
    if funnel.config_hash != config.config_hash:
        blockers.append(AllocationBlocker.FUNNEL_CONFIG_MISMATCH)
        reasons.append("FUNNEL_CONFIG_HASH_MISMATCH")
    if decision.trade_date != funnel.trade_date or decision.as_of != funnel.as_of:
        blockers.append(AllocationBlocker.FUNNEL_DATE_MISMATCH)
        reasons.append("MODE_AND_FUNNEL_SNAPSHOT_MISMATCH")
    if funnel.status != "SUCCESS":
        blockers.append(AllocationBlocker.FUNNEL_NOT_READY)
        reasons.append("FUNNEL_STATUS_NOT_SUCCESS")
    if decision.mode is TerminalMode.LAST_SHOT and (
        not decision.is_final_trade_date or decision.trade_date != config.final_trade_date
    ):
        blockers.append(AllocationBlocker.FINAL_DAY_REQUIRED)
        reasons.append("LAST_SHOT_FINAL_DAY_ONLY")

    canonical_core: list[str] = []
    for symbol in inputs.core_symbols:
        try:
            canonical = str(CanonicalSymbol.from_core(symbol))
        except (SymbolContractError, TypeError, ValueError):
            blockers.append(AllocationBlocker.CORE_SYMBOL_INVALID)
            reasons.append("CORE_SYMBOL_NOT_CANONICAL")
            continue
        canonical_core.append(canonical)
    if len(set(canonical_core)) != len(canonical_core):
        blockers.append(AllocationBlocker.CORE_HOLDINGS_DUPLICATE)
        reasons.append("CORE_SYMBOLS_DUPLICATE")

    terminal_assessments = tuple(funnel.terminal)
    if CandidateBlocker.DUPLICATE_SYMBOL in funnel.blockers or any(
        CandidateBlocker.DUPLICATE_SYMBOL in item.blockers for item in funnel.rejected
    ):
        blockers.append(AllocationBlocker.TERMINAL_CANDIDATE_DUPLICATE)
        reasons.append("FUNNEL_CONTAINS_DUPLICATE_SYMBOL")
    if len(terminal_assessments) < _MIN_TERMINAL_CANDIDATES:
        blockers.append(AllocationBlocker.TERMINAL_CANDIDATE_SHORTFALL)
        reasons.append("TERMINAL_CANDIDATES_BELOW_MINIMUM")
    if len(terminal_assessments) > _MAX_TERMINAL_CANDIDATES:
        blockers.append(AllocationBlocker.AGGRESSIVE_COUNT_INVALID)
        reasons.append("TERMINAL_CANDIDATES_EXCEED_THREE")

    symbols: list[str] = []
    for assessment in terminal_assessments:
        if assessment.symbol in symbols:
            blockers.append(AllocationBlocker.TERMINAL_CANDIDATE_DUPLICATE)
            reasons.append("TERMINAL_SYMBOLS_DUPLICATE")
        symbols.append(assessment.symbol)
        if not assessment.hard_gate_pass or not assessment.terminal_eligible or assessment.blockers:
            blockers.append(AllocationBlocker.TERMINAL_CANDIDATE_BLOCKED)
            reasons.append(f"TERMINAL_CANDIDATE_BLOCKED:{assessment.symbol}")
        if assessment.max_weight is None or assessment.max_weight <= _ZERO or assessment.max_weight > _SINGLE_NAME_CAP:
            blockers.append(AllocationBlocker.SINGLE_NAME_CAP_EXCEEDED)
            reasons.append(f"TERMINAL_WEIGHT_INVALID:{assessment.symbol}")
        if (
            decision.mode in (TerminalMode.NORMAL, TerminalMode.ATTACK)
            and (
                "SAME_DAY_EXIT_UNAVAILABLE" in assessment.risk_tags
                or assessment.symbol in inputs.same_day_exit_unavailable_symbols
            )
        ):
            blockers.append(AllocationBlocker.SAME_DAY_EXIT_UNAVAILABLE)
            reasons.append(f"SAME_DAY_EXIT_FORBIDDEN:{assessment.symbol}")
        if (
            decision.mode is TerminalMode.LAST_SHOT
            and "NEW_LISTING" in assessment.risk_tags
            and assessment.max_weight is not None
            and assessment.max_weight > config.allocation.new_listing_final_day_cap
        ):
            blockers.append(AllocationBlocker.SINGLE_NAME_CAP_EXCEEDED)
            reasons.append(f"NEW_LISTING_FINAL_DAY_CAP_EXCEEDED:{assessment.symbol}")
    if set(canonical_core) & set(symbols):
        blockers.append(AllocationBlocker.CORE_AGGRESSIVE_OVERLAP)
        reasons.append("CORE_AND_AGGRESSIVE_SYMBOL_OVERLAP")

    _append_mode_budget_blockers(config, decision.mode, blockers, reasons)
    aggressive_budget, cash_target = _budget_for_mode(config, decision.mode)
    if aggressive_budget + cash_target > _ONE:
        blockers.append(AllocationBlocker.BUDGET_EXCEEDED)
        reasons.append("AGGRESSIVE_AND_CASH_BUDGET_EXCEED_100_PERCENT")

    if decision.mode is TerminalMode.PROTECT:
        # Protect does not add a newly selected high-volatility/new-listing
        # name.  The funnel is still required and auditable, but its terminal
        # tier is deliberately not converted into new risk.
        aggressive_symbols: tuple[str, ...] = ()
        aggressive_budget = _ZERO
        cash_target = config.allocation.protect_cash_target
        reasons.append("PROTECT_AGGRESSIVE_SLEEVE_EMPTY")
    else:
        required = max(
            _MIN_TERMINAL_CANDIDATES,
            _ceil_div_decimal(aggressive_budget, _SINGLE_NAME_CAP),
        )
        if required > _MAX_TERMINAL_CANDIDATES or len(terminal_assessments) < required:
            blockers.append(AllocationBlocker.TERMINAL_CANDIDATE_SHORTFALL)
            reasons.append("TERMINAL_CANDIDATES_CANNOT_COVER_MODE_BUDGET")
            required = min(_MAX_TERMINAL_CANDIDATES, len(terminal_assessments))
        sorted_assessments = tuple(sorted(terminal_assessments, key=_assessment_sort_key))
        aggressive_symbols = tuple(item.symbol for item in sorted_assessments[:required])
        if decision.mode is TerminalMode.LAST_SHOT:
            unavailable = set(inputs.same_day_exit_unavailable_symbols)
            unavailable.update(
                item.symbol
                for item in sorted_assessments
                if "SAME_DAY_EXIT_UNAVAILABLE" in item.risk_tags
            )
            if unavailable - set(aggressive_symbols):
                reasons.append("LAST_SHOT_SAME_DAY_EXIT_FACTS_RETAINED")

    position_count = len(canonical_core) + len(aggressive_symbols)
    if not _MIN_POSITIONS <= position_count <= _MAX_POSITIONS:
        blockers.append(AllocationBlocker.POSITION_COUNT_INVALID)
        reasons.append(f"POSITION_COUNT_OUTSIDE_{_MIN_POSITIONS}_{_MAX_POSITIONS}")
    if not 0 <= len(aggressive_symbols) <= _MAX_TERMINAL_CANDIDATES:
        blockers.append(AllocationBlocker.AGGRESSIVE_COUNT_INVALID)
        reasons.append("AGGRESSIVE_SYMBOL_COUNT_OUTSIDE_0_3")
    if aggressive_budget > Decimal(len(aggressive_symbols)) * _SINGLE_NAME_CAP:
        blockers.append(AllocationBlocker.SINGLE_NAME_CAP_EXCEEDED)
        reasons.append("AGGRESSIVE_BUDGET_EXCEEDS_TERMINAL_SINGLE_NAME_CAPS")
    if decision.mode is TerminalMode.PROTECT and aggressive_symbols:
        blockers.append(AllocationBlocker.PROTECT_RISK_INCREASE)
        reasons.append("PROTECT_WOULD_ADD_AGGRESSIVE_RISK")

    # De-duplicate while preserving a stable enum/reason order for audit logs.
    unique_blockers = tuple(dict.fromkeys(blockers))
    unique_reasons = tuple(dict.fromkeys(reasons))
    return unique_blockers, unique_reasons, (aggressive_symbols, aggressive_budget, cash_target)


def _append_mode_budget_blockers(
    config: TerminalStrategyConfig,
    mode: TerminalMode,
    blockers: list[AllocationBlocker],
    reasons: list[str],
) -> None:
    allocation = config.allocation
    bounds: tuple[Decimal, Decimal, Decimal, Decimal]
    if mode is TerminalMode.PROTECT:
        bounds = (_ZERO, Decimal("0.10"), Decimal("0.20"), _ONE)
        actual = (allocation.protect_aggressive_cap, allocation.protect_cash_target)
        configured_aggressive = allocation.protect_aggressive_cap
    elif mode is TerminalMode.NORMAL:
        bounds = (Decimal("0.20"), Decimal("0.40"), Decimal("0.15"), Decimal("0.20"))
        actual = (allocation.normal_aggressive_target, allocation.normal_cash_target)
        configured_aggressive = allocation.normal_aggressive_target
    elif mode is TerminalMode.ATTACK:
        bounds = (Decimal("0.40"), Decimal("0.60"), Decimal("0.10"), Decimal("0.15"))
        actual = (allocation.attack_aggressive_min, allocation.attack_cash_target)
        configured_aggressive = allocation.attack_aggressive_max
    elif mode is TerminalMode.LAST_SHOT:
        bounds = (_ZERO, Decimal("0.60"), Decimal("0.10"), _ONE)
        actual = (allocation.last_shot_aggressive_cap, allocation.last_shot_cash_target)
        configured_aggressive = allocation.last_shot_aggressive_cap
    else:
        return
    if not bounds[0] <= actual[0] <= bounds[1] or not bounds[2] <= actual[1] <= bounds[3]:
        blockers.append(AllocationBlocker.MODE_BUDGET_OUT_OF_RANGE)
        reasons.append(f"{mode.value}_BUDGET_OUT_OF_SAFE_RANGE")
    if configured_aggressive + actual[1] > _ONE:
        blockers.append(AllocationBlocker.BUDGET_EXCEEDED)
        reasons.append(f"{mode.value}_CONFIGURED_BUDGET_EXCEEDS_100_PERCENT")


def _budget_for_mode(config: TerminalStrategyConfig, mode: TerminalMode) -> tuple[Decimal, Decimal]:
    allocation = config.allocation
    if mode is TerminalMode.PROTECT:
        return _ZERO, allocation.protect_cash_target
    if mode is TerminalMode.NORMAL:
        return allocation.normal_aggressive_target, allocation.normal_cash_target
    if mode is TerminalMode.ATTACK:
        return allocation.attack_aggressive_min, allocation.attack_cash_target
    if mode is TerminalMode.LAST_SHOT:
        return allocation.last_shot_aggressive_cap, allocation.last_shot_cash_target
    return _ZERO, _ONE


def _ceil_div_decimal(value: Decimal, divisor: Decimal) -> int:
    if value <= _ZERO:
        return 0
    quotient = value / divisor
    integer = int(quotient)
    return integer if quotient == integer else integer + 1


def _assessment_sort_key(item: CandidateAssessment) -> tuple[Decimal, str, str]:
    return (
        -(item.score if item.score is not None else _ZERO),
        item.symbol,
        item.observation_hash,
    )


__all__ = [
    "AllocationBlocker",
    "TerminalAllocationError",
    "TerminalAllocationEvaluation",
    "TerminalAllocationInputs",
    "TerminalAllocationPolicy",
    "build_terminal_allocation_policy",
    "create_terminal_allocation_policy",
    "evaluate_terminal_allocation",
    "plan_terminal_allocation",
]

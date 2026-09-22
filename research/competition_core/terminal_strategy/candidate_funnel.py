"""Deterministic, read-only terminal candidate funnel.

The funnel is intentionally a shadow component.  It accepts already captured
observations from S4/S5/S5.5 adapters, applies permanent hard gates, scores
eligible observations deterministically, and returns an auditable 10/5/2--3
selection.  It never fetches data, creates broker quantities, or writes orders.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash

from .config import TerminalStrategyConfig
from .models import TerminalMode, TerminalModelError


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_CENT = Decimal("0.01")
_HUNDRED = Decimal("100")
_MAX_SINGLE_WEIGHT = Decimal("0.20")
_RELATIVE_FIELDS = ("relative_strength_1d", "relative_strength_3d", "relative_strength_5d")
_RISK_FIELDS = ("announcement_risk", "regulatory_risk", "reduction_risk", "earnings_risk")


class CandidateFunnelError(TerminalModelError):
    """Raised for invalid funnel arguments, never for an individual blocker."""


class CandidateBlocker(StrEnum):
    MODE_UNAVAILABLE = "MODE_UNAVAILABLE"
    CANONICAL_SYMBOL_INVALID = "CANONICAL_SYMBOL_INVALID"
    DUPLICATE_SYMBOL = "DUPLICATE_SYMBOL"
    OBSERVATION_FUTURE = "OBSERVATION_FUTURE"
    OBSERVATION_DATE_MISMATCH = "OBSERVATION_DATE_MISMATCH"
    QUOTE_MISSING = "QUOTE_MISSING"
    QUOTE_STALE = "QUOTE_STALE"
    QUOTE_FUTURE = "QUOTE_FUTURE"
    QUOTE_DATE_MISMATCH = "QUOTE_DATE_MISMATCH"
    QUOTE_AGE_MISMATCH = "QUOTE_AGE_MISMATCH"
    SUSPENDED = "SUSPENDED"
    NOT_TRADEABLE = "NOT_TRADEABLE"
    PRICE_LOCKED = "PRICE_LOCKED"
    EXECUTABLE_QUOTE_MISSING = "EXECUTABLE_QUOTE_MISSING"
    BID_ASK_INVALID = "BID_ASK_INVALID"
    LIQUIDITY_MISSING = "LIQUIDITY_MISSING"
    LIQUIDITY_BELOW_MINIMUM = "LIQUIDITY_BELOW_MINIMUM"
    RELATIVE_STRENGTH_MISSING = "RELATIVE_STRENGTH_MISSING"
    TREND_CONTINUITY_MISSING = "TREND_CONTINUITY_MISSING"
    CLOSE_STRENGTH_MISSING = "CLOSE_STRENGTH_MISSING"
    INTRADAY_DRAWDOWN_MISSING = "INTRADAY_DRAWDOWN_MISSING"
    INTRADAY_DRAWDOWN_TOO_LARGE = "INTRADAY_DRAWDOWN_TOO_LARGE"
    OPENING_GAP_MISSING = "OPENING_GAP_MISSING"
    OPENING_GAP_TOO_LARGE = "OPENING_GAP_TOO_LARGE"
    OVERHEAT_MISSING = "OVERHEAT_MISSING"
    OVERHEATED_SOFT_GATE = "OVERHEATED_SOFT_GATE"
    EVIDENCE_MISSING = "EVIDENCE_MISSING"
    EVIDENCE_BELOW_MINIMUM = "EVIDENCE_BELOW_MINIMUM"
    RISK_FACTS_MISSING = "RISK_FACTS_MISSING"
    CORRELATION_MISSING = "CORRELATION_MISSING"
    CORRELATION_TOO_HIGH = "CORRELATION_TOO_HIGH"
    EXECUTION_HISTORY_MISSING = "EXECUTION_HISTORY_MISSING"
    EXECUTION_PROBABILITY_LOW = "EXECUTION_PROBABILITY_LOW"
    RULES_MISSING = "RULES_MISSING"
    LISTING_AGE_MISSING = "LISTING_AGE_MISSING"
    ACCOUNT_STATE_MISSING = "ACCOUNT_STATE_MISSING"
    ORDER_RECONCILIATION_MISSING = "ORDER_RECONCILIATION_MISSING"
    SAME_DAY_EXIT_UNAVAILABLE = "SAME_DAY_EXIT_UNAVAILABLE"
    TERMINAL_RULES_MISSING = "TERMINAL_RULES_MISSING"
    TERMINAL_WEIGHT_EXCEEDS_CAP = "TERMINAL_WEIGHT_EXCEEDS_CAP"
    TERMINAL_INVALIDATION_INVALID = "TERMINAL_INVALIDATION_INVALID"
    SCORE_COMPONENT_MISSING = "SCORE_COMPONENT_MISSING"


@dataclass(frozen=True, slots=True)
class CandidateObservation:
    """One fully explicit, provider-neutral candidate observation.

    ``None`` means the source did not establish a fact.  The funnel never
    converts it to a favorable default; critical missing values become hard
    blockers and noncritical scoring gaps become zero contribution with an
    audit reason.
    """

    symbol: str
    as_of: datetime
    trade_date: date

    relative_strength_1d: Decimal | None = None
    relative_strength_3d: Decimal | None = None
    relative_strength_5d: Decimal | None = None
    benchmark_relative_1d: Decimal | None = None
    benchmark_relative_3d: Decimal | None = None
    benchmark_relative_5d: Decimal | None = None
    amount_cny: Decimal | None = None
    turnover_rate: Decimal | None = None
    volume_ratio: Decimal | None = None
    liquidity_score: Decimal | None = None
    opening_gap: Decimal | None = None
    close_strength: Decimal | None = None
    intraday_max_drawdown: Decimal | None = None
    trend_continuity: Decimal | None = None
    consecutive_strong_sessions: int | None = None
    single_day_pulse: bool | None = None

    industry: str | None = None
    correlation_cluster: str | None = None
    correlation_to_holdings: Mapping[str, Decimal] | tuple[tuple[str, Decimal], ...] | None = None

    announcement_risk: bool | None = None
    regulatory_risk: bool | None = None
    reduction_risk: bool | None = None
    earnings_risk: bool | None = None
    risk_facts_complete: bool | None = None
    overheat_score: Decimal | None = None

    evidence_complete: bool | None = None
    evidence_quality: Decimal | None = None
    evidence_missing: tuple[str, ...] = ()
    evidence_sources: tuple[str, ...] = ()

    quote_as_of: datetime | None = None
    quote_trade_date: date | None = None
    quote_age_seconds: Decimal | None = None
    is_suspended: bool | None = None
    is_tradeable: bool | None = None
    price_locked: bool | None = None
    executable_quote: bool | None = None
    has_valid_bid_ask: bool | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    current_price: Decimal | None = None
    previous_close: Decimal | None = None

    simulated_fill_rate: Decimal | None = None
    simulated_slippage_p95: Decimal | None = None
    simulated_failure_reasons: tuple[str, ...] = ()

    board: str | None = None
    listed_sessions: int | None = None
    is_new_listing: bool | None = None
    price_limit_pct: Decimal | None = None
    price_limit_rule_known: bool | None = None
    new_listing_rule_known: bool | None = None

    holding_quantity: int | None = None
    sellable_quantity: int | None = None
    same_day_exit_unavailable: bool | None = None
    orders_reconciled: bool | None = None

    trigger_price: Decimal | None = None
    invalidation_price: Decimal | None = None
    limit_price: Decimal | None = None
    max_weight: Decimal | None = None
    source_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise CandidateFunnelError("candidate symbol must be a non-empty string")
        if not isinstance(self.as_of, datetime) or self.as_of.utcoffset() is None:
            raise CandidateFunnelError("candidate as_of must be timezone-aware")
        if not isinstance(self.trade_date, date) or isinstance(self.trade_date, datetime):
            raise CandidateFunnelError("candidate trade_date must be a date")
        if self.as_of.astimezone(SHANGHAI).date() != self.trade_date:
            raise CandidateFunnelError("candidate as_of date must match trade_date")
        for name in _DECIMAL_FIELDS:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _finite_decimal(value, name))
        for name in _RATIO_FIELDS:
            value = getattr(self, name)
            if value is not None and not _ZERO <= value <= _ONE:
                raise CandidateFunnelError(f"{name} must be between 0 and 1")
        for name in ("liquidity_score", "overheat_score"):
            value = getattr(self, name)
            if value is not None and not _ZERO <= value <= _HUNDRED:
                raise CandidateFunnelError(f"{name} must be between 0 and 100")
        for name in _BOOL_FIELDS:
            value = getattr(self, name)
            if value is not None and type(value) is not bool:
                raise CandidateFunnelError(f"{name} must be boolean or null")
        for name in _TEXT_FIELDS:
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise CandidateFunnelError(f"{name} must be a non-empty string or null")
                object.__setattr__(self, name, value.strip())
        for name in ("evidence_missing", "evidence_sources", "simulated_failure_reasons"):
            value = getattr(self, name)
            if not isinstance(value, tuple) or any(not isinstance(item, str) or not item.strip() for item in value):
                raise CandidateFunnelError(f"{name} must be a tuple of non-empty strings")
            object.__setattr__(self, name, tuple(item.strip() for item in value))
        if self.correlation_to_holdings is not None:
            object.__setattr__(self, "correlation_to_holdings", _correlations(self.correlation_to_holdings))
        for name in ("listed_sessions", "holding_quantity", "sellable_quantity", "consecutive_strong_sessions"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise CandidateFunnelError(f"{name} must be a non-negative integer or null")
        if self.holding_quantity is not None and self.sellable_quantity is not None and self.sellable_quantity > self.holding_quantity:
            raise CandidateFunnelError("sellable_quantity cannot exceed holding_quantity")
        for name in ("amount_cny", "turnover_rate", "volume_ratio", "liquidity_score", "intraday_max_drawdown",
                     "overheat_score", "quote_age_seconds", "simulated_fill_rate", "simulated_slippage_p95"):
            value = getattr(self, name)
            if value is not None and value < _ZERO:
                raise CandidateFunnelError(f"{name} cannot be negative")
        if self.quote_trade_date is not None and (
            not isinstance(self.quote_trade_date, date) or isinstance(self.quote_trade_date, datetime)
        ):
            raise CandidateFunnelError("quote_trade_date must be a date or null")

    @property
    def observation_hash(self) -> str:
        return content_hash(self.to_dict())

    @property
    def holding_correlation(self) -> Decimal | None:
        if self.correlation_to_holdings is None:
            return None
        values = [value for _, value in self.correlation_to_holdings]
        return max(values, default=_ZERO)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for field in fields(self):
            value = getattr(self, field.name)
            result[field.name] = _json_value(value)
        result["schema_version"] = "competition.terminal-candidate-observation.v1"
        return {"schema_version": result.pop("schema_version"), **dict(sorted(result.items()))}


@dataclass(frozen=True, slots=True)
class CandidateAssessment:
    symbol: str
    mode: TerminalMode
    score: Decimal | None
    score_components: tuple[tuple[str, Decimal | None], ...]
    risk_penalties: tuple[tuple[str, Decimal], ...]
    blockers: tuple[CandidateBlocker, ...]
    warnings: tuple[str, ...]
    hard_gate_pass: bool
    focus_eligible: bool
    terminal_eligible: bool
    trigger_price: Decimal | None
    invalidation_price: Decimal | None
    limit_price: Decimal | None
    max_weight: Decimal | None
    observation_hash: str
    # A small, provider-neutral risk vocabulary retained for downstream
    # research components.  These are facts/labels only; they do not turn a
    # candidate into a broker instruction or position size.
    risk_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.mode, TerminalMode):
            object.__setattr__(self, "mode", TerminalMode(self.mode))
        if self.score is not None:
            object.__setattr__(self, "score", _finite_decimal(self.score, "score"))
        if not isinstance(self.score_components, tuple):
            raise CandidateFunnelError("score_components must be a tuple")
        if not isinstance(self.risk_penalties, tuple):
            raise CandidateFunnelError("risk_penalties must be a tuple")
        for name, value in self.score_components:
            if not isinstance(name, str) or (value is not None and _finite_decimal(value, name) < _ZERO):
                raise CandidateFunnelError("score component is invalid")
        for name, value in self.risk_penalties:
            if not isinstance(name, str) or _finite_decimal(value, name) < _ZERO:
                raise CandidateFunnelError("risk penalty is invalid")
        if not isinstance(self.blockers, tuple) or not all(isinstance(item, CandidateBlocker) for item in self.blockers):
            raise CandidateFunnelError("assessment blockers must be typed")
        if not isinstance(self.warnings, tuple) or not all(isinstance(item, str) and item for item in self.warnings):
            raise CandidateFunnelError("assessment warnings must be non-empty strings")
        if not isinstance(self.risk_tags, tuple) or not all(
            isinstance(item, str) and item for item in self.risk_tags
        ):
            raise CandidateFunnelError("assessment risk_tags must be non-empty strings")
        for name in ("trigger_price", "invalidation_price", "limit_price", "max_weight"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _finite_decimal(value, name))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "competition.terminal-candidate-assessment.v1",
            "symbol": self.symbol,
            "mode": self.mode.value,
            "score": _json_value(self.score),
            "score_components": {name: _json_value(value) for name, value in self.score_components},
            "risk_penalties": {name: _json_value(value) for name, value in self.risk_penalties},
            "blockers": [item.value for item in self.blockers],
            "warnings": list(self.warnings),
            "hard_gate_pass": self.hard_gate_pass,
            "focus_eligible": self.focus_eligible,
            "terminal_eligible": self.terminal_eligible,
            "trigger_price": _json_value(self.trigger_price),
            "invalidation_price": _json_value(self.invalidation_price),
            "limit_price": _json_value(self.limit_price),
            "max_weight": _json_value(self.max_weight),
            "observation_hash": self.observation_hash,
            "risk_tags": list(self.risk_tags),
        }


@dataclass(frozen=True, slots=True)
class FunnelResult:
    mode: TerminalMode
    as_of: datetime
    trade_date: date
    watchlist: tuple[CandidateAssessment, ...]
    focus: tuple[CandidateAssessment, ...]
    terminal: tuple[CandidateAssessment, ...]
    rejected: tuple[CandidateAssessment, ...]
    status: str
    reasons: tuple[str, ...]
    blockers: tuple[CandidateBlocker, ...]
    input_count: int
    config_hash: str
    input_hash: str
    execution_ready: bool = False

    @property
    def watch_symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.watchlist)

    @property
    def focus_symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.focus)

    @property
    def terminal_symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.terminal)

    @property
    def terminal_role(self) -> str:
        return "aggressive_sleeve_candidates_only"

    @property
    def funnel_hash(self) -> str:
        return content_hash(self.to_dict(include_hash=False))

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": "competition.terminal-candidate-funnel.v1",
            "mode": self.mode.value,
            "as_of": self.as_of.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "status": self.status,
            "watchlist": [item.to_dict() for item in self.watchlist],
            "focus": [item.to_dict() for item in self.focus],
            "terminal": [item.to_dict() for item in self.terminal],
            "rejected": [item.to_dict() for item in self.rejected],
            "reasons": list(self.reasons),
            "blockers": [item.value for item in self.blockers],
            "input_count": self.input_count,
            "config_hash": self.config_hash,
            "input_hash": self.input_hash,
            "execution_ready": self.execution_ready,
            "terminal_role": "aggressive_sleeve_candidates_only",
        }
        if include_hash:
            result["funnel_hash"] = self.funnel_hash
        return result


def build_candidate_funnel(
    observations: Iterable[CandidateObservation],
    config: TerminalStrategyConfig,
    mode: TerminalMode | str,
    *,
    observed_at: datetime | None = None,
) -> FunnelResult:
    """Build one deterministic shadow funnel from explicit observations."""

    if not isinstance(config, TerminalStrategyConfig):
        raise CandidateFunnelError("config must be a TerminalStrategyConfig")
    try:
        selected_mode = mode if isinstance(mode, TerminalMode) else TerminalMode(mode)
    except ValueError as exc:
        raise CandidateFunnelError("mode must be a TerminalMode") from exc
    now = observed_at or datetime.now(tz=SHANGHAI)
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise CandidateFunnelError("observed_at must be timezone-aware")
    items = tuple(observations)
    if not all(isinstance(item, CandidateObservation) for item in items):
        raise CandidateFunnelError("observations must contain CandidateObservation values")
    if items:
        trade_date = items[0].trade_date
        as_of = items[0].as_of
        if any(item.trade_date != trade_date or item.as_of != as_of for item in items):
            raise CandidateFunnelError("observations must share one trade date and as_of")
    else:
        trade_date = now.astimezone(SHANGHAI).date()
        as_of = now
    input_hash = content_hash({
        "mode": selected_mode.value,
        "config_hash": config.config_hash,
        "observations": sorted(
            (item.to_dict() for item in items),
            key=lambda value: (value["symbol"], content_hash(value)),
        ),
    })
    if selected_mode is TerminalMode.UNAVAILABLE:
        assessments = tuple(sorted(
            (
                _blocked_assessment(
                    item, selected_mode, (CandidateBlocker.MODE_UNAVAILABLE,),
                    overheat_threshold=config.funnel.overheat_score_threshold,
                )
                for item in items
            ),
            key=_assessment_sort_key,
        ))
        return _result(
            selected_mode, as_of, trade_date, (), (), (), assessments,
            ("MODE_UNAVAILABLE",), (CandidateBlocker.MODE_UNAVAILABLE,), config, input_hash,
            input_count=len(items),
        )

    assessments: list[CandidateAssessment] = []
    seen: set[str] = set()
    for item in items:
        duplicate = item.symbol in seen
        seen.add(item.symbol)
        blockers, warnings = _hard_gate(item, config, selected_mode, now)
        if duplicate:
            blockers.append(CandidateBlocker.DUPLICATE_SYMBOL)
        if blockers:
            assessments.append(
                _assessment(
                    item, selected_mode, None, {}, {}, blockers, warnings, False, False,
                    overheat_threshold=config.funnel.overheat_score_threshold,
                )
            )
            continue
        score, components, penalties, score_warnings = _score(item, items, config, selected_mode)
        warnings.extend(score_warnings)
        assessments.append(
            _assessment(
                item, selected_mode, score, components, penalties, (), warnings, True, False,
                overheat_threshold=config.funnel.overheat_score_threshold,
            )
        )

    ordered = sorted(assessments, key=_assessment_sort_key)
    eligible = [item for item in ordered if item.hard_gate_pass]
    watchlist = tuple(eligible[: config.funnel.watch_count])
    watch_symbols = {item.symbol for item in watchlist}
    focus: list[CandidateAssessment] = []
    for item in watchlist:
        observation = next(candidate for candidate in items if candidate.symbol == item.symbol)
        focus_ok = (
            observation.trend_continuity is not None
            and observation.trend_continuity >= Decimal("0.50")
            and observation.consecutive_strong_sessions is not None
            and observation.consecutive_strong_sessions >= config.funnel.continuity_sessions
            and observation.single_day_pulse is False
        )
        focus.append(replace(item, focus_eligible=focus_ok))
    focus = sorted(focus, key=_assessment_sort_key)[: config.funnel.focus_count]
    focus_symbols = {item.symbol for item in focus if item.focus_eligible}
    focus = [item for item in focus if item.focus_eligible]

    terminal: list[CandidateAssessment] = []
    for item in focus:
        observation = next(candidate for candidate in items if candidate.symbol == item.symbol)
        terminal_ok, terminal_blockers = _terminal_gate(observation, selected_mode)
        if terminal_ok:
            terminal.append(replace(item, terminal_eligible=True))
        else:
            for index, original in enumerate(assessments):
                if original.symbol == item.symbol and original.observation_hash == item.observation_hash:
                    assessments[index] = replace(original, blockers=tuple(dict.fromkeys(original.blockers + tuple(terminal_blockers))))
                    break
    terminal = sorted(terminal, key=_assessment_sort_key)[: config.funnel.terminal_count_max]
    terminal_symbols = {item.symbol for item in terminal}
    rejected = tuple(sorted(
        (item for item in assessments if not item.hard_gate_pass or item.symbol not in watch_symbols or item.symbol not in terminal_symbols),
        key=_assessment_sort_key,
    ))
    reasons: list[str] = []
    if len(watchlist) < config.funnel.watch_count:
        reasons.append("WATCHLIST_SHORTFALL")
    if len(focus) < config.funnel.focus_count:
        reasons.append("FOCUS_SHORTFALL")
    if len(terminal) < config.funnel.terminal_count_min:
        reasons.append("TERMINAL_CANDIDATE_SHORTFALL")
    blockers = tuple(sorted({blocker for item in assessments for blocker in item.blockers}, key=lambda value: value.value))
    status = "SUCCESS" if len(terminal) >= config.funnel.terminal_count_min else "PARTIAL"
    return _result(selected_mode, as_of, trade_date, watchlist, tuple(focus), tuple(terminal), rejected,
                   tuple(reasons), blockers, config, input_hash, input_count=len(items), status=status)


evaluate_candidate_funnel = build_candidate_funnel
build_terminal_candidate_funnel = build_candidate_funnel


def _hard_gate(
    item: CandidateObservation,
    config: TerminalStrategyConfig,
    mode: TerminalMode,
    observed_at: datetime,
) -> tuple[list[CandidateBlocker], list[str]]:
    blockers: list[CandidateBlocker] = []
    warnings: list[str] = []
    try:
        CanonicalSymbol.from_core(item.symbol)
    except (SymbolContractError, TypeError, ValueError):
        blockers.append(CandidateBlocker.CANONICAL_SYMBOL_INVALID)
    if item.as_of > observed_at:
        blockers.append(CandidateBlocker.OBSERVATION_FUTURE)
    if item.as_of.astimezone(SHANGHAI).date() != item.trade_date:
        blockers.append(CandidateBlocker.OBSERVATION_DATE_MISMATCH)

    if item.quote_as_of is None:
        blockers.append(CandidateBlocker.QUOTE_MISSING)
    elif item.quote_as_of.tzinfo is None or item.quote_as_of.utcoffset() is None:
        blockers.append(CandidateBlocker.QUOTE_MISSING)
    else:
        if item.quote_as_of > observed_at:
            blockers.append(CandidateBlocker.QUOTE_FUTURE)
        age = Decimal(str((observed_at - item.quote_as_of).total_seconds()))
        if age > Decimal(config.funnel.max_quote_age_seconds):
            blockers.append(CandidateBlocker.QUOTE_STALE)
        if item.quote_age_seconds is not None and abs(item.quote_age_seconds - max(age, _ZERO)) > Decimal("1"):
            blockers.append(CandidateBlocker.QUOTE_AGE_MISMATCH)
    if item.quote_trade_date != item.trade_date:
        blockers.append(CandidateBlocker.QUOTE_DATE_MISMATCH)
    if item.is_suspended is not False:
        blockers.append(CandidateBlocker.SUSPENDED)
    if item.is_tradeable is not True:
        blockers.append(CandidateBlocker.NOT_TRADEABLE)
    if item.price_locked is not False:
        blockers.append(CandidateBlocker.PRICE_LOCKED)
    if item.executable_quote is not True or item.has_valid_bid_ask is not True:
        blockers.append(CandidateBlocker.EXECUTABLE_QUOTE_MISSING)
    if any(value is None or value <= _ZERO for value in (item.bid_price, item.ask_price, item.current_price, item.previous_close)):
        blockers.append(CandidateBlocker.BID_ASK_INVALID)
    elif item.bid_price > item.ask_price:
        blockers.append(CandidateBlocker.BID_ASK_INVALID)

    if any(value is None for value in (item.amount_cny, item.turnover_rate, item.volume_ratio, item.liquidity_score)):
        blockers.append(CandidateBlocker.LIQUIDITY_MISSING)
    elif (
        item.amount_cny <= _ZERO or item.turnover_rate < _ZERO or item.volume_ratio <= _ZERO
        or item.liquidity_score < config.funnel.minimum_liquidity_score
    ):
        blockers.append(CandidateBlocker.LIQUIDITY_BELOW_MINIMUM)

    if any(getattr(item, name) is None for name in _RELATIVE_FIELDS):
        blockers.append(CandidateBlocker.RELATIVE_STRENGTH_MISSING)
    if item.trend_continuity is None or item.consecutive_strong_sessions is None or item.single_day_pulse is None:
        blockers.append(CandidateBlocker.TREND_CONTINUITY_MISSING)
    if item.close_strength is None:
        blockers.append(CandidateBlocker.CLOSE_STRENGTH_MISSING)
    if item.intraday_max_drawdown is None:
        blockers.append(CandidateBlocker.INTRADAY_DRAWDOWN_MISSING)
    elif item.intraday_max_drawdown > config.funnel.maximum_intraday_drawdown:
        blockers.append(CandidateBlocker.INTRADAY_DRAWDOWN_TOO_LARGE)
    if item.opening_gap is None:
        blockers.append(CandidateBlocker.OPENING_GAP_MISSING)
    elif abs(item.opening_gap) > config.funnel.maximum_opening_gap:
        blockers.append(CandidateBlocker.OPENING_GAP_TOO_LARGE)
    if item.overheat_score is None:
        blockers.append(CandidateBlocker.OVERHEAT_MISSING)
    elif item.overheat_score >= config.funnel.overheat_score_threshold:
        if mode is TerminalMode.LAST_SHOT:
            warnings.append("OVERHEAT_SOFT_GATE_RELAXED_LAST_SHOT")
        else:
            blockers.append(CandidateBlocker.OVERHEATED_SOFT_GATE)

    if item.evidence_complete is not True or not item.evidence_sources or item.evidence_missing:
        blockers.append(CandidateBlocker.EVIDENCE_MISSING)
    if item.evidence_quality is None:
        blockers.append(CandidateBlocker.EVIDENCE_MISSING)
    elif item.evidence_quality < config.funnel.minimum_evidence_quality:
        blockers.append(CandidateBlocker.EVIDENCE_BELOW_MINIMUM)
    if item.risk_facts_complete is not True or any(getattr(item, name) is None for name in _RISK_FIELDS):
        blockers.append(CandidateBlocker.RISK_FACTS_MISSING)

    if item.correlation_to_holdings is None:
        blockers.append(CandidateBlocker.CORRELATION_MISSING)
    else:
        if any(value > config.funnel.maximum_correlation for _, value in item.correlation_to_holdings):
            blockers.append(CandidateBlocker.CORRELATION_TOO_HIGH)

    if item.simulated_fill_rate is None or item.simulated_slippage_p95 is None:
        blockers.append(CandidateBlocker.EXECUTION_HISTORY_MISSING)
    elif item.simulated_fill_rate < config.funnel.minimum_execution_probability:
        blockers.append(CandidateBlocker.EXECUTION_PROBABILITY_LOW)
    if item.board is None or item.listed_sessions is None or item.is_new_listing is None:
        blockers.append(CandidateBlocker.LISTING_AGE_MISSING)
    if item.price_limit_pct is None or item.price_limit_pct <= _ZERO or item.price_limit_rule_known is not True or item.new_listing_rule_known is not True:
        blockers.append(CandidateBlocker.RULES_MISSING)
    if item.holding_quantity is None or item.sellable_quantity is None or item.same_day_exit_unavailable is None:
        blockers.append(CandidateBlocker.ACCOUNT_STATE_MISSING)
    if item.orders_reconciled is not True:
        blockers.append(CandidateBlocker.ORDER_RECONCILIATION_MISSING)
    if item.same_day_exit_unavailable and mode is not TerminalMode.LAST_SHOT:
        blockers.append(CandidateBlocker.SAME_DAY_EXIT_UNAVAILABLE)
    elif item.same_day_exit_unavailable:
        warnings.append("SAME_DAY_EXIT_UNAVAILABLE_LAST_SHOT")
    return list(dict.fromkeys(blockers)), list(dict.fromkeys(warnings))


def _terminal_gate(item: CandidateObservation, mode: TerminalMode) -> tuple[bool, list[CandidateBlocker]]:
    blockers: list[CandidateBlocker] = []
    if any(value is None or value <= _ZERO for value in (item.trigger_price, item.invalidation_price, item.limit_price, item.max_weight)):
        blockers.append(CandidateBlocker.TERMINAL_RULES_MISSING)
    else:
        if item.max_weight > _MAX_SINGLE_WEIGHT:
            blockers.append(CandidateBlocker.TERMINAL_WEIGHT_EXCEEDS_CAP)
        if item.invalidation_price >= item.trigger_price:
            blockers.append(CandidateBlocker.TERMINAL_INVALIDATION_INVALID)
    return not blockers, blockers


def _score(
    item: CandidateObservation,
    all_items: tuple[CandidateObservation, ...],
    config: TerminalStrategyConfig,
    mode: TerminalMode,
) -> tuple[Decimal, dict[str, Decimal | None], dict[str, Decimal], list[str]]:
    warnings: list[str] = []
    relative_scores = [
        _percentile(item_value, [getattr(other, field) for other in all_items])
        for field, item_value in ((field, getattr(item, field)) for field in _RELATIVE_FIELDS)
    ]
    relative = sum(relative_scores, _ZERO) / Decimal(len(relative_scores))
    trend = item.trend_continuity or _ZERO
    liquidity_values = [
        _percentile(item.amount_cny, [other.amount_cny for other in all_items]),
        _percentile(item.turnover_rate, [other.turnover_rate for other in all_items]),
        _percentile(item.volume_ratio, [other.volume_ratio for other in all_items]),
    ]
    liquidity = (item.liquidity_score / _HUNDRED + sum(liquidity_values, _ZERO)) / Decimal("4")
    drawdown_quality = _ONE - (item.intraday_max_drawdown / config.funnel.maximum_intraday_drawdown)
    close = (item.close_strength + max(_ZERO, drawdown_quality)) / Decimal("2")
    evidence = item.evidence_quality or _ZERO
    execution = item.simulated_fill_rate * max(_ZERO, _ONE - item.simulated_slippage_p95)
    correlation = item.holding_correlation or _ZERO
    diversification = _ONE - correlation
    components: dict[str, Decimal | None] = {
        "relative_strength": relative,
        "trend_continuity": trend,
        "liquidity_quality": liquidity,
        "close_intraday_strength": close,
        "evidence_quality": evidence,
        "execution_probability": execution,
        "diversification_value": diversification,
    }
    penalties: dict[str, Decimal] = {}
    penalties["opening_gap"] = abs(item.opening_gap) / config.funnel.maximum_opening_gap * Decimal("0.05")
    penalties["intraday_drawdown"] = item.intraday_max_drawdown / config.funnel.maximum_intraday_drawdown * Decimal("0.10")
    if mode is not TerminalMode.LAST_SHOT:
        penalties["overheat"] = item.overheat_score / _HUNDRED * config.funnel.overheat_penalty
    else:
        warnings.append("NEW_LISTING_AND_OVERHEAT_NO_SCORE_BONUS")
        penalties["overheat"] = _ZERO
    for name in _RISK_FIELDS:
        penalties[name] = Decimal("0.05") if getattr(item, name) else _ZERO
    penalties["single_day_pulse"] = Decimal("0.10") if item.single_day_pulse else _ZERO
    penalties["same_day_exit"] = Decimal("0.10") if item.same_day_exit_unavailable else _ZERO
    penalties["simulated_failures"] = Decimal("0.02") * Decimal(len(item.simulated_failure_reasons))
    weights = config.funnel.weight_map
    weighted = sum((weights[name] * (value or _ZERO) for name, value in components.items()), _ZERO)
    score = max(_ZERO, weighted - sum(penalties.values(), _ZERO)) * _HUNDRED
    return score.quantize(Decimal("0.0001")), components, penalties, warnings


def _assessment(
    item: CandidateObservation,
    mode: TerminalMode,
    score: Decimal | None,
    components: Mapping[str, Decimal | None],
    penalties: Mapping[str, Decimal],
    blockers: Iterable[CandidateBlocker],
    warnings: Iterable[str],
    hard_gate_pass: bool,
    terminal_eligible: bool,
    *,
    overheat_threshold: Decimal = Decimal("80"),
) -> CandidateAssessment:
    return CandidateAssessment(
        symbol=item.symbol,
        mode=mode,
        score=score,
        score_components=tuple(sorted(components.items())),
        risk_penalties=tuple(sorted(penalties.items())),
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
        hard_gate_pass=hard_gate_pass,
        focus_eligible=False,
        terminal_eligible=terminal_eligible,
        trigger_price=item.trigger_price,
        invalidation_price=item.invalidation_price,
        limit_price=item.limit_price,
        max_weight=item.max_weight,
        observation_hash=item.observation_hash,
        risk_tags=_risk_tags(item, overheat_threshold),
    )


def _blocked_assessment(
    item: CandidateObservation,
    mode: TerminalMode,
    blockers: Iterable[CandidateBlocker],
    *,
    overheat_threshold: Decimal = Decimal("80"),
) -> CandidateAssessment:
    return _assessment(
        item, mode, None, {}, {}, blockers, (), False, False,
        overheat_threshold=overheat_threshold,
    )


def _risk_tags(item: CandidateObservation, overheat_threshold: Decimal) -> tuple[str, ...]:
    """Preserve explicit risk facts for downstream policy checks.

    The funnel already applies the permanent hard gates.  Allocation still
    needs to distinguish a new listing, an overheated observation, and a
    same-day exit constraint when selecting a protective sleeve, so those
    facts are carried forward as stable labels rather than inferred later.
    """

    tags: list[str] = []
    if item.is_new_listing is True:
        tags.append("NEW_LISTING")
    if item.overheat_score is not None and item.overheat_score >= overheat_threshold:
        tags.append("OVERHEATED")
    if item.same_day_exit_unavailable is True:
        tags.append("SAME_DAY_EXIT_UNAVAILABLE")
    if item.price_locked is True:
        tags.append("PRICE_LOCKED")
    if item.is_suspended is True:
        tags.append("SUSPENDED")
    if item.intraday_max_drawdown is not None and item.intraday_max_drawdown >= Decimal("0.04"):
        tags.append("HIGH_INTRADAY_DRAWDOWN")
    return tuple(tags)


def _assessment_sort_key(item: CandidateAssessment) -> tuple[Decimal, str, str]:
    return (-(item.score if item.score is not None else _ZERO), item.symbol, item.observation_hash)


def _result(
    mode: TerminalMode,
    as_of: datetime,
    trade_date: date,
    watchlist: tuple[CandidateAssessment, ...],
    focus: tuple[CandidateAssessment, ...],
    terminal: tuple[CandidateAssessment, ...],
    rejected: tuple[CandidateAssessment, ...],
    reasons: tuple[str, ...],
    blockers: tuple[CandidateBlocker, ...],
    config: TerminalStrategyConfig,
    input_hash: str,
    *,
    input_count: int | None = None,
    status: str | None = None,
) -> FunnelResult:
    return FunnelResult(
        mode=mode,
        as_of=as_of,
        trade_date=trade_date,
        watchlist=watchlist,
        focus=focus,
        terminal=terminal,
        rejected=rejected,
        status=status or ("UNAVAILABLE" if mode is TerminalMode.UNAVAILABLE else "PARTIAL"),
        reasons=reasons,
        blockers=blockers,
        input_count=input_count if input_count is not None else len(watchlist) + len(rejected),
        config_hash=config.config_hash,
        input_hash=input_hash,
        execution_ready=False,
    )


def _correlations(value: Mapping[str, Decimal] | tuple[tuple[str, Decimal], ...]) -> tuple[tuple[str, Decimal], ...]:
    entries = value.items() if isinstance(value, Mapping) else value
    result: list[tuple[str, Decimal]] = []
    for symbol, correlation in entries:
        if not isinstance(symbol, str) or not symbol.strip():
            raise CandidateFunnelError("correlation symbol must be non-empty")
        parsed = _finite_decimal(correlation, "correlation")
        if not _ZERO <= parsed <= _ONE:
            raise CandidateFunnelError("correlation must be between 0 and 1")
        result.append((symbol.strip(), parsed))
    return tuple(sorted(result))


def _percentile(value: Decimal | None, values: Iterable[Decimal | None]) -> Decimal:
    if value is None:
        return _ZERO
    present = sorted(item for item in values if item is not None)
    if not present:
        return _ZERO
    if len(present) == 1 or present[0] == present[-1]:
        return Decimal("0.5")
    positions = [Decimal(index) for index, item in enumerate(present) if item == value]
    return (sum(positions, _ZERO) / Decimal(len(positions))) / Decimal(len(present) - 1)


def _finite_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise CandidateFunnelError(f"{name} must be a finite decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CandidateFunnelError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise CandidateFunnelError(f"{name} must be a finite decimal")
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


_DECIMAL_FIELDS = (
    "relative_strength_1d", "relative_strength_3d", "relative_strength_5d",
    "benchmark_relative_1d", "benchmark_relative_3d", "benchmark_relative_5d",
    "amount_cny", "turnover_rate", "volume_ratio", "liquidity_score", "opening_gap",
    "close_strength", "intraday_max_drawdown", "trend_continuity", "overheat_score",
    "evidence_quality", "quote_age_seconds", "bid_price", "ask_price", "current_price",
    "previous_close", "simulated_fill_rate", "simulated_slippage_p95", "price_limit_pct",
    "trigger_price", "invalidation_price", "limit_price", "max_weight",
)
_RATIO_FIELDS = (
    "close_strength", "intraday_max_drawdown", "trend_continuity", "evidence_quality",
    "simulated_fill_rate", "simulated_slippage_p95", "price_limit_pct", "max_weight",
)
_BOOL_FIELDS = (
    "single_day_pulse", *_RISK_FIELDS, "risk_facts_complete", "evidence_complete",
    "is_suspended", "is_tradeable", "price_locked", "executable_quote", "has_valid_bid_ask",
    "price_limit_rule_known", "new_listing_rule_known", "is_new_listing", "same_day_exit_unavailable",
    "orders_reconciled",
)
_TEXT_FIELDS = ("industry", "correlation_cluster", "board", "source_hash")


__all__ = [
    "CandidateAssessment", "CandidateBlocker", "CandidateFunnelError", "CandidateObservation",
    "FunnelResult", "build_candidate_funnel", "build_terminal_candidate_funnel",
    "evaluate_candidate_funnel",
]

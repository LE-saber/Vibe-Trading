"""Pure orchestration for immutable, read-only terminal evaluations.

The service consumes only already-captured typed inputs.  It never resolves a
provider, reads a live account, calls a network endpoint, writes an order, or
hands an approval to S6.  Its job is to connect the deterministic terminal
components and preserve every upstream blocker in one auditable result.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import datetime
import re
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash

from .allocation import (
    TerminalAllocationError,
    TerminalAllocationInputs,
    TerminalAllocationPolicy,
    build_terminal_allocation_policy,
)
from .award_line import forecast_award_line
from .candidate_funnel import CandidateObservation, FunnelResult, build_candidate_funnel
from .config import TerminalStrategyConfig
from .models import (
    AwardLineForecast,
    AwardLinePoint,
    TerminalBlocker,
    TerminalMode,
    TerminalModeDecision,
    TerminalModelError,
    TerminalSnapshot,
)
from .state_machine import decide_terminal_mode


SHANGHAI = ZoneInfo("Asia/Shanghai")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")
_HASH = re.compile(r"(?:sha256:)?[0-9a-fA-F]{64}\Z")

AwardLineHistory = tuple[AwardLinePoint, ...]


class TerminalServiceError(TerminalModelError):
    """Raised when a request cannot be safely normalised or evaluated."""


@dataclass(frozen=True, slots=True)
class TerminalSourceHashes:
    """Hashes of all upstream inputs required for later audit/S6 binding."""

    s4_hash: str
    s5_hash: str
    s5_5_hash: str
    account_snapshot_hash: str
    leaderboard_snapshot_hash: str
    config_hash: str
    implementation_hash: str

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            object.__setattr__(self, field.name, _normalise_hash(value, field.name))

    @property
    def s55_hash(self) -> str:
        """Short alias retained for adapters that use ``S5.5`` spelling."""

        return self.s5_5_hash

    @property
    def source_hashes_hash(self) -> str:
        return content_hash(self.to_dict(include_hash=False))

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result = {
            "schema_version": "competition.terminal-source-hashes.v1",
            "s4_hash": self.s4_hash,
            "s5_hash": self.s5_hash,
            "s5_5_hash": self.s5_5_hash,
            "account_snapshot_hash": self.account_snapshot_hash,
            "leaderboard_snapshot_hash": self.leaderboard_snapshot_hash,
            "config_hash": self.config_hash,
            "implementation_hash": self.implementation_hash,
        }
        if include_hash:
            result["source_hashes_hash"] = self.source_hashes_hash
        return result


TerminalEvaluationSourceHashes = TerminalSourceHashes


@dataclass(frozen=True, slots=True)
class TerminalEvaluationRequest:
    """Complete, provider-neutral input for one terminal evaluation."""

    run_id: str
    snapshot: TerminalSnapshot
    award_line_history: tuple[AwardLinePoint, ...]
    candidates: tuple[CandidateObservation, ...]
    core_symbols: tuple[str, ...]
    config: TerminalStrategyConfig
    source_hashes: TerminalSourceHashes
    observed_at: datetime
    previous_mode: TerminalMode | None = None
    downgrade_confirmation_count: int = 0
    same_day_exit_unavailable_symbols: tuple[str, ...] = ()
    full_snapshot: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or _IDENTIFIER.fullmatch(self.run_id) is None:
            raise TerminalServiceError("run_id is invalid")
        if ".." in self.run_id or "/" in self.run_id or "\\" in self.run_id:
            raise TerminalServiceError("run_id contains path traversal")
        if not isinstance(self.snapshot, TerminalSnapshot):
            raise TerminalServiceError("snapshot must be a TerminalSnapshot")
        if not isinstance(self.config, TerminalStrategyConfig):
            raise TerminalServiceError("config must be a TerminalStrategyConfig")
        if not isinstance(self.source_hashes, TerminalSourceHashes):
            raise TerminalServiceError("source_hashes must be TerminalSourceHashes")
        if self.source_hashes.config_hash != self.config.config_hash:
            raise TerminalServiceError("source hash config_hash does not match config")
        if (
            self.snapshot.account_snapshot_hash is not None
            and self.snapshot.account_snapshot_hash != self.source_hashes.account_snapshot_hash
        ):
            raise TerminalServiceError("account snapshot hash is not source-bound")
        if (
            self.snapshot.leaderboard_snapshot_hash is not None
            and self.snapshot.leaderboard_snapshot_hash != self.source_hashes.leaderboard_snapshot_hash
        ):
            raise TerminalServiceError("leaderboard snapshot hash is not source-bound")
        if not isinstance(self.observed_at, datetime) or self.observed_at.utcoffset() is None:
            raise TerminalServiceError("observed_at must be timezone-aware")
        if self.observed_at < self.snapshot.as_of:
            raise TerminalServiceError("observed_at cannot precede snapshot.as_of")
        if not isinstance(self.award_line_history, (tuple, list)):
            raise TerminalServiceError("award_line_history must contain AwardLinePoint values")
        history_input = tuple(self.award_line_history)
        if not all(isinstance(item, AwardLinePoint) for item in history_input):
            raise TerminalServiceError("award_line_history must contain AwardLinePoint values")
        history = tuple(sorted(history_input, key=lambda item: (item.as_of, item.rank)))
        if len({(item.as_of, item.rank) for item in history}) != len(history):
            raise TerminalServiceError("award_line_history contains duplicate points")
        object.__setattr__(self, "award_line_history", history)
        if not isinstance(self.candidates, (tuple, list)):
            raise TerminalServiceError("candidates must contain CandidateObservation values")
        candidates_input = tuple(self.candidates)
        if not all(isinstance(item, CandidateObservation) for item in candidates_input):
            raise TerminalServiceError("candidates must contain CandidateObservation values")
        if any(item.trade_date != self.snapshot.trade_date for item in candidates_input):
            raise TerminalServiceError("candidate trade_date must match snapshot.trade_date")
        if any(item.as_of != self.snapshot.as_of for item in candidates_input):
            raise TerminalServiceError("candidate as_of must match snapshot.as_of")
        object.__setattr__(
            self,
            "candidates",
            tuple(sorted(candidates_input, key=lambda item: (item.symbol, item.observation_hash))),
        )
        if not isinstance(self.core_symbols, (tuple, list)):
            raise TerminalServiceError("core_symbols must be a tuple")
        core_input = tuple(self.core_symbols)
        core = tuple(item.strip() for item in core_input if isinstance(item, str))
        if len(core) != len(core_input) or not core:
            raise TerminalServiceError("core_symbols must contain non-empty strings")
        for symbol in core:
            try:
                CanonicalSymbol.from_core(symbol)
            except (SymbolContractError, TypeError, ValueError) as exc:
                raise TerminalServiceError("core_symbols must be canonical") from exc
        if len(set(core)) != len(core):
            raise TerminalServiceError("core_symbols must be unique")
        object.__setattr__(self, "core_symbols", tuple(sorted(core)))
        if self.previous_mode is not None and not isinstance(self.previous_mode, TerminalMode):
            try:
                object.__setattr__(self, "previous_mode", TerminalMode(self.previous_mode))
            except ValueError as exc:
                raise TerminalServiceError("previous_mode is invalid") from exc
        if type(self.downgrade_confirmation_count) is not int or self.downgrade_confirmation_count < 0:
            raise TerminalServiceError("downgrade_confirmation_count must be non-negative")
        if not isinstance(self.same_day_exit_unavailable_symbols, (tuple, list)):
            raise TerminalServiceError("same_day_exit_unavailable_symbols must be a tuple")
        same_day_input = tuple(self.same_day_exit_unavailable_symbols)
        same_day = tuple(item.strip() for item in same_day_input if isinstance(item, str))
        if len(same_day) != len(same_day_input):
            raise TerminalServiceError("same-day exit symbols must be non-empty strings")
        for symbol in same_day:
            try:
                CanonicalSymbol.from_core(symbol)
            except (SymbolContractError, TypeError, ValueError) as exc:
                raise TerminalServiceError("same-day exit symbols must be canonical") from exc
        if len(set(same_day)) != len(same_day):
            raise TerminalServiceError("same-day exit symbols must be unique")
        object.__setattr__(self, "same_day_exit_unavailable_symbols", tuple(sorted(same_day)))
        if type(self.full_snapshot) is not bool:
            raise TerminalServiceError("full_snapshot must be boolean")

    @property
    def request_hash(self) -> str:
        return content_hash(self.to_dict(include_hash=False))

    @property
    def evaluation_id(self) -> str:
        return content_hash(
            {
                "schema_version": "competition.terminal-evaluation-id.v1",
                "run_id": self.run_id,
                "request_hash": self.request_hash,
                "implementation_hash": self.source_hashes.implementation_hash,
            }
        )

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": "competition.terminal-evaluation-request.v1",
            "run_id": self.run_id,
            "snapshot": self.snapshot.to_dict(),
            "award_line_history": [item.to_dict() for item in self.award_line_history],
            "candidates": [item.to_dict() for item in self.candidates],
            "core_symbols": list(self.core_symbols),
            "config": self.config.to_dict(),
            "config_hash": self.config.config_hash,
            "source_hashes": self.source_hashes.to_dict(),
            "observed_at": self.observed_at.isoformat(),
            "previous_mode": self.previous_mode.value if self.previous_mode else None,
            "downgrade_confirmation_count": self.downgrade_confirmation_count,
            "same_day_exit_unavailable_symbols": list(self.same_day_exit_unavailable_symbols),
            "full_snapshot": self.full_snapshot,
        }
        if include_hash:
            result["request_hash"] = self.request_hash
            result["evaluation_id"] = self.evaluation_id
        return result


@dataclass(frozen=True, slots=True)
class TerminalEvaluationResult:
    """All derived read-only products from one deterministic evaluation."""

    run_id: str
    evaluation_id: str
    request_hash: str
    status: str
    snapshot: TerminalSnapshot
    award_line_history: tuple[AwardLinePoint, ...]
    forecast: AwardLineForecast
    mode_decision: TerminalModeDecision
    funnel: FunnelResult
    allocation_policy: TerminalAllocationPolicy | None
    source_hashes: TerminalSourceHashes
    config_hash: str
    blockers: tuple[str, ...]
    reasons: tuple[str, ...]
    execution_ready: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or _IDENTIFIER.fullmatch(self.run_id) is None:
            raise TerminalServiceError("result run_id is invalid")
        if not isinstance(self.evaluation_id, str) or _HASH.fullmatch(self.evaluation_id) is None:
            raise TerminalServiceError("result evaluation_id is invalid")
        if not isinstance(self.request_hash, str) or _HASH.fullmatch(self.request_hash) is None:
            raise TerminalServiceError("result request_hash is invalid")
        if self.status not in {"COMPLETE", "BLOCKED", "UNAVAILABLE"}:
            raise TerminalServiceError("result status is invalid")
        if not isinstance(self.snapshot, TerminalSnapshot):
            raise TerminalServiceError("result snapshot is invalid")
        if not isinstance(self.forecast, AwardLineForecast):
            raise TerminalServiceError("result forecast is invalid")
        if not isinstance(self.mode_decision, TerminalModeDecision):
            raise TerminalServiceError("result mode_decision is invalid")
        if not isinstance(self.funnel, FunnelResult):
            raise TerminalServiceError("result funnel is invalid")
        if self.allocation_policy is not None and not isinstance(
            self.allocation_policy, TerminalAllocationPolicy
        ):
            raise TerminalServiceError("result allocation_policy is invalid")
        if self.allocation_policy is not None and self.status != "COMPLETE":
            raise TerminalServiceError("blocked result cannot carry a policy")
        if self.allocation_policy is None and self.status == "COMPLETE":
            raise TerminalServiceError("complete result requires an allocation policy")
        if self.config_hash != self.source_hashes.config_hash:
            raise TerminalServiceError("result config hash is not source-bound")
        if self.mode_decision.execution_ready:
            raise TerminalServiceError("service result mode decision must not be execution-ready")
        if self.funnel.execution_ready:
            raise TerminalServiceError("service result funnel must not be execution-ready")
        if self.allocation_policy is not None and self.allocation_policy.execution_ready:
            raise TerminalServiceError("service result allocation must not be execution-ready")
        if type(self.execution_ready) is not bool or self.execution_ready:
            raise TerminalServiceError("service result is always research-only")
        if not isinstance(self.blockers, tuple) or not all(isinstance(item, str) and item for item in self.blockers):
            raise TerminalServiceError("result blockers must be non-empty strings")
        if not isinstance(self.reasons, tuple) or not all(isinstance(item, str) and item for item in self.reasons):
            raise TerminalServiceError("result reasons must be non-empty strings")

    @property
    def result_hash(self) -> str:
        return content_hash(self.to_dict(include_hash=False))

    @property
    def forecast_hash(self) -> str:
        return self.forecast.forecast_hash

    @property
    def mode_decision_hash(self) -> str:
        return self.mode_decision.decision_hash

    @property
    def candidate_funnel_hash(self) -> str:
        return self.funnel.funnel_hash

    @property
    def allocation_policy_hash(self) -> str | None:
        return self.allocation_policy.policy_hash if self.allocation_policy else None

    @property
    def source_hashes_hash(self) -> str:
        return self.source_hashes.source_hashes_hash

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": "competition.terminal-evaluation-result.v1",
            "run_id": self.run_id,
            "evaluation_id": self.evaluation_id,
            "request_hash": self.request_hash,
            "status": self.status,
            "snapshot": self.snapshot.to_dict(),
            "award_line_history": [item.to_dict() for item in self.award_line_history],
            "forecast": self.forecast.to_dict(),
            "mode_decision": self.mode_decision.to_dict(),
            "funnel": self.funnel.to_dict(),
            "allocation_policy": self.allocation_policy.to_dict() if self.allocation_policy else None,
            "source_hashes": self.source_hashes.to_dict(),
            "config_hash": self.config_hash,
            "award_line_forecast_hash": self.forecast_hash,
            "mode_decision_hash": self.mode_decision_hash,
            "candidate_funnel_hash": self.candidate_funnel_hash,
            "allocation_policy_hash": self.allocation_policy_hash,
            "source_hashes_hash": self.source_hashes_hash,
            "blockers": list(self.blockers),
            "reasons": list(self.reasons),
            "execution_ready": False,
        }
        if include_hash:
            result["result_hash"] = self.result_hash
        return result


TerminalEvaluation = TerminalEvaluationResult
TerminalRequest = TerminalEvaluationRequest


class TerminalStrategyService:
    """Pure orchestrator for the terminal forecast/state/funnel/allocation chain."""

    def evaluate(self, request: TerminalEvaluationRequest) -> TerminalEvaluationResult:
        if not isinstance(request, TerminalEvaluationRequest):
            raise TerminalServiceError("request must be a TerminalEvaluationRequest")

        forecast = forecast_award_line(
            request.snapshot,
            request.config,
            request.award_line_history,
            observed_at=request.observed_at,
        )
        decision = decide_terminal_mode(
            request.snapshot,
            forecast,
            request.config,
            previous_mode=request.previous_mode,
            downgrade_confirmation_count=request.downgrade_confirmation_count,
            full_snapshot=request.full_snapshot,
            observed_at=request.observed_at,
        )
        # The state-machine module predates this service and marks an available
        # decision as execution-ready for its own historical API.  At this
        # research boundary the flag is always false, so the persisted hash
        # cannot be mistaken for an approval or S6 plan.
        decision = replace(decision, execution_ready=False)
        funnel = build_candidate_funnel(
            request.candidates,
            request.config,
            decision.mode,
            observed_at=request.observed_at,
        )

        allocation_policy: TerminalAllocationPolicy | None = None
        allocation_blockers: tuple[str, ...] = ()
        allocation_reasons: tuple[str, ...] = ()
        allocation_inputs = TerminalAllocationInputs(
            mode_decision=decision,
            funnel=funnel,
            core_symbols=request.core_symbols,
            same_day_exit_unavailable_symbols=request.same_day_exit_unavailable_symbols,
        )
        try:
            allocation_policy = build_terminal_allocation_policy(allocation_inputs, request.config)
        except TerminalAllocationError as exc:
            allocation_blockers = tuple(item.value for item in exc.blockers)
            allocation_reasons = exc.reasons

        blockers = _unique_codes(
            tuple(item.value for item in forecast.blockers)
            + tuple(item.value for item in decision.blockers)
            + tuple(item.value for item in funnel.blockers)
            + allocation_blockers
        )
        reasons = _unique_codes(
            (f"FORECAST_METHOD={forecast.method}",) if forecast.method else ()
        )
        reasons = _unique_codes(reasons + decision.reasons + funnel.reasons + allocation_reasons)
        if allocation_policy is not None:
            status = "COMPLETE"
        elif decision.mode is TerminalMode.UNAVAILABLE or not forecast.available:
            status = "UNAVAILABLE"
        else:
            status = "BLOCKED"
        return TerminalEvaluationResult(
            run_id=request.run_id,
            evaluation_id=request.evaluation_id,
            request_hash=request.request_hash,
            status=status,
            snapshot=request.snapshot,
            award_line_history=request.award_line_history,
            forecast=forecast,
            mode_decision=decision,
            funnel=funnel,
            allocation_policy=allocation_policy,
            source_hashes=request.source_hashes,
            config_hash=request.config.config_hash,
            blockers=blockers,
            reasons=reasons,
            execution_ready=False,
        )

    def evaluate_and_persist(self, request: TerminalEvaluationRequest, persistence: Any):
        """Evaluate and hand the immutable result to an explicit persistence object."""

        result = self.evaluate(request)
        return persistence.persist(result)


TerminalEvaluationService = TerminalStrategyService


def evaluate_terminal_request(request: TerminalEvaluationRequest) -> TerminalEvaluationResult:
    return TerminalStrategyService().evaluate(request)


run_terminal_evaluation = evaluate_terminal_request


def _normalise_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise TerminalServiceError(f"{field} is missing or invalid")
    if value.lower().startswith("sha256:"):
        return "sha256:" + value[7:].lower()
    return value.lower()


def _unique_codes(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


__all__ = [
    "TerminalEvaluationRequest",
    "TerminalEvaluationResult",
    "TerminalEvaluationSourceHashes",
    "TerminalEvaluation",
    "TerminalRequest",
    "AwardLineHistory",
    "TerminalServiceError",
    "TerminalSourceHashes",
    "TerminalStrategyService",
    "TerminalEvaluationService",
    "evaluate_terminal_request",
    "run_terminal_evaluation",
]

"""Local runtime orchestration for read-only terminal evaluations.

The runtime is the composition boundary only.  It accepts already loaded
pipeline artifacts and captured read-only views, invokes the two adapters and
the existing pure service, and optionally persists the resulting immutable
evaluation.  It has no HTTP, network, environment, broker, or order access.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import content_hash

from .config import TerminalStrategyConfig
from .models import AwardLinePoint, TerminalMode
from .persistence import TerminalEvaluationPersistence, TerminalPersistenceError
from .pipeline_adapter import (
    PipelineAdapterBlocker,
    PipelineAdapterDiagnostics,
    adapt_pipeline_candidates,
)
from .service import (
    TerminalEvaluationRequest,
    TerminalEvaluationResult,
    TerminalServiceError,
    TerminalSourceHashes,
    TerminalStrategyService,
)
from .snapshot_adapter import (
    SnapshotAdapterBlocker,
    SnapshotAdapterDiagnostics,
    SnapshotAdapterResult,
    adapt_account_leaderboard_snapshot,
)


_HASH = re.compile(r"^(?:sha256:)?[0-9a-fA-F]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_IMPLEMENTATION_FILES = (
    "allocation.py",
    "award_line.py",
    "candidate_funnel.py",
    "config.py",
    "models.py",
    "persistence.py",
    "pipeline_adapter.py",
    "realtime_stage.py",
    "runtime.py",
    "service.py",
    "snapshot_adapter.py",
    "state_machine.py",
)


@dataclass(frozen=True, slots=True)
class RuntimeDiagnostics:
    blockers: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    snapshot: SnapshotAdapterDiagnostics | None = None
    candidates: PipelineAdapterDiagnostics | None = None
    stage_hashes: tuple[tuple[str, str], ...] = ()
    implementation_hash: str | None = None
    core_symbols: tuple[str, ...] = ()
    persistence_error: str | None = None

    @property
    def blocked(self) -> bool:
        return bool(self.blockers)

    def to_dict(self) -> dict[str, Any]:
        return {
            "blockers": list(self.blockers),
            "reasons": list(self.reasons),
            "snapshot": None if self.snapshot is None else self.snapshot.to_dict(),
            "candidates": None if self.candidates is None else self.candidates.to_dict(),
            "stage_hashes": {name: digest for name, digest in self.stage_hashes},
            "implementation_hash": self.implementation_hash,
            "core_symbols": list(self.core_symbols),
            "persistence_error": self.persistence_error,
        }


@dataclass(frozen=True, slots=True)
class TerminalRuntimeResult:
    """Runtime result; never execution-ready."""

    evaluation: TerminalEvaluationResult | None
    status: str
    diagnostics: RuntimeDiagnostics
    path: Path | None = None
    execution_ready: bool = False

    @property
    def evaluation_id(self) -> str | None:
        return None if self.evaluation is None else self.evaluation.evaluation_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "evaluation": None if self.evaluation is None else self.evaluation.to_dict(),
            "diagnostics": self.diagnostics.to_dict(),
            "path": None if self.path is None else str(self.path),
            "execution_ready": False,
        }


RuntimeResult = TerminalRuntimeResult
TerminalRuntime = TerminalRuntimeResult


def terminal_implementation_hash() -> str:
    """Hash a fixed set of files in this terminal package only."""

    package = Path(__file__).resolve().parent
    files: list[dict[str, str]] = []
    for name in _IMPLEMENTATION_FILES:
        path = package / name
        if not path.is_file() or path.is_symlink():
            raise TerminalServiceError(f"controlled terminal implementation file is unavailable: {name}")
        files.append({"name": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    return content_hash({"schema_version": "competition.terminal-implementation.v1", "files": files})


def run_terminal_runtime(
    run_id: str,
    s4_stage: Mapping[str, Any],
    s5_stage: Mapping[str, Any],
    s5_5_stage: Mapping[str, Any],
    account_view: Mapping[str, Any],
    leaderboard_view: Mapping[str, Any],
    observed_at: datetime,
    config: TerminalStrategyConfig,
    persistence_root: str | Path,
    *,
    award_line_history: Iterable[AwardLinePoint] = (),
    previous_mode: TerminalMode | None = None,
    downgrade_confirmation_count: int = 0,
    same_day_exit_unavailable_symbols: Iterable[str] | None = None,
    core_symbols: Iterable[str] | None = None,
    implementation_hash: str | None = None,
    freshness_threshold_seconds: int | None = None,
) -> TerminalRuntimeResult:
    """Run one deterministic terminal evaluation and persist its result."""

    blockers: list[str] = []
    reasons: list[str] = []
    impl_hash = _normalise_hash_or_none(implementation_hash)
    if implementation_hash is not None and impl_hash is None:
        blockers.append("IMPLEMENTATION_HASH_INVALID")
    if impl_hash is None and implementation_hash is None:
        try:
            impl_hash = terminal_implementation_hash()
        except (OSError, TerminalServiceError):
            blockers.append("IMPLEMENTATION_HASH_UNAVAILABLE")

    stage_hashes = _stage_hashes((s4_stage, s5_stage, s5_5_stage), blockers)
    remaining_trade_dates = _derive_remaining_trade_dates(config, observed_at, reasons, blockers)
    snapshot_result = adapt_account_leaderboard_snapshot(
        account_view,
        leaderboard_view,
        remaining_trade_dates,
        observed_at,
        config,
        freshness_threshold_seconds,
    )
    blockers.extend(snapshot_result.diagnostics.blockers)

    explicit_core, core_error = _normalise_core_symbols(core_symbols)
    if core_error:
        blockers.append(core_error)
        selected_core: tuple[str, ...] = ()
    elif explicit_core is not None:
        selected_core = explicit_core
    else:
        selected_core = tuple(sorted(snapshot_result.account_position_facts))
    if not selected_core:
        blockers.append("CORE_SYMBOLS_MISSING")

    candidate_result = None
    if snapshot_result.snapshot is not None and impl_hash is not None:
        candidate_result = adapt_pipeline_candidates(
            s4_stage,
            s5_stage,
            s5_5_stage,
            snapshot_result.account_position_facts,
            observed_at,
            same_day_exit_unavailable_symbols,
            max_quote_age_seconds=(
                freshness_threshold_seconds
                if freshness_threshold_seconds is not None
                else config.funnel.max_quote_age_seconds
            ),
        )
        blockers.extend(candidate_result.diagnostics.blockers)

    fatal = bool(blockers and _fatal_blockers(blockers))
    if snapshot_result.snapshot is None:
        fatal = True
    if candidate_result is None:
        fatal = True
    if not impl_hash:
        fatal = True
    if not selected_core:
        fatal = True

    if fatal:
        diagnostics = RuntimeDiagnostics(
            blockers=_unique(blockers),
            reasons=tuple(reasons),
            snapshot=snapshot_result.diagnostics,
            candidates=None if candidate_result is None else candidate_result.diagnostics,
            stage_hashes=stage_hashes,
            implementation_hash=impl_hash,
            core_symbols=selected_core,
        )
        status = "UNAVAILABLE" if snapshot_result.snapshot is None else "BLOCKED"
        return TerminalRuntimeResult(None, status, diagnostics, None, False)

    assert snapshot_result.snapshot is not None
    assert candidate_result is not None
    assert impl_hash is not None
    account_hash = snapshot_result.account_hash
    leaderboard_hash = snapshot_result.leaderboard_hash
    if not account_hash or not leaderboard_hash:
        diagnostics = RuntimeDiagnostics(
            blockers=_unique(blockers + ["SOURCE_HASH_MISSING"]),
            reasons=tuple(reasons), snapshot=snapshot_result.diagnostics,
            candidates=candidate_result.diagnostics, stage_hashes=stage_hashes,
            implementation_hash=impl_hash, core_symbols=selected_core,
        )
        return TerminalRuntimeResult(None, "UNAVAILABLE", diagnostics, None, False)
    try:
        source_hashes = TerminalSourceHashes(
            s4_hash=stage_hashes[0][1],
            s5_hash=stage_hashes[1][1],
            s5_5_hash=stage_hashes[2][1],
            account_snapshot_hash=account_hash,
            leaderboard_snapshot_hash=leaderboard_hash,
            config_hash=config.config_hash,
            implementation_hash=impl_hash,
        )
        history = tuple(award_line_history)
        same_day_for_request = _normalise_symbol_tuple(same_day_exit_unavailable_symbols)
        request = TerminalEvaluationRequest(
            run_id=run_id,
            snapshot=snapshot_result.snapshot,
            award_line_history=history,
            candidates=candidate_result.candidates,
            core_symbols=selected_core,
            config=config,
            source_hashes=source_hashes,
            observed_at=observed_at,
            previous_mode=previous_mode,
            downgrade_confirmation_count=downgrade_confirmation_count,
            same_day_exit_unavailable_symbols=same_day_for_request,
            full_snapshot=not bool(blockers),
        )
    except (TerminalServiceError, TypeError, ValueError) as exc:
        diagnostics = RuntimeDiagnostics(
            blockers=_unique(blockers + ["REQUEST_INVALID"]),
            reasons=tuple(reasons + [type(exc).__name__]), snapshot=snapshot_result.diagnostics,
            candidates=candidate_result.diagnostics, stage_hashes=stage_hashes,
            implementation_hash=impl_hash, core_symbols=selected_core,
        )
        return TerminalRuntimeResult(None, "BLOCKED", diagnostics, None, False)

    service = TerminalStrategyService()
    try:
        evaluation = service.evaluate(request)
        path = TerminalEvaluationPersistence(persistence_root).persist(evaluation)
    except (TerminalPersistenceError, OSError, TypeError, ValueError) as exc:
        diagnostics = RuntimeDiagnostics(
            blockers=_unique(blockers + ["PERSISTENCE_FAILED"]),
            reasons=tuple(reasons + [type(exc).__name__]), snapshot=snapshot_result.diagnostics,
            candidates=candidate_result.diagnostics, stage_hashes=stage_hashes,
            implementation_hash=impl_hash, core_symbols=selected_core,
            persistence_error=type(exc).__name__,
        )
        return TerminalRuntimeResult(evaluation, evaluation.status, diagnostics, None, False)
    diagnostics = RuntimeDiagnostics(
        blockers=_unique(blockers), reasons=tuple(reasons), snapshot=snapshot_result.diagnostics,
        candidates=candidate_result.diagnostics, stage_hashes=stage_hashes,
        implementation_hash=impl_hash, core_symbols=selected_core,
    )
    return TerminalRuntimeResult(evaluation, evaluation.status, diagnostics, path, False)


evaluate_terminal_runtime = run_terminal_runtime
run_terminal_evaluation_runtime = run_terminal_runtime


def _stage_hashes(stages: tuple[Any, ...], blockers: list[str]) -> tuple[tuple[str, str], ...]:
    names = ("S4", "S5", "S5_5")
    result: list[tuple[str, str]] = []
    for name, stage in zip(names, stages):
        value = stage.get("artifact_hash") if isinstance(stage, Mapping) else None
        digest = _normalise_hash_or_none(value)
        if digest is None:
            blockers.append(f"{name}_HASH_INVALID")
        else:
            result.append((name, digest))
    return tuple(result)


def _derive_remaining_trade_dates(
    config: TerminalStrategyConfig | Any,
    observed_at: datetime,
    reasons: list[str],
    blockers: list[str],
) -> tuple[date, ...]:
    """Derive the sessions remaining from the confirmed competition window.

    ``competition.remaining_trade_dates`` is the immutable, confirmed calendar
    window from the competition configuration.  It is deliberately not passed
    through unchanged: after each observed session, dates before that session
    must disappear.  A date outside this window is never coerced to the nearest
    session because doing so would silently turn weekends, holidays, pre-start
    observations, and post-final observations into valid trading days.
    """

    if not isinstance(config, TerminalStrategyConfig):
        blockers.append("CONFIG_INVALID")
        return ()
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        blockers.append("OBSERVED_AT_INVALID")
        return ()

    observed_trade_date = observed_at.astimezone(_SHANGHAI).date()
    calendar = config.competition.remaining_trade_dates
    if (
        not calendar
        or any(not isinstance(item, date) or isinstance(item, datetime) for item in calendar)
        or len(set(calendar)) != len(calendar)
        or calendar != tuple(sorted(calendar))
        or config.competition.final_trade_date not in calendar
        or config.competition.final_trade_date != max(calendar)
    ):
        blockers.append("CALENDAR_INVALID")
        reasons.append("CONFIRMED_TRADE_CALENDAR_INVALID")
        return ()
    if observed_trade_date not in calendar:
        blockers.append("TRADE_DATE_NOT_IN_CALENDAR")
        if observed_trade_date < calendar[0]:
            reason = "OBSERVED_TRADE_DATE_BEFORE_CALENDAR"
        elif observed_trade_date > config.competition.final_trade_date:
            reason = "OBSERVED_TRADE_DATE_AFTER_FINAL_SESSION"
        else:
            reason = "OBSERVED_TRADE_DATE_NOT_CONFIRMED_SESSION"
        reasons.append(reason)
        return ()

    remaining = tuple(item for item in calendar if item >= observed_trade_date)
    if not remaining or remaining[-1] != config.competition.final_trade_date:
        blockers.append("CALENDAR_INVALID")
        reasons.append("FINAL_TRADE_DATE_NOT_PRESERVED")
        return ()
    return remaining


def _normalise_hash_or_none(value: Any) -> str | None:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        return None
    return value.lower().removeprefix("sha256:")


def _normalise_core_symbols(values: Iterable[str] | None) -> tuple[tuple[str, ...] | None, str | None]:
    if values is None:
        return None, None
    if isinstance(values, (str, bytes)):
        return None, "CORE_SYMBOLS_INVALID"
    try:
        raw = tuple(values)
    except TypeError:
        return None, "CORE_SYMBOLS_INVALID"
    result: list[str] = []
    for item in raw:
        try:
            result.append(CanonicalSymbol.from_core(item).symbol)
        except (SymbolContractError, TypeError, ValueError):
            return None, "CORE_SYMBOLS_INVALID"
    if len(result) != len(set(result)):
        return None, "CORE_SYMBOLS_INVALID"
    return tuple(sorted(result)), None


def _normalise_symbol_tuple(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None or isinstance(values, (str, bytes)):
        return ()
    result: list[str] = []
    try:
        for item in values:
            try:
                result.append(CanonicalSymbol.from_core(item).symbol)
            except (SymbolContractError, TypeError, ValueError):
                return ()
    except TypeError:
        return ()
    return tuple(sorted(set(result)))


def _fatal_blockers(blockers: Iterable[str]) -> bool:
    fatal_codes = {
        "IMPLEMENTATION_HASH_INVALID", "IMPLEMENTATION_HASH_UNAVAILABLE", "S4_HASH_INVALID",
        "S5_HASH_INVALID", "S5_5_HASH_INVALID", "STAGE_HASH_INVALID", "STAGE_HASH_MISMATCH",
        "STAGE_SCHEMA_INVALID", "STAGE_RELATION_INVALID", "STAGE_CONTEXT_INVALID", "STAGE_IDENTITY_INVALID",
        "STAGE_STATUS_NOT_SUCCESS", "ROW_SCHEMA_INVALID", "ROW_SYMBOL_MISMATCH",
        "DUPLICATE_SYMBOL", "SYMBOL_INVALID", "OBSERVED_AT_INVALID", "CONFIG_INVALID", "STAGE_FUTURE",
        "ACCOUNT_VIEW_INVALID", "ACCOUNT_SCHEMA_INVALID", "LEADERBOARD_VIEW_INVALID",
        "LEADERBOARD_SCHEMA_INVALID", "SNAPSHOT_UNAVAILABLE", "REMAINING_DATES_INVALID",
        "TRADE_DATE_NOT_IN_CALENDAR", "CALENDAR_INVALID",
        "SOURCE_HASH_MISSING", "CORE_SYMBOLS_INVALID", "CORE_SYMBOLS_MISSING",
        "REQUEST_INVALID",
    }
    return any(item in fatal_codes for item in blockers)


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item for item in values if item))


__all__ = [
    "RuntimeDiagnostics",
    "RuntimeResult",
    "TerminalRuntime",
    "TerminalRuntimeResult",
    "evaluate_terminal_runtime",
    "run_terminal_evaluation_runtime",
    "run_terminal_runtime",
    "terminal_implementation_hash",
]

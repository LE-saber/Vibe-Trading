"""Shared read-only terminal-strategy orchestration.

This module is deliberately independent of FastAPI.  The API and the command
line surface both call the same bounded runtime entry point so neither surface
can acquire a second set of terminal-strategy rules.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from competition_core.pipeline.errors import PipelineError


def evaluate_terminal_request(
    *,
    tasks,
    run_id: str,
    account_reader: Callable[[], dict[str, Any]],
    leaderboard_reader: Callable[[], dict[str, Any]],
    core_symbols=None,
    same_day_exit_unavailable_symbols=None,
    previous_mode=None,
    downgrade_confirmation_count: int = 0,
) -> dict[str, Any]:
    """Evaluate terminal facts using only server-owned research inputs.

    ``account_reader`` and ``leaderboard_reader`` are read-only boundary
    functions.  The verified imported account is preferred by the shared API
    helper before either provider reader is called.
    """

    # Keep these imports local.  ``research_api`` imports this module from its
    # endpoint, while the CLI imports this module without starting FastAPI.
    from competition_core.research_api import (
        _imported_terminal_account_view,
        _previous_terminal_mode,
        _terminal_now,
        _terminal_stage_bundle,
    )
    from competition_core.terminal_strategy import load_terminal_strategy_config
    from competition_core.terminal_strategy.api_views import decision_projection
    from competition_core.terminal_strategy.runtime import run_terminal_runtime

    _, stages = _terminal_stage_bundle(tasks, run_id)
    config = load_terminal_strategy_config()
    continuity_mode = _previous_terminal_mode(tasks, run_id, previous_mode)
    account_view = _imported_terminal_account_view(tasks, run_id)
    if account_view is None:
        account_view = account_reader()
    leaderboard_view = leaderboard_reader()
    runtime = run_terminal_runtime(
        run_id,
        stages["S4"],
        stages["S5"],
        stages["S5_5"],
        account_view,
        leaderboard_view,
        _terminal_now(tasks),
        config,
        tasks.root,
        previous_mode=continuity_mode,
        downgrade_confirmation_count=downgrade_confirmation_count,
        same_day_exit_unavailable_symbols=same_day_exit_unavailable_symbols,
        core_symbols=core_symbols,
    )
    if runtime.evaluation is None or runtime.path is None:
        if runtime.diagnostics.persistence_error:
            raise PipelineError("STALE_DATA", "terminal evaluation could not be immutably persisted")
        code = "NOT_READY" if runtime.status == "BLOCKED" else "SOURCE_UNAVAILABLE"
        raise PipelineError(code, "terminal evaluation is unavailable")
    response = decision_projection(runtime.evaluation)
    response["diagnostics"] = {
        "blockers": list(runtime.diagnostics.blockers),
        "reasons": list(runtime.diagnostics.reasons),
    }
    return response


__all__ = ["evaluate_terminal_request"]

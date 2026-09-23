"""Operate the read-only Terminal Tournament Strategy from the command line.

Prerequisites:

* ``status``, ``decision``, ``candidates`` and ``history`` require an existing
  research run and hash-verified terminal evaluation artifacts as applicable.
* ``evaluate`` requires S4/S5 plus a saved, verified realtime observation.
  It reads a verified imported Ninef account when available and otherwise uses
  the read-only Ninef fallback.
* ``sync-inputs`` reads the current Ninef account, fee schedule and complete
  order receipt into the run's content-addressed execution-input store.
* ``plan`` additionally requires a complete terminal evaluation, the latest
  realtime and execution-status observations, and complete imported Ninef
  account/fee inputs.  It creates a shadow portfolio attempt only.

No command approves, executes, submits, cancels, or force-enables a trade.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from competition_core.pipeline.errors import PipelineError
from competition_core.research_tasks import ResearchTasks, checked_directory
from competition_core.terminal_strategy.api_views import (
    load_persisted,
    persisted_candidates_projection,
    persisted_decision_projection,
    persisted_history,
    status_projection,
)


_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


class _CLIUsageError(ValueError):
    pass


class _Parser(argparse.ArgumentParser):
    """Make parse failures follow the same JSON stderr contract as runtime errors."""

    def error(self, message):
        raise _CLIUsageError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=ROOT / "runs",
        help="ResearchTasks runs directory (default: research/runs)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def add_command_runs_root(command):
        # Accept the documented global option after the command as well.  The
        # subparser default is suppressed so a value supplied before the
        # command is not overwritten by argparse.
        command.add_argument(
            "--runs-root",
            type=Path,
            default=argparse.SUPPRESS,
            help=argparse.SUPPRESS,
        )

    status = commands.add_parser("status", help="show terminal status globally or for one run")
    add_command_runs_root(status)
    status.add_argument("run_id", nargs="?", help="optional research run identifier")

    sync_inputs = commands.add_parser(
        "sync-inputs", help="import the verified read-only Ninef account inputs"
    )
    add_command_runs_root(sync_inputs)
    sync_inputs.add_argument("run_id")

    evaluate = commands.add_parser("evaluate", help="evaluate server-owned terminal facts")
    add_command_runs_root(evaluate)
    evaluate.add_argument("run_id")

    decision = commands.add_parser("decision", help="show a persisted terminal decision")
    add_command_runs_root(decision)
    decision.add_argument("run_id")
    # Validation is centralized in the hash-verified projection loader.
    decision.add_argument("--evaluation-id")

    candidates = commands.add_parser("candidates", help="show one persisted candidate tier")
    add_command_runs_root(candidates)
    candidates.add_argument("run_id")
    candidates.add_argument("--tier", choices=("watch", "focus", "terminal"), required=True)
    candidates.add_argument("--evaluation-id")

    history = commands.add_parser("history", help="show persisted terminal evaluation history")
    add_command_runs_root(history)
    history.add_argument("run_id")
    history.add_argument("--limit", type=int, default=20)

    plan = commands.add_parser("plan", help="create one server-bound terminal shadow plan")
    add_command_runs_root(plan)
    plan.add_argument("run_id")
    plan.add_argument("--evaluation-id")

    return parser


def _public_error(code: str, *, usage: bool = False) -> dict[str, Any]:
    messages = {
        "INVALID_ARGUMENTS": "invalid terminal strategy input",
        "NOT_FOUND": "research run or terminal evaluation was not found",
        "NOT_READY": "terminal strategy prerequisites are not ready",
        "STALE_DATA": "terminal strategy evidence is stale or invalid",
        "TASK_BUSY": "research run is busy",
        "SOURCE_UNAVAILABLE": "terminal strategy source is unavailable",
        "NOT_CONFIGURED": "terminal strategy source is not configured",
        "LIMIT_EXCEEDED": "terminal strategy request exceeds its limit",
        "CLI_USAGE": "invalid terminal strategy command",
    }
    safe_code = code if isinstance(code, str) and _SAFE_CODE.fullmatch(code) else "TERMINAL_CLI_FAILED"
    return {
        "ok": False,
        "error": {
            "code": safe_code,
            "message": messages.get(safe_code, "terminal strategy command could not be completed"),
            "details": {},
        },
    }


def _emit_error(code: str, *, stream=None) -> int:
    stream = stream or sys.stderr
    stream.write(json.dumps(_public_error(code), ensure_ascii=False, sort_keys=True) + "\n")
    return 2 if code in {"INVALID_ARGUMENTS", "CLI_USAGE"} else 1


def _run_id(value: str) -> str:
    if not isinstance(value, str) or _RUN_ID.fullmatch(value) is None:
        raise PipelineError("INVALID_ARGUMENTS", "invalid research task identifier")
    return value


def _runs_root(value: Path) -> Path:
    """Resolve the configured ResearchTasks root without accepting traversal."""

    if not isinstance(value, Path):
        raise PipelineError("INVALID_ARGUMENTS", "invalid runs root")
    try:
        root = value.expanduser().resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise PipelineError("INVALID_ARGUMENTS", "invalid runs root") from exc
    if root == root.parent:
        raise PipelineError("INVALID_ARGUMENTS", "invalid runs root")
    return root


def _tasks(root: Path) -> ResearchTasks:
    return ResearchTasks(root)


def _read_status(root: Path, run_id: str | None) -> dict[str, Any]:
    if run_id is None:
        return status_projection(root)
    run_id = _run_id(run_id)
    directory = checked_directory(root, run_id, existing=False)
    if not directory.is_dir() or directory.is_symlink():
        raise PipelineError("NOT_FOUND", "research run does not exist")
    result = status_projection(root, [run_id])
    return result


def _read_decision(root: Path, run_id: str, evaluation_id: str | None) -> dict[str, Any]:
    return persisted_decision_projection(load_persisted(root, _run_id(run_id), evaluation_id))


def _read_candidates(root: Path, run_id: str, tier: str, evaluation_id: str | None) -> dict[str, Any]:
    return persisted_candidates_projection(
        load_persisted(root, _run_id(run_id), evaluation_id), tier
    )


def _read_history(root: Path, run_id: str, limit: int) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise PipelineError("INVALID_ARGUMENTS", "history limit is invalid")
    run_id = _run_id(run_id)
    return {
        "schema_version": "competition.terminal-history-view.v1",
        "run_id": run_id,
        "history": persisted_history(root, run_id, limit),
        "execution_ready": False,
        "orders_submitted": 0,
    }


def _evaluate(root: Path, run_id: str) -> dict[str, Any]:
    from competition_core.research_api import get_ninef_account_view, get_ninef_leaderboard_view
    from competition_core.terminal_strategy.orchestration import evaluate_terminal_request

    return evaluate_terminal_request(
        tasks=_tasks(root),
        run_id=_run_id(run_id),
        account_reader=get_ninef_account_view,
        leaderboard_reader=get_ninef_leaderboard_view,
    )


def _sync_inputs(root: Path, run_id: str) -> dict[str, Any]:
    """Import only server-read Ninef execution inputs and return a bounded view."""

    result = _tasks(root).import_ninef_execution_inputs(_run_id(run_id))
    return {
        key: result[key]
        for key in (
            "run_id",
            "input_id",
            "source_kind",
            "as_of",
            "provider",
            "position_count",
            "pending_order_count",
            "execution_ready",
            "orders_submitted",
        )
    }


def _plan(root: Path, run_id: str, evaluation_id: str | None) -> dict[str, Any]:
    from competition_core.research_portfolio import create_terminal_shadow_plan

    return create_terminal_shadow_plan(
        tasks=_tasks(root),
        run_id=_run_id(run_id),
        evaluation_id=evaluation_id,
    )


def dispatch(args: argparse.Namespace) -> dict[str, Any]:
    root = _runs_root(args.runs_root)
    if args.command == "status":
        return _read_status(root, args.run_id)
    if args.command == "sync-inputs":
        return _sync_inputs(root, args.run_id)
    if args.command == "evaluate":
        return _evaluate(root, args.run_id)
    if args.command == "decision":
        return _read_decision(root, args.run_id, args.evaluation_id)
    if args.command == "candidates":
        return _read_candidates(root, args.run_id, args.tier, args.evaluation_id)
    if args.command == "history":
        return _read_history(root, args.run_id, args.limit)
    if args.command == "plan":
        return _plan(root, args.run_id, args.evaluation_id)
    raise PipelineError("INVALID_ARGUMENTS", "unknown terminal strategy command")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        payload = dispatch(args)
    except _CLIUsageError:
        return _emit_error("CLI_USAGE")
    except PipelineError as exc:
        return _emit_error(exc.code)
    except (OSError, RuntimeError, TypeError, ValueError, KeyError):
        # Provider and filesystem exception text is intentionally not exposed.
        return _emit_error("TERMINAL_CLI_FAILED")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

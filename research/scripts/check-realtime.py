"""Save a fresh, read-only realtime observation for a committed S5 research run."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.realtime import run_realtime_check
from competition_core.pipeline.account import load_account_snapshot
from competition_core.research_worker import _execution_inputs


_SAFE_MESSAGES = {
    "INVALID_ARGUMENTS": "invalid realtime check input",
    "NOT_FOUND": "research run does not exist",
    "NOT_READY": "realtime check is not ready",
    "STALE_DATA": "realtime check input evidence is stale or invalid",
    "SOURCE_UNAVAILABLE": "realtime quote source is unavailable",
    "TASK_BUSY": "research run is busy",
}


def _imported_account(repository):
    """Use a run's verified imported account when its pointer exists.

    Older non-terminal runs may not have execution inputs yet, so a completely
    absent pointer preserves the historical account=None behavior.  A present
    pointer is immutable evidence and must fail closed if its content or hash
    is invalid.
    """

    pointer = repository.directory / "execution-inputs.json"
    if pointer.is_symlink():
        raise PipelineError("STALE_DATA", "execution input pointer is invalid")
    if not pointer.exists():
        return None
    try:
        account, _, _ = _execution_inputs(repository.directory)
    except PipelineError:
        raise
    except (OSError, TypeError, ValueError, KeyError) as exc:
        raise PipelineError("STALE_DATA", "execution input account is invalid") from exc
    if account is None:
        raise PipelineError("STALE_DATA", "execution input account is missing")
    return account


def _safe_error(exc):
    if isinstance(exc, PipelineError):
        code = exc.code if isinstance(exc.code, str) else "REALTIME_CHECK_FAILED"
        return {"code": code, "message": _SAFE_MESSAGES.get(code, "realtime check failed"), "details": {}}
    return {"code": "REALTIME_CHECK_FAILED", "message": "realtime check failed", "details": {}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--runs-root", type=Path, default=ROOT / "runs")
    parser.add_argument("--check-id")
    parser.add_argument("--runtime-url")
    parser.add_argument("--account-file", type=Path, help="Explicit account export; includes held instruments in the quote batch")
    args = parser.parse_args()
    try:
        repository = PipelineRepository(args.runs_root, args.run_id)
        if not (repository.directory / "run.json").is_file():
            raise PipelineError("NOT_READY", "research run does not exist")
        account = (load_account_snapshot(args.account_file)[0] if args.account_file
                   else _imported_account(repository))
        result = run_realtime_check(repository=repository,
                                    check_id=args.check_id, endpoint=args.runtime_url, account=account)
        print(json.dumps({key: result[key] for key in ("run_id", "check_id", "source_error",
                         "ai_and_quote_eligible", "enough_candidates_for_portfolio", "execution_ready")}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": _safe_error(exc)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

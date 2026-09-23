"""Fetch current listing/calendar/suspension/exact-limit references for a saved S5 run."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.tushare.client import configured_tushare_client
from competition_core.pipeline.account import load_account_snapshot
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status_runner import run_execution_status_check
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.research_worker import _execution_inputs


_SAFE_MESSAGES = {
    "INVALID_ARGUMENTS": "invalid execution status input",
    "NOT_FOUND": "research run does not exist",
    "NOT_READY": "execution status is not ready",
    "STALE_DATA": "execution status input evidence is stale or invalid",
    "SOURCE_UNAVAILABLE": "execution status source is unavailable",
    "TASK_BUSY": "research run is busy",
}


def _imported_account(repository):
    """Use verified imported account inputs when the run declares a pointer."""

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
        code = exc.code if isinstance(exc.code, str) else "EXECUTION_REFERENCE_FAILED"
        return {"code": code, "message": _SAFE_MESSAGES.get(code, "execution status check failed"), "details": {}}
    return {"code": "EXECUTION_REFERENCE_FAILED", "message": "execution status check failed", "details": {}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--runs-root", type=Path, default=ROOT / "runs")
    parser.add_argument("--status-id")
    parser.add_argument("--account-file", type=Path)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--source-label", default="tushare.configured")
    args = parser.parse_args()
    try:
        repository = PipelineRepository(args.runs_root, args.run_id)
        if not (repository.directory / "run.json").is_file():
            raise PipelineError("NOT_READY", "research run does not exist")
        account = (load_account_snapshot(args.account_file)[0] if args.account_file
                   else _imported_account(repository))
        result = run_execution_status_check(repository=repository,
            client=configured_tushare_client(token_file=args.token_file, base_url=args.base_url),
            account=account, status_id=args.status_id, source_name=args.source_label)
        print(json.dumps({key: result[key] for key in ("run_id", "status_id", "status", "summary", "execution_ready")}, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": _safe_error(exc)}, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--status-id")
    parser.add_argument("--account-file", type=Path)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--source-label", default="tushare.configured")
    args = parser.parse_args()
    try:
        repository = PipelineRepository(ROOT / "runs", args.run_id)
        if not (repository.directory / "run.json").is_file():
            raise PipelineError("NOT_READY", "research run does not exist")
        account = load_account_snapshot(args.account_file)[0] if args.account_file else None
        result = run_execution_status_check(repository=repository,
            client=configured_tushare_client(token_file=args.token_file, base_url=args.base_url),
            account=account, status_id=args.status_id, source_name=args.source_label)
        print(json.dumps({key: result[key] for key in ("run_id", "status_id", "status", "summary", "execution_ready")}, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict() if isinstance(exc, PipelineError) else
                          {"code": "EXECUTION_REFERENCE_FAILED", "exception_type": type(exc).__name__}}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

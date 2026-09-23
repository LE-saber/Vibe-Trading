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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--check-id")
    parser.add_argument("--runtime-url")
    parser.add_argument("--account-file", type=Path, help="Explicit account export; includes held instruments in the quote batch")
    args = parser.parse_args()
    try:
        repository = PipelineRepository(ROOT / "runs", args.run_id)
        if not (repository.directory / "run.json").is_file():
            raise PipelineError("NOT_READY", "research run does not exist")
        account = load_account_snapshot(args.account_file)[0] if args.account_file else None
        result = run_realtime_check(repository=repository,
                                    check_id=args.check_id, endpoint=args.runtime_url, account=account)
        print(json.dumps({key: result[key] for key in ("run_id", "check_id", "source_error",
                         "ai_and_quote_eligible", "enough_candidates_for_portfolio", "execution_ready")}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict() if isinstance(exc, PipelineError) else
                          {"code": "REALTIME_CHECK_FAILED", "exception_type": type(exc).__name__}}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

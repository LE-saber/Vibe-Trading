"""Build an audited manual-confirmation draft from a saved S5 run and quote check."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.account import load_account_snapshot
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.portfolio_runner import run_portfolio_draft


def read_input(path):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate input key")
            result[key] = value
        return result
    def nonfinite(_):
        raise ValueError("nonfinite input")
    with path.open("rb") as handle:
        body = handle.read(2 * 1024 * 1024 + 1)
    if len(body) > 2 * 1024 * 1024:
        raise ValueError("input too large")
    return json.loads(body.decode("utf-8-sig"), object_pairs_hook=unique, parse_constant=nonfinite)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--check-id", required=True)
    parser.add_argument("--draft-id")
    parser.add_argument("--account-file", type=Path)
    parser.add_argument("--fee-file", type=Path)
    status_group = parser.add_mutually_exclusive_group()
    status_group.add_argument("--trading-metadata-file", type=Path)
    status_group.add_argument("--status-id", help="Fresh source-bound execution-status observation in this run")
    args = parser.parse_args()
    try:
        repository = PipelineRepository(ROOT / "runs", args.run_id)
        if not (repository.directory / "run.json").is_file():
            raise PipelineError("NOT_READY", "research run does not exist")
        account, source_hash = load_account_snapshot(args.account_file) if args.account_file else (None, None)
        fees = FeeSchedule.from_dict(read_input(args.fee_file)) if args.fee_file else None
        metadata = read_input(args.trading_metadata_file) if args.trading_metadata_file else None
        result = run_portfolio_draft(repository=repository, check_id=args.check_id, draft_id=args.draft_id,
            account=account, account_source_hash=source_hash, fees=fees, trading_metadata=metadata, status_id=args.status_id)
        print(json.dumps({key: result[key] for key in ("portfolio_status", "output_symbols", "execution_ready", "production_accepted")}))
        if result["portfolio_status"] == "NOT_READY":
            raise SystemExit(2)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict() if isinstance(exc, PipelineError) else
                          {"code": "PORTFOLIO_DRAFT_FAILED", "exception_type": type(exc).__name__}}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

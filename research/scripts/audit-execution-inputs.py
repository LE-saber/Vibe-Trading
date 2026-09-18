"""Audit saved execution inputs offline; --test-mode tolerates account age only."""

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.account import load_account_snapshot
from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.input_audit import audit_execution_inputs
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI


def read_input(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate input key")
            result[key] = value
        return result

    def nonfinite(_):
        raise ValueError("nonfinite input")

    with path.open("rb") as handle:
        body = handle.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise ValueError("input too large")
    return json.loads(body.decode("utf-8-sig"), object_pairs_hook=unique, parse_constant=nonfinite)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-file", type=Path, required=True)
    parser.add_argument("--fee-file", type=Path)
    parser.add_argument("--quotes-file", type=Path)
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--research-date", type=date.fromisoformat, required=True)
    parser.add_argument("--market-data-reference-date", type=date.fromisoformat, required=True)
    parser.add_argument("--phase", choices=["preopen", "intraday", "postclose"], required=True)
    parser.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    parser.add_argument("--test-mode", action="store_true")
    args = parser.parse_args()
    try:
        account, source_hash = load_account_snapshot(args.account_file)
        fees = FeeSchedule.from_dict(read_input(args.fee_file)) if args.fee_file else None
        context = ResearchContext(args.research_date, args.phase, args.market_data_reference_date, args.as_of)
        result = audit_execution_inputs(account=account, fees=fees, context=context,
            config=load_pipeline_config(), observed_at=datetime.now(SHANGHAI), test_mode=args.test_mode,
            quotes=read_input(args.quotes_file) if args.quotes_file else None,
            symbols=read_input(args.symbols_file) if args.symbols_file else None)
        result["account_source_sha256"] = source_hash
        print(json.dumps(result, ensure_ascii=True, allow_nan=False))
    except Exception as exc:
        print(json.dumps(dict(validation_completed=False, error_code="INPUT_AUDIT_FAILED",
            exception_type=type(exc).__name__, execution_ready=False, production_accepted=False,
            orders_submitted=0)))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

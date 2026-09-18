"""Read-only source probe on historical S4 symbols, NOT current research/portfolio acceptance."""

import argparse
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.tushare.client import configured_tushare_client
from competition_core.pipeline.artifacts import atomic_json, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status import collect_execution_status
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortlist-run", required=True)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--audit-id", required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--source-label", default="tushare.configured")
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", args.audit_id):
            raise PipelineError("INVALID_ARGUMENTS", "invalid probe identifier")
        directory = ROOT / "storage/artifacts/pipeline-engineering" / args.audit_id
        if directory.exists():
            raise PipelineError("NOT_READY", "probe ID already exists")
        repo = PipelineRepository(ROOT / "runs", args.shortlist_run)
        s4, s0 = repo.load_stage("S4"), repo.load_stage("S0")
        if s4 is None or s0 is None or s4["status"] != "SUCCESS":
            raise PipelineError("NOT_READY", "saved S4 source sample unavailable")
        started = datetime.now(SHANGHAI)
        if started.date() != args.date:
            raise PipelineError("NOT_READY", "source probe date must be the actual current date")
        client = configured_tushare_client(token_file=args.token_file, base_url=args.base_url)
        # Resolve history reference from an actual calendar, not date-minus-one.
        params = {"exchange": "SSE", "start_date": (args.date - timedelta(days=30)).strftime("%Y%m%d"),
                  "end_date": args.date.strftime("%Y%m%d"), "fields": "exchange,cal_date,is_open,pretrade_date"}
        calendar = client.trade_cal(**params)
        received = datetime.now(SHANGHAI)
        ctx = ResearchContext.resolve(research_date=args.date, phase="intraday", as_of=received,
                                       calendar=calendar, observed_at=received)
        from contracts.symbol import CanonicalSymbol
        rules = {}
        for row in s0["rows"]:
            value = row.get("identity", {}).get("new_listing_special_sessions")
            if type(value) is int:
                board = CanonicalSymbol.from_core(row["symbol"]).board
                if board in rules and rules[board] != value:
                    raise PipelineError("STALE_DATA", "inconsistent saved board metadata")
                rules[board] = value
        result = collect_execution_status(client=client, symbols=s4["output_symbols"], context=ctx,
            special_sessions_by_board=rules, directory=directory, source_name=args.source_label)
        atomic_json(directory / "bootstrap-calendar.json", {"parameters": params, "received_at": received.isoformat(),
            "rows": json.loads(calendar.reindex(columns=params["fields"].split(",")).to_json(orient="records"))}, immutable=True)
        report = {"scope": "execution_reference_probe_not_current_selection_or_portfolio", "source_sample_run": args.shortlist_run,
            "source_sample_s4_hash": s4["artifact_hash"], "research_date": args.date.isoformat(),
            "market_data_reference_date": ctx.market_data_reference_date.isoformat(), "summary": result["summary"],
            "status": result["status"], "observed_at": result["observed_at"],
            "observation_hash": file_hash(directory / "observation.json"), "execution_ready": False,
            "production_pipeline_status": "NOT_ACCEPTED", "historical_research_stages_modified": False}
        atomic_json(directory / "probe-report.json", report, immutable=True)
        print(json.dumps(report, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict() if isinstance(exc, PipelineError) else
                          {"code": "SOURCE_PROBE_FAILED", "exception_type": type(exc).__name__}}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

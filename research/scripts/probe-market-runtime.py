"""Read-only transport probe; no research-date promotion or execution acceptance."""

import argparse
from collections import Counter
from datetime import datetime
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.market_runtime.quotes import fetch_quotes, runtime_origin
from competition_core.pipeline.artifacts import atomic_json, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.time_context import SHANGHAI


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortlist-run", required=True)
    parser.add_argument("--audit-id", required=True)
    parser.add_argument("--runtime-url")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", args.audit_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid audit identifier")
    repository = PipelineRepository(ROOT / "runs", args.shortlist_run)
    s4 = repository.load_stage("S4")
    if s4 is None or s4["status"] != "SUCCESS":
        raise PipelineError("NOT_READY", "probe needs a saved S4 symbol set, not model-created symbols")
    directory = ROOT / "storage/artifacts/pipeline-engineering" / args.audit_id
    if directory.exists():
        raise PipelineError("NOT_READY", "audit identifier already used")
    origin = runtime_origin(args.runtime_url)
    request = {"symbols": s4["output_symbols"], "endpoint": origin,
               "historical_shortlist_run": args.shortlist_run, "s4_hash": s4["artifact_hash"],
               "observed_at": datetime.now(SHANGHAI).isoformat(), "purpose": "transport_only_not_current_selection"}
    atomic_json(directory / "request.json", request, immutable=True)
    try:
        response = fetch_quotes(list(s4["output_symbols"]), endpoint=origin)
        atomic_json(directory / "response.json", response, immutable=True)
        rows = response["response"]["quotes"]
        report = {"transport_status": "RESPONSE_RECEIVED", "requested_count": len(s4["output_symbols"]),
                  "returned_count": len(rows), "timestamp_quality_counts": dict(Counter(row.get("timestamp_quality") for row in rows)),
                  "missing_trade_dates": sum(row.get("trade_date") is None for row in rows),
                  "observed_at": datetime.now(SHANGHAI).isoformat(),
                  "response_hash": file_hash(directory / "response.json"), "execution_ready": False,
                  "production_pipeline_status": "NOT_ACCEPTED", "scope": "transport_probe_not_intraday_acceptance"}
    except Exception as exc:
        report = {"transport_status": "FAILED", "error_type": type(exc).__name__, "execution_ready": False,
                  "production_pipeline_status": "NOT_ACCEPTED", "observed_at": datetime.now(SHANGHAI).isoformat()}
    atomic_json(directory / "report.json", report, immutable=True)
    print(report)
    print(directory / "report.json")


if __name__ == "__main__":
    main()

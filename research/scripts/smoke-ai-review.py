"""One real, explicitly recorded non-tool call before freezing a batch review."""

import argparse
from datetime import date, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.vibe.review import effective_runtime, invoke_review, probe_vibe
from competition_core.pipeline.artifacts import atomic_json, file_hash, read_json
from competition_core.pipeline.config import PipelineConfig, load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.research_packet import build_research_packet
from competition_core.pipeline.reviews import _fingerprint, _validate_payload
from competition_core.pipeline.time_context import ResearchContext
from competition_core.storage.evidence import EvidenceRepository


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--audit-id", required=True)
    parser.add_argument("--config", type=Path, help="Explicit prototype review policy; never rewrites the saved S4 config")
    args = parser.parse_args()
    repo = PipelineRepository(ROOT / "runs", args.run_id)
    directory = PipelineRepository(ROOT / "storage/artifacts/pipeline-engineering", args.audit_id).directory
    if any(directory.iterdir()):
        raise PipelineError("NOT_READY", "smoke audit ID is already used; do not repeat a possibly billed request")
    try:
        inputs, stage = read_json(repo.directory / "inputs.json"), repo.load_stage("S4")
        if args.symbol not in stage["output_symbols"]:
            raise PipelineError("INVALID_ARGUMENTS", "smoke symbol must be one of the saved S4 finalists")
        candidate = next(row for row in stage["rows"] if row["symbol"] == args.symbol)
        bundle = EvidenceRepository().get(candidate["evidence_id"])
        data = inputs["context"]
        context = ResearchContext(date.fromisoformat(data["research_date"]), data["phase"],
            date.fromisoformat(data["market_data_reference_date"]), datetime.fromisoformat(data["as_of"]))
        config = load_pipeline_config(args.config) if args.config else PipelineConfig.from_dict(inputs["config"])
        vibe = Path(inputs["vibe_root"])
        packet = build_research_packet(candidate=candidate, bundle=bundle, context=context, config=config)
        atomic_json(directory / "packet.json", packet, immutable=True)
        fingerprint = _fingerprint(vibe)
        runtime = effective_runtime(probe_vibe(directory=directory / "preflight", vibe_root=vibe), config.section("ai"))
        payload = invoke_review(packet=packet, runtime=runtime, directory=directory / "attempt-1", vibe_root=vibe,
            timeout_seconds=config.section("ai")["timeout_seconds"], packet_max_chars=config.section("ai")["packet_max_chars"],
            reasoning_effort=config.section("ai")["reasoning_effort"], max_output_tokens=config.section("ai")["max_output_tokens"])
        validated = _validate_payload(payload, packet, runtime)
        if fingerprint != _fingerprint(vibe):
            raise PipelineError("STALE_DATA", "review code changed during real smoke call")
        report = {"status": "PASS", "production_pipeline_status": "NOT_ACCEPTED", "run_id": args.run_id, "symbol": args.symbol,
                  "packet_chars": packet["packet_chars"], "packet_hash": packet["packet_hash"], "runtime": runtime,
                  "review_config_hash": config.config_hash, "screening_config_hash": inputs["config_hash"],
                  "llm_calls": 1, "agent_tool_calls": 0, "review": validated, "code_hashes": fingerprint,
                  "raw_response_hash": file_hash(directory / "attempt-1/response.json")}
        atomic_json(directory / "report.json", report, immutable=True)
        print({key: report[key] for key in ("status", "symbol", "packet_chars", "llm_calls", "review")}, flush=True)
    except Exception as exc:
        error = exc.to_dict() if isinstance(exc, PipelineError) else {"code": "AI_REVIEW_FAILED", "exception_type": type(exc).__name__}
        atomic_json(directory / "failure.json", {"status": "NOT_ACCEPTED", "error": error}, immutable=True)
        print({"status": "NOT_ACCEPTED", "error": error}, flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

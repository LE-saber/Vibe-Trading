"""Compare two genuine stage recalculations; never promote a partial run to E2E PASS."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.persistence import PipelineRepository


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("first")
    parser.add_argument("second")
    parser.add_argument("--runs-root", type=Path, default=ROOT / "runs")
    parser.add_argument("--through-stage", choices=("S2", "S3", "S4"), default="S2")
    args = parser.parse_args()
    repositories = [PipelineRepository(args.runs_root, name) for name in (args.first, args.second)]
    inputs = [read_json(repo.directory / "inputs.json") for repo in repositories]
    same_inputs = all(inputs[0][key] == inputs[1][key] for key in
                      ("snapshot_id", "config_hash", "context", "manifest_hash", "code_hashes"))
    stages = []
    stage_names = ("S0", "S1", "S2", "S3", "S4")
    for stage in stage_names[:stage_names.index(args.through_stage) + 1]:
        a, b = [repo.load_stage(stage) for repo in repositories]
        assert a is not None and b is not None
        same = (a["semantic_hash"] == b["semantic_hash"] and a["rows"] == b["rows"]
                and a["input_symbols"] == b["input_symbols"] and a["output_symbols"] == b["output_symbols"])
        stages.append({"stage": stage, "input_count": len(a["input_symbols"]), "output_count": len(a["output_symbols"]),
                       "exact_match": same, "semantic_hash": a["semantic_hash"], "summary": a["summary"],
                       "non_rank_rejections": dict(Counter(reason for row in a["rows"] for reason in row["reject_reasons"] if not reason.startswith("rank_below"))),
                       "artifacts": [{"path": str(repo.directory / f"{stage}.json"), "sha256": file_hash(repo.directory / f"{stage}.json"),
                                      "parquet_sha256": value["parquet_hash"], "elapsed_seconds": value["elapsed_seconds"]}
                                     for repo, value in zip(repositories, (a, b))]})
    feature_file = read_json(repositories[0].directory / "features.json")
    values = feature_file["rows"]
    technical_diagnostic = {
        "macd_missing": sum(row["macd_histogram"] is None for row in values),
        "macd_negative": sum(row["macd_histogram"] is not None and row["macd_histogram"] < 0 for row in values),
        "macd_zero": sum(row["macd_histogram"] == 0 for row in values),
        "rsi_zero": sum(row["rsi_14"] == 0 for row in values),
        "missing_reasons": dict(Counter(reason for row in feature_file["technical_provenance"]["missing_reasons"].values() for reason in row.values())),
    }
    config = inputs[0]["config"]
    expected_counts = stages[1]["output_count"] == config["stages"]["s1_top_n"] and stages[2]["output_count"] == config["stages"]["s2_top_n"]
    if args.through_stage in {"S3", "S4"}:
        expected_counts = expected_counts and stages[3]["output_count"] == config["stages"]["s3_top_n"]
    if args.through_stage == "S4":
        expected_counts = expected_counts and 10 <= stages[4]["output_count"] <= 15
    no_llm = all(item["summary"].get("llm_calls") == 0 and item["summary"].get("agent_tool_calls") == 0 for item in stages)
    passed = same_inputs and expected_counts and no_llm and all(item["exact_match"] for item in stages)
    gate = {"S2": "P10C", "S3": "P10D", "S4": "P10E"}[args.through_stage]
    report = {"schema_version": "p10.screening.audit.v2", "gate": gate, "status": "PASS" if passed else "NOT_ACCEPTED",
              "production_pipeline_status": "NOT_ACCEPTED", "execution_ready": False,
              "first_run": args.first, "second_run": args.second, "same_inputs": same_inputs,
              "snapshot_id": inputs[0]["snapshot_id"], "config_hash": inputs[0]["config_hash"], "context": inputs[0]["context"],
              "stages": stages, "technical_diagnostic": technical_diagnostic,
              "caveats": ["off_hours_historical_screening_only", "no_ai_realtime_or_account_acceptance",
                          "engineered_baseline_not_validated_alpha_profitability", "frozen_real_sources_not_refetched_for_replay"]}
    report["report_id"] = content_hash(report)
    directory = ROOT / "storage/artifacts/pipeline-engineering"
    stem = f"{gate.lower()}-{args.first}-{args.second}"
    atomic_json(directory / f"{stem}.json", report, immutable=True)
    text = [f"# {gate} screening gate: {report['status']}", "", "Production pipeline: NOT_ACCEPTED; execution_ready=false.", "",
            f"Runs: `{args.first}` / `{args.second}`", f"Snapshot: `{report['snapshot_id']}`", f"Config: `{report['config_hash']}`", "",
            "| Stage | Input | Output | Exact replay | First elapsed (s) |", "|---|---:|---:|---|---:|"]
    text.extend(f"| {item['stage']} | {item['input_count']} | {item['output_count']} | {item['exact_match']} | {item['artifacts'][0]['elapsed_seconds']:.3f} |" for item in stages)
    text += ["", f"Zero LLM/Agent Tool calls: {no_llm}. The independent replay recomputes all stages using frozen real inputs.",
             f"MACD negative/zero/missing: {technical_diagnostic['macd_negative']}/{technical_diagnostic['macd_zero']}/{technical_diagnostic['macd_missing']}.",
             "Missing history, feature confidence and sector-data gaps remain explicitly reported in each stage JSON.",
             "", "This is a deterministic engineering baseline, not a validated profit forecast or an executable portfolio.",
             "All stage paths/hashes and detailed missing/rejection counts are in the sibling JSON."]
    atomic_bytes(directory / f"{stem}.md", ("\n".join(text) + "\n").encode("utf-8"), immutable=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for name, root in (("core", ROOT), ("vibe", ROOT.parent / "Vibe-Trading")):
        status = subprocess.run(["git", "status", "--short"], cwd=root, capture_output=True, check=True)
        atomic_bytes(directory / f"{gate.lower()}-{name}-status-{timestamp}.txt", status.stdout, immutable=True)
    print({"status": report["status"], "report": str(directory / f"{stem}.json"), "technical": technical_diagnostic}, flush=True)
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()

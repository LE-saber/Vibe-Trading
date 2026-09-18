"""Kill our own real S3 worker mid-stage, then resume strictly from frozen sources."""

import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, file_hash, read_json
from competition_core.pipeline.persistence import PipelineRepository


def execute(command, *, interrupt_after=None):
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding="utf-8", errors="replace",
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    lines, interrupted = [], False
    import json
    try:
        for line in process.stdout:
            lines.append(line)
            print(line.rstrip(), flush=True)
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if interrupt_after is not None and event.get("evidence_completed") == interrupt_after:
                process.kill()  # Only the process created above; no PID discovery/globs.
                interrupted = True
                break
        tail, _ = process.communicate(timeout=30)
        if tail:
            lines.append(tail)
        return process.returncode, interrupted, "".join(lines)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--interrupt-after", type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.interrupt_after < 60:
        parser.error("interrupt-after must be between1 and59 for the defaultTop60 gate")
    repository = PipelineRepository(ROOT / "runs", args.run_id)
    if (repository.directory / "inputs.json").exists():
        parser.error("fault audit requires a new run_id; existing runs are never replaced")
    command = [sys.executable, str(ROOT / "scripts/run-research.py"), "--through-stage", "S3", "--run-id", args.run_id,
               "--manifest", str(args.manifest.resolve()), "--snapshot", str(args.snapshot.resolve())]
    if args.token_file:
        command += ["--token-file", str(args.token_file.resolve())]
    if args.base_url:
        command += ["--base-url", args.base_url]
    code, interrupted, initial_log = execute(command, interrupt_after=args.interrupt_after)
    atomic_bytes(repository.directory / "logs/s3-interruption.log", initial_log.encode("utf-8"), immutable=True)
    before_state = repository.state()
    before_checkpoints = {path.name: file_hash(path) for path in (repository.directory / "evidence").glob("*.checkpoint.json")}
    assert interrupted and 0 < len(before_checkpoints) < 60 and not (repository.directory / "S3.json").exists(), "worker did not stop mid-S3"
    before_stages = {stage: file_hash(repository.directory / f"{stage}.json") for stage in ("S0", "S1", "S2")}
    before_sources = {path.name: file_hash(path) for path in (repository.directory / "sources").glob("*.json")}
    resume = [sys.executable, str(ROOT / "scripts/run-research.py"), "--through-stage", "S3", "--resume", args.run_id, "--frozen-sources-only"]
    resume_code, _, resume_log = execute(resume)
    atomic_bytes(repository.directory / "logs/s3-resume.log", resume_log.encode("utf-8"), immutable=True)
    assert resume_code == 0, "strict frozen-source resume failed"
    stage = repository.load_stage("S3")
    checkpoints = {path.name: file_hash(path) for path in (repository.directory / "evidence").glob("*.checkpoint.json")}
    checks = {
        "actual_process_killed_mid_s3": interrupted and code != 0,
        "s0_s1_s2_unchanged": before_stages == {stage: file_hash(repository.directory / f"{stage}.json") for stage in before_stages},
        "completed_evidence_unchanged": all(checkpoints.get(name) == digest for name, digest in before_checkpoints.items()),
        "frozen_sources_unchanged_no_network_allowed": before_sources == {path.name: file_hash(path) for path in (repository.directory / "sources").glob("*.json")},
        "all_sixty_evidence_complete": len(checkpoints) == 60 and stage["summary"]["evidence_count"] == 60,
        "top_twenty_complete": len(stage["output_symbols"]) == 20,
    }
    report = {"gate": "P10D_REAL_S3_KILL_RESUME", "status": "PASS" if all(checks.values()) else "NOT_ACCEPTED",
              "production_pipeline_status": "NOT_ACCEPTED", "run_id": args.run_id,
              "interrupted_at": len(before_checkpoints), "interrupted_state": before_state,
              "checks": checks, "stage_hashes_before": before_stages,
              "source_hashes": before_sources, "evidence_hashes_before": before_checkpoints,
              "s3_artifact_hash": stage["artifact_hash"], "s3_semantic_hash": stage["semantic_hash"]}
    path = repository.directory / "s3-resume-audit.json"
    atomic_json(path, report, immutable=True)
    print({"status": report["status"], "report": str(path)}, flush=True)
    raise SystemExit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()

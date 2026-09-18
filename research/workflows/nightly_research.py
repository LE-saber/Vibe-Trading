"""Failure-isolated nightly data-quality and Qlib refresh orchestration."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from competition_core.storage.workflow import WorkflowRepository
from workflows.artifacts import write_json

CHINA = ZoneInfo("Asia/Shanghai")


def run_nightly(
    *,
    run_id: str,
    data_quality: Callable[[], dict],
    research_refresh: Callable[[], dict],
    artifact_root: str | Path,
    repository: WorkflowRepository,
) -> dict:
    started = datetime.now(CHINA)
    quality = data_quality()
    if quality.get("status") != "pass":
        raise RuntimeError("nightly data quality failed; research refresh was not run")
    fallback = False
    try:
        research = research_refresh()
    except Exception as exc:  # noqa: BLE001 - deterministic production path must continue
        fallback = True
        research = {"status": "unavailable", "error_type": type(exc).__name__}
    run_dir = Path(artifact_root).resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"workflow artifact already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    write_json(run_dir, "data_quality.json", quality)
    write_json(run_dir, "research.json", research)
    manifest = {
        "run_id": run_id,
        "phase": "nightly",
        "status": "complete",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(CHINA).isoformat(),
        "fallback_used": fallback,
    }
    write_json(run_dir, "run_manifest.json", manifest)
    repository.put_run(run_id, "nightly", "complete", fallback, str(run_dir), manifest)
    return manifest

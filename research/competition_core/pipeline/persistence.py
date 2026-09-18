"""Atomic stage checkpoints with an OS-released single-run writer lock."""

from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
import os
from pathlib import Path
import re
from typing import Any
import uuid

import pandas as pd

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, confined_path, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError

STAGES = ("S0", "S1", "S2", "S3", "S4", "S5", "S5_5", "S6")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()  # Operational timestamp only.


class PipelineRepository:
    def __init__(self, root: Path, run_id: str | None = None):
        self.run_id = run_id or f"research-{uuid.uuid4().hex[:20]}"
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", self.run_id):
            raise PipelineError("INVALID_ARGUMENTS", "invalid pipeline run_id")
        self.directory = Path(root).resolve() / self.run_id
        self.directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def lock(self):
        """OS locks disappear on process death, so kill/resume needs no stale PID guess."""
        descriptor = os.open(self.directory / ".writer.lock", os.O_RDWR | os.O_CREAT)
        acquired = False
        try:
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"0")
            os.lseek(descriptor, 0, os.SEEK_SET)
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                raise PipelineError("NOT_READY", "this pipeline run already has an active writer") from exc
            yield
        finally:
            if acquired:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def initialize(self, specification: dict[str, Any]) -> dict[str, Any]:
        frozen = {**specification, "specification_hash": content_hash(specification)}
        atomic_json(self.directory / "inputs.json", frozen, immutable=True)
        atomic_json(self.directory / "config.json", specification["config"], immutable=True)
        state_path = self.directory / "run.json"
        if not state_path.exists():
            timestamp = _now()
            atomic_json(state_path, {"run_id": self.run_id, **specification["context"],
                        "snapshot_id": specification["snapshot_id"], "config_version": specification["config"]["version"],
                        "config_hash": specification["config_hash"], "specification_hash": frozen["specification_hash"],
                        "status": "PENDING", "current_stage": None, "created_at": timestamp,
                        "updated_at": timestamp, "stages": {}, "production_accepted": False, "execution_ready": False})
        state = self.state()
        if state.get("specification_hash") != frozen["specification_hash"]:
            raise PipelineError("STALE_DATA", "run state differs from frozen inputs")
        return state

    def state(self) -> dict[str, Any]:
        return read_json(self.directory / "run.json")

    def update(self, **values: Any) -> dict[str, Any]:
        state = self.state()
        state.update(values, updated_at=_now())
        atomic_json(self.directory / "run.json", state)
        return state

    def start_stage(self, stage: str) -> None:
        self._validate_stage(stage)
        for previous in STAGES[:STAGES.index(stage)]:
            if self.load_stage(previous) is None:
                raise PipelineError("NOT_READY", "previous pipeline checkpoint is missing", stage=previous)
        self.update(status="RUNNING", current_stage=stage, error=None)

    def commit_stage(self, stage: str, result: dict[str, Any], *, metadata: dict[str, Any], elapsed: float) -> dict[str, Any]:
        self._validate_stage(stage)
        state = self.state()
        if state.get("status") != "RUNNING" or state.get("current_stage") != stage:
            raise PipelineError("NOT_READY", "stage must be started before committing its checkpoint")
        self._validate_result(stage, result)
        if set(result) & set(metadata):
            raise PipelineError("INVALID_ARGUMENTS", "stage metadata must not overwrite deterministic results")
        selected = set(result["output_symbols"])
        selected_rows = [row for row in result["rows"] if row["symbol"] in selected]
        order = {symbol: i for i, symbol in enumerate(result["output_symbols"])}
        selected_rows.sort(key=lambda row: order[row["symbol"]])
        # JSON is the full audit, Parquet is the selected set with portable
        # structured-cell JSON rather than an inferred, lossy nested schema.
        from competition_core.pipeline.artifacts import json_bytes
        table = pd.DataFrame([{key: json_bytes(value).decode("utf-8").strip() if isinstance(value, (dict, list)) else value
                               for key, value in row.items()} for row in selected_rows])
        stream = BytesIO()
        table.to_parquet(stream, index=False, engine="pyarrow")
        parquet = self.directory / f"{stage}.parquet"
        atomic_bytes(parquet, stream.getvalue(), immutable=True)
        envelope = {**result, **metadata, "semantic_hash": content_hash(result), "semantic_keys": sorted(result),
                    "elapsed_seconds": round(elapsed, 6), "parquet_path": parquet.name, "parquet_hash": file_hash(parquet)}
        envelope["artifact_hash"] = content_hash(envelope)
        atomic_json(self.directory / f"{stage}.json", envelope, immutable=True)
        self._record_checkpoint(stage, envelope)
        return envelope

    def load_stage(self, stage: str) -> dict[str, Any] | None:
        self._validate_stage(stage)
        path = self.directory / f"{stage}.json"
        if not path.exists():
            return None
        result = read_json(path)
        expected_hash = result.pop("artifact_hash", None)
        if content_hash(result) != expected_hash:
            raise PipelineError("STALE_DATA", "stage artifact hash mismatch", stage=stage)
        result["artifact_hash"] = expected_hash
        if result.get("stage") != stage or result.get("parquet_path") != f"{stage}.parquet":
            raise PipelineError("STALE_DATA", "stage artifact identity mismatch", stage=stage)
        semantic = {key: result[key] for key in result["semantic_keys"]}
        self._validate_result(stage, semantic)
        if content_hash(semantic) != result.get("semantic_hash"):
            raise PipelineError("STALE_DATA", "stage semantic hash mismatch", stage=stage)
        if file_hash(self.directory / result["parquet_path"]) != result.get("parquet_hash"):
            raise PipelineError("STALE_DATA", "stage Parquet hash mismatch", stage=stage)
        for reference in result.get("source_provenance", []):
            if "path" in reference and "sha256" in reference:
                if file_hash(confined_path(self.directory, reference["path"])) != reference["sha256"]:
                    raise PipelineError("STALE_DATA", "stage feature/source artifact hash mismatch", stage=stage)
            if "request_id" in reference:
                if not re.fullmatch(r"[0-9a-f]{64}", reference["request_id"]):
                    raise PipelineError("STALE_DATA", "invalid stage source reference", stage=stage)
                source = read_json(self.directory / "sources" / f"{reference['request_id']}.json")
                stored = source.pop("integrity_hash", None)
                if stored != reference["integrity_hash"] or stored != content_hash(source):
                    raise PipelineError("STALE_DATA", "stage source proof hash mismatch", stage=stage)
        return result

    def recover_checkpoint(self, stage: str) -> dict[str, Any] | None:
        result = self.load_stage(stage)
        if result is not None:
            self._record_checkpoint(stage, result)
        return result

    def _record_checkpoint(self, stage: str, result: dict[str, Any]) -> None:
        state = self.state()
        record = {key: result[key] for key in ("status", "semantic_hash", "artifact_hash", "elapsed_seconds")}
        record.update(output_count=len(result["output_symbols"]), artifact=f"{stage}.json")
        if state["stages"].get(stage) != record:
            state["stages"][stage] = record
            self.update(stages=state["stages"], current_stage=stage)

    @staticmethod
    def _validate_stage(stage: str) -> None:
        if stage not in STAGES:
            raise PipelineError("INVALID_ARGUMENTS", "unknown pipeline checkpoint")

    @staticmethod
    def _validate_result(stage: str, result: dict[str, Any]) -> None:
        inputs, outputs = result["input_symbols"], result["output_symbols"]
        rows = result["rows"]
        if (result.get("stage") != stage or result.get("status") not in {"SUCCESS", "PARTIAL"}
                or len(inputs) != len(set(inputs)) or len(outputs) != len(set(outputs))
                or not set(outputs) <= set(inputs) or len(rows) != len(inputs)
                or {row["symbol"] for row in rows} != set(inputs)
                or any(row.get("selected") is not (row["symbol"] in set(outputs)) for row in rows)):
            raise PipelineError("STALE_DATA", "stage input/output identities or selection flags are inconsistent", stage=stage)

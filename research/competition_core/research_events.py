"""Append-only, run-scoped progress events for the local research workbench."""

from datetime import datetime
from pathlib import Path
import json
import math
import re
import threading

from competition_core.pipeline.artifacts import atomic_json, confined_path, content_hash, read_json
from competition_core.pipeline.errors import PipelineError

_KINDS = frozenset({"QUEUED", "STAGE_STARTED", "PROGRESS", "HEARTBEAT", "TERMINAL", "FAILED"})
_STAGES = frozenset({"PREPARE", "S0_S4", "S0", "S1", "S2", "S3", "S4", "S5", "S5_5",
                     "EXECUTION_REFERENCES", "S6", "UNKNOWN"})
_EVENT_NAME = re.compile(r"([0-9]{20})\.json")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}")
_MAX_EVENT_COUNT = 1_000_000
_MAX_EVENT_SECONDS = 1_000_000.0
_WRITE_LOCK = threading.RLock()


def _event_directory(directory: Path) -> Path:
    return confined_path(Path(directory).resolve(), "task-events")


def _event_files(directory: Path) -> list[Path]:
    root = _event_directory(directory)
    if not root.is_dir():
        return []
    return sorted(path for path in root.iterdir()
                  if path.is_file() and _EVENT_NAME.fullmatch(path.name))


def append_task_event(directory, *, run_id, attempt_id, event_time, kind, stage="UNKNOWN", **fields):
    """Write one immutable bounded event; callers already serialize task writers."""
    if kind not in _KINDS or stage not in _STAGES:
        raise PipelineError("INVALID_ARGUMENTS", "invalid research event classification")
    if (not isinstance(run_id, str) or _RUN_ID.fullmatch(run_id) is None
            or not isinstance(attempt_id, str) or _ATTEMPT_ID.fullmatch(attempt_id) is None):
        raise PipelineError("INVALID_ARGUMENTS", "invalid research event identity")
    if not isinstance(event_time, datetime) or event_time.utcoffset() is None:
        raise PipelineError("INVALID_ARGUMENTS", "research event time must be timezone-aware")
    with _WRITE_LOCK:
        files = _event_files(Path(directory))
        sequence = int(files[-1].stem) + 1 if files else 1
        event = {"schema_version": "competition.task-event.v1", "run_id": run_id,
                 "attempt_id": attempt_id, "event_seq": sequence,
                 "event_time": event_time.isoformat(), "kind": kind, "stage": stage}
        integer_fields = ("completed_count", "total_count", "output_count")
        for name in integer_fields:
            value = fields.get(name)
            if type(value) is int and 0 <= value <= _MAX_EVENT_COUNT:
                event[name] = value
        elapsed_seconds = fields.get("elapsed_seconds")
        if (type(elapsed_seconds) is int and 0 <= elapsed_seconds <= _MAX_EVENT_SECONDS
                or type(elapsed_seconds) is float and math.isfinite(elapsed_seconds)
                and 0 <= elapsed_seconds <= _MAX_EVENT_SECONDS):
            event["elapsed_seconds"] = elapsed_seconds
        if type(fields.get("checkpoint_reused")) is bool:
            event["checkpoint_reused"] = fields["checkpoint_reused"]
        for name in ("error_code", "diagnostic_id"):
            value = fields.get(name)
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
                event[name] = value
        if type(fields.get("retryable")) is bool:
            event["retryable"] = fields["retryable"]
        payload = event | {"integrity_hash": content_hash(event)}
        atomic_json(confined_path(Path(directory), f"task-events/{sequence:020d}.json"),
                    payload, immutable=True)
    return event


def read_task_events(directory, *, after=0, limit=100):
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 500:
        raise PipelineError("INVALID_ARGUMENTS", "invalid research event cursor")
    files = _event_files(Path(directory))
    latest = int(files[-1].stem) if files else 0
    if after > latest:
        raise PipelineError("INVALID_EVENT_CURSOR", "research event cursor is ahead of the stream")
    selected = [path for path in files if int(path.stem) > after][:limit]
    events = []
    for path in selected:
        value = read_json(path)
        integrity = value.pop("integrity_hash", None)
        if integrity != content_hash(value) or value.get("event_seq") != int(path.stem):
            raise PipelineError("STALE_DATA", "research event integrity mismatch")
        events.append(value)
    return {"events": events, "latest_seq": latest,
            "has_more": bool(selected) and int(selected[-1].stem) < latest}


def sse_frame(event):
    sequence = event.get("event_seq")
    if type(sequence) is not int or sequence < 1:
        raise PipelineError("STALE_DATA", "research event sequence is invalid")
    body = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return f"id: {sequence}\nevent: competition.progress\ndata: {body}\n\n"

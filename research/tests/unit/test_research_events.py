from datetime import datetime
import math

import pytest

from competition_core.pipeline.errors import PipelineError
from competition_core.research_events import append_task_event, read_task_events, sse_frame


NOW = datetime.fromisoformat("2026-09-03T10:00:00+08:00")


def test_events_are_immutable_monotonic_and_replayable_after_restart(tmp_path):
    directory = tmp_path / "runs" / "web-example"
    first = append_task_event(directory, run_id="web-example", attempt_id="a" * 32,
                              event_time=NOW, kind="QUEUED")
    second = append_task_event(directory, run_id="web-example", attempt_id="a" * 32,
        event_time=NOW, kind="PROGRESS", stage="S3", completed_count=7, total_count=60)
    original = (directory / "task-events" / "00000000000000000001.json").read_bytes()

    replay = read_task_events(directory, after=1)

    assert first["event_seq"] == 1 and second["event_seq"] == 2
    assert [item["event_seq"] for item in replay["events"]] == [2]
    assert replay["events"][0]["completed_count"] == 7
    assert (directory / "task-events" / "00000000000000000001.json").read_bytes() == original


def test_old_run_without_events_is_compatible_and_future_cursor_is_rejected(tmp_path):
    directory = tmp_path / "old-run"
    directory.mkdir()
    assert read_task_events(directory, after=0) == {"events": [], "latest_seq": 0, "has_more": False}
    with pytest.raises(PipelineError) as raised:
        read_task_events(directory, after=1)
    assert raised.value.code == "INVALID_EVENT_CURSOR"


def test_event_projection_drops_unbounded_or_sensitive_fields(tmp_path):
    directory = tmp_path / "event-projection"
    event = append_task_event(directory, run_id="web-example", attempt_id="b" * 32,
        event_time=NOW, kind="FAILED", stage="PREPARE", error_code="SOURCE_UNAVAILABLE",
        retryable=True, provider_body="token=private", output_count=-1, elapsed_seconds=1.25)
    saved = read_task_events(directory, after=0)["events"][0]
    assert saved["error_code"] == "SOURCE_UNAVAILABLE" and saved["retryable"] is True
    assert saved["elapsed_seconds"] == 1.25
    assert "provider_body" not in saved and "output_count" not in saved
    assert "token" not in str(saved)
    assert sse_frame(event).startswith("id: 1\nevent: competition.progress\n")


def test_event_projection_drops_invalid_numeric_fields(tmp_path):
    directory = tmp_path / "invalid-numbers"
    append_task_event(directory, run_id="web-example", attempt_id="d" * 32,
        event_time=NOW, kind="PROGRESS", stage="S5", completed_count=1_000_001,
        total_count=-1, output_count=math.inf, elapsed_seconds=math.nan)

    saved = read_task_events(directory)["events"][0]

    assert not {"completed_count", "total_count", "output_count", "elapsed_seconds"}.intersection(saved)


def test_heartbeat_event_is_bounded_and_replayable(tmp_path):
    directory = tmp_path / "heartbeat"
    append_task_event(directory, run_id="web-example", attempt_id="c" * 32,
                      event_time=NOW, kind="HEARTBEAT", stage="S5")
    event = read_task_events(directory)["events"][0]
    assert event["kind"] == "HEARTBEAT" and event["stage"] == "S5"

import math

import pytest

import competition_core.pipeline.artifacts as artifacts
from competition_core.pipeline.artifacts import atomic_json, confined_path, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError


def test_json_is_deterministic_hash_verified_and_immutable(tmp_path):
    path = tmp_path / "snapshot.json"
    first = {"z": [1, None], "a": "沪深"}
    atomic_json(path, first, immutable=True)
    atomic_json(path, {"a": "沪深", "z": [1, None]}, immutable=True)
    assert file_hash(path) == content_hash(first)
    assert read_json(path, expected_hash=content_hash(first)) == first
    with pytest.raises(PipelineError, match="content conflict"):
        atomic_json(path, {"z": [2]}, immutable=True)
    assert read_json(path) == first
    assert not list(tmp_path.glob(".partial-*"))


def test_checkpoint_replace_does_not_change_content_addressed_artifact(tmp_path):
    payload = {"status": "PENDING"}
    path = atomic_json(tmp_path / "run.json", payload)
    previous_hash = file_hash(path)
    atomic_json(path, {"status": "RUNNING"})
    with pytest.raises(PipelineError, match="hash mismatch"):
        read_json(path, expected_hash=previous_hash)


def test_mutable_checkpoint_retries_transient_permission_error(tmp_path, monkeypatch):
    path = atomic_json(tmp_path / "run.json", {"status": "PENDING"})
    actual_replace = artifacts.os.replace
    attempts = []
    delays = []

    def sharing_conflict_then_replace(source, target):
        attempts.append((source, target))
        if len(attempts) < 3:
            raise PermissionError("synthetic Windows sharing conflict")
        return actual_replace(source, target)

    monkeypatch.setattr(artifacts.os, "replace", sharing_conflict_then_replace)
    monkeypatch.setattr(artifacts.time, "sleep", delays.append)

    atomic_json(path, {"status": "RUNNING"})

    assert read_json(path) == {"status": "RUNNING"}
    assert len(attempts) == 3
    assert delays == [0.01, 0.02]
    assert not list(tmp_path.glob(".partial-*"))


def test_nonfinite_json_fails_before_replacing_previous_checkpoint(tmp_path):
    path = atomic_json(tmp_path / "scores.json", {"score": None})
    with pytest.raises(ValueError):
        atomic_json(path, {"score": math.nan})
    assert read_json(path) == {"score": None}


@pytest.mark.parametrize("relative", ["..", "../outside.json", "."])
def test_artifact_traversal_rejected(tmp_path, relative):
    with pytest.raises(PipelineError):
        confined_path(tmp_path, relative)
    assert confined_path(tmp_path, "ai/600519.SH.json") == tmp_path / "ai/600519.SH.json"

"""Checkpoint crash-recovery tests; no live data or trading actions."""

import json

import pytest

from competition_core.pipeline.artifacts import read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from tests.pipeline_fixtures import context, small_config


def repository(tmp_path):
    repository = PipelineRepository(tmp_path, "fixture-run")
    config = small_config()
    repository.initialize({"context": context().to_dict(), "snapshot_id": "fixture-only",
                           "config": config.to_dict(), "config_hash": config.config_hash})
    return repository


def result(stage="S0"):
    return {"stage": stage, "status": "SUCCESS", "input_symbols": ["300750.SZ"], "output_symbols": ["300750.SZ"],
            "rows": [{"symbol": "300750.SZ", "selected": True, "features": {"known": 1, "missing": None}, "reject_reasons": []}]}


def test_checkpoint_full_json_parquet_and_hash_roundtrip(tmp_path):
    repo = repository(tmp_path)
    repo.start_stage("S0")
    committed = repo.commit_stage("S0", result(), metadata={"source": "fixture"}, elapsed=.5)
    assert repo.load_stage("S0") == committed
    assert repo.state()["stages"]["S0"]["output_count"] == 1
    assert committed["semantic_hash"] and (repo.directory / "S0.parquet").is_file()
    assert committed["rows"][0]["features"]["missing"] is None
    assert not repo.state()["production_accepted"]


def test_process_interruption_after_artifact_before_state_recovers_without_recompute(tmp_path, monkeypatch):
    repo = repository(tmp_path)
    repo.start_stage("S0")
    original = repo._record_checkpoint
    monkeypatch.setattr(repo, "_record_checkpoint", lambda *args: (_ for _ in ()).throw(RuntimeError("fixture interruption")))
    with pytest.raises(RuntimeError, match="interruption"):
        repo.commit_stage("S0", result(), metadata={"source": "fixture"}, elapsed=.5)
    assert repo.state()["stages"] == {}
    monkeypatch.setattr(repo, "_record_checkpoint", original)
    recovered = repo.recover_checkpoint("S0")
    assert recovered["output_symbols"] == ["300750.SZ"]
    assert repo.state()["stages"]["S0"]["artifact_hash"] == recovered["artifact_hash"]


def test_stage_cannot_start_ahead_of_missing_checkpoint(tmp_path):
    repo = repository(tmp_path)
    with pytest.raises(PipelineError, match="previous"):
        repo.start_stage("S2")


@pytest.mark.parametrize("artifact", ["S0.json", "S0.parquet"])
def test_corrupt_stage_is_not_silently_recomputed(tmp_path, artifact):
    repo = repository(tmp_path)
    repo.start_stage("S0")
    repo.commit_stage("S0", result(), metadata={}, elapsed=.1)
    path = repo.directory / artifact
    if artifact.endswith("json"):
        value = read_json(path)
        value["output_symbols"] = []
        path.write_text(json.dumps(value), encoding="utf-8")
    else:
        path.write_bytes(b"fixture tampering")
    with pytest.raises(PipelineError, match="hash mismatch"):
        repo.load_stage("S0")


def test_same_run_rejects_different_inputs_and_config(tmp_path):
    repo = repository(tmp_path)
    changed = read_json(repo.directory / "inputs.json")
    changed.pop("specification_hash")
    changed["snapshot_id"] = "different"
    with pytest.raises(PipelineError, match="immutable"):
        repo.initialize(changed)


def test_checkpoint_commit_requires_valid_transition_and_identity_subset(tmp_path):
    repo = repository(tmp_path)
    with pytest.raises(PipelineError, match="started"):
        repo.commit_stage("S0", result(), metadata={}, elapsed=.1)
    repo.start_stage("S0")
    invalid = result()
    invalid["output_symbols"] = ["600519.SH"]
    with pytest.raises(PipelineError, match="identities"):
        repo.commit_stage("S0", invalid, metadata={}, elapsed=.1)


def test_os_lock_rejects_concurrent_writers_and_is_reusable_after_release(tmp_path):
    repo = repository(tmp_path)
    with repo.lock():
        with pytest.raises(PipelineError, match="active writer"):
            with PipelineRepository(tmp_path, "fixture-run").lock():
                pytest.fail("concurrent lock was granted")
    with repo.lock():
        assert True


@pytest.mark.parametrize("bad", ["../escape", "..\\escape", "C:\\temp", "folder/name", ""])
def test_run_id_cannot_escape_directory(tmp_path, bad):
    if bad == "":
        # None chooses a random ID; an explicit empty ID also does not form a path.
        assert PipelineRepository(tmp_path, bad).directory.parent == tmp_path
    else:
        with pytest.raises(PipelineError):
            PipelineRepository(tmp_path, bad)

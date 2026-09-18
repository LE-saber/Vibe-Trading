"""S4 resumes verified S0–S3 without source or technical requests."""

import pytest

from competition_core.pipeline.artifacts import atomic_bytes, file_hash
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.runner import run_screening
from tests.integration.test_screening_pipeline_integration import canonical, wrapper, VIBE
from tests.integration.test_evidence_pipeline_integration import run, evidence_store, SupplementalSource
from tests.pipeline_fixtures import context, small_config, technical_rows


def advance(canonical, repo, store, source, technical=None, progress=None):
    root, manifest, snapshot, panel, sessions = canonical
    return run_screening(dataset_root=root, manifest_path=root / manifest["manifest_path"], snapshot=snapshot,
        context=context(), config=small_config(), repository=repo, source_client=wrapper(source), vibe_root=VIBE,
        technical_provider=technical or (lambda *a, **kw: pytest.fail("must reuse technical checkpoint")),
        through_stage="S4", progress=progress, evidence_store=store)


def test_s4_resume_no_calls_immutable_previous_stages_and_repeat(canonical, tmp_path):
    repo = PipelineRepository(tmp_path, "first")
    store = evidence_store(tmp_path / "core-store")
    _, source = run(canonical, repo, store)
    source.calls.clear()
    previous = {stage: file_hash(repo.directory / f"{stage}.json") for stage in ("S0", "S1", "S2", "S3")}
    state = advance(canonical, repo, store, source)
    assert state["completed_through"] == "S4" and state["pending_stage"] == "S5"
    assert not source.calls
    assert previous == {stage: file_hash(repo.directory / f"{stage}.json") for stage in previous}
    stage = repo.load_stage("S4")
    assert stage["summary"]["llm_calls"] == stage["summary"]["agent_tool_calls"] == 0
    assert len(stage["rows"]) == 10 and stage["summary"]["sector_unavailable_count"] == 10
    # Synthetic stocks are strongly correlated; shortage must remain a shortage.
    assert not stage["summary"]["constraints_relaxed"]
    for row in stage["rows"]:
        assert row["keep_reasons"] if row["selected"] else row["reject_reasons"]
    before = file_hash(repo.directory / "S4.json")
    advance(canonical, repo, store, source)
    assert file_hash(repo.directory / "S4.json") == before and not source.calls


def test_s4_independent_recalculation_exact(canonical, tmp_path):
    first, second = PipelineRepository(tmp_path, "a"), PipelineRepository(tmp_path, "b")
    store = evidence_store(tmp_path / "core-store")
    _, source = run(canonical, first, store)
    advance(canonical, first, store, source)
    for path in (first.directory / "sources").glob("*.json"):
        atomic_bytes(second.directory / "sources" / path.name, path.read_bytes(), immutable=True)
    other = SupplementalSource(canonical[3], canonical[4])
    advance(canonical, second, store, other,
            technical=lambda frame, **kw: {"rows": technical_rows(frame, kw["sessions"], kw["symbols"]),
                                            "provenance": {"source": "synthetic-test"}})
    assert not other.calls
    assert first.load_stage("S4")["semantic_hash"] == second.load_stage("S4")["semantic_hash"]


def test_complete_s4_resume_skips_panel_load_and_preserves_reuse_events(canonical, tmp_path, monkeypatch):
    repo = PipelineRepository(tmp_path, "complete-resume")
    store = evidence_store(tmp_path / "core-store")
    _, source = run(canonical, repo, store)
    advance(canonical, repo, store, source)
    before = repo.state()
    stage_hashes = {stage: file_hash(repo.directory / f"{stage}.json") for stage in ("S0", "S1", "S2", "S3", "S4")}

    def fail_load(*args, **kwargs):
        raise AssertionError("complete checkpoint recovery must not load the canonical panel")

    monkeypatch.setattr(CanonicalDataset, "load", fail_load)
    events = []
    source.calls.clear()
    state = advance(canonical, repo, store, source, progress=events.append)

    assert state["status"] == before["status"] == "PARTIAL"
    assert state["completed_through"] == before["completed_through"] == "S4"
    assert state["pending_stage"] == before["pending_stage"] == "S5"
    assert state["stages"] == before["stages"]
    assert stage_hashes == {stage: file_hash(repo.directory / f"{stage}.json") for stage in stage_hashes}
    assert source.calls == []
    assert events == [
        {"stage": stage, "checkpoint_reused": True, "count": len(repo.load_stage(stage)["output_symbols"])}
        for stage in ("S0", "S1", "S2", "S3", "S4")
    ]

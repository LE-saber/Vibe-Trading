"""Synthetic S3 source failures, Core Evidence Store persistence and resume."""

from collections import Counter

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import atomic_bytes, file_hash
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.runner import run_screening
from competition_core.storage.evidence import EvidenceRepository
from tests.integration.test_screening_pipeline_integration import canonical, SyntheticSource, wrapper, VIBE
from tests.pipeline_fixtures import context, small_config, technical_rows


class SupplementalSource(SyntheticSource):
    def __init__(self, panel, sessions, *, fail=()):
        super().__init__(panel, sessions)
        self.fail = fail

    def __getattr__(self, endpoint):
        if endpoint not in {"moneyflow", "top_list", "margin_detail"}:
            return super().__getattr__(endpoint)
        def fetch(**request):
            self.calls.append((endpoint, dict(request)))
            if endpoint in self.fail:
                raise RuntimeError("explicit synthetic source fault")
            rows = []
            for i, symbol in enumerate(sorted(self.panel.symbol.unique())):
                row = dict(ts_code=symbol, trade_date=request["trade_date"])
                if endpoint == "moneyflow":
                    row.update(buy_lg_amount=2 + i / 20, sell_lg_amount=2., buy_elg_amount=1., sell_elg_amount=1., net_mf_amount=i - 3.)
                elif endpoint == "margin_detail":
                    if i == 0:  # Unknown coverage, not a zero-balance assertion.
                        continue
                    row.update(rzye=1000., rqye=0., rzmre=100., rzche=120., rqyl=0., rzrqye=1000.)
                else:
                    continue  # Successful complete market query, nobody listed.
                rows.append(row)
            offset = request.get("offset", 0)
            return pd.DataFrame(rows[offset:offset + 1000])
        return fetch


def evidence_store(root):
    return EvidenceRepository(root / "state/core.sqlite", root / "artifacts/evidence")


def run(canonical, repo, store, source=None, progress=None, technical=None):
    root, manifest, snapshot, panel, sessions = canonical
    source = source or SupplementalSource(panel, sessions)
    technical = technical or (lambda frame, **kw: {"rows": technical_rows(frame, kw["sessions"], kw["symbols"]), "provenance": {"source": "synthetic-test"}})
    state = run_screening(dataset_root=root, manifest_path=root / manifest["manifest_path"], snapshot=snapshot,
        context=context(), config=small_config(), repository=repo, source_client=wrapper(source), vibe_root=VIBE,
        technical_provider=technical, through_stage="S3", progress=progress, evidence_store=store)
    return state, source


def test_whole_market_batch_queries_persist_evidence_and_replay_deterministically(canonical, tmp_path):
    repo, replay = PipelineRepository(tmp_path, "one"), PipelineRepository(tmp_path, "two")
    store = evidence_store(tmp_path / "core-store")
    state, source = run(canonical, repo, store)
    assert state["completed_through"] == "S3" and state["pending_stage"] == "S4"
    stage = repo.load_stage("S3")
    assert len(stage["output_symbols"]) == 10 and stage["summary"]["evidence_count"] == 12
    assert stage["summary"]["llm_calls"] == stage["summary"]["agent_tool_calls"] == 0
    counts = Counter((endpoint, request.get("trade_date")) for endpoint, request in source.calls if endpoint in {"moneyflow", "top_list", "margin_detail"})
    assert len(counts) == 11 and max(counts.values()) == 2  # One data page, then empty termination.
    assert all("ts_code" not in request for endpoint, request in source.calls if endpoint in {"moneyflow", "top_list", "margin_detail"})
    for row in stage["rows"]:
        bundle = store.get(row["evidence_id"])
        assert bundle.symbol.symbol == row["symbol"]
        assert bundle.sections["dragon_tiger"]["listed"] is False
    for path in (repo.directory / "sources").glob("*.json"):
        atomic_bytes(replay.directory / "sources" / path.name, path.read_bytes(), immutable=True)
    _, replay_source = run(canonical, replay, store)
    assert replay_source.calls == []
    assert stage["semantic_hash"] == replay.load_stage("S3")["semantic_hash"]


@pytest.mark.parametrize("failure", ["moneyflow", "top_list", "margin_detail"])
def test_optional_source_failure_is_missing_and_does_not_abort_s3(canonical, tmp_path, failure):
    repo, store = PipelineRepository(tmp_path, "failure"), evidence_store(tmp_path / "core-store")
    state, _ = run(canonical, repo, store, source=SupplementalSource(canonical[3], canonical[4], fail={failure}))
    assert state["completed_through"] == "S3"
    stage = repo.load_stage("S3")
    section = {"moneyflow": "flow", "top_list": "dragon_tiger", "margin_detail": "margin"}[failure]
    assert stage["summary"]["coverage_incomplete_counts"][section] == 12
    bundle = store.get(stage["rows"][0]["evidence_id"])
    if failure == "top_list":
        assert bundle.sections[section]["listed"] is None
    elif failure == "moneyflow":
        assert bundle.sections[section]["main_flow_5d_cny"] is None


def test_mid_s3_interruption_resumes_without_recomputing_screening_or_refetching(canonical, tmp_path, monkeypatch):
    repo, store = PipelineRepository(tmp_path, "interrupted"), evidence_store(tmp_path / "core-store")
    source = SupplementalSource(canonical[3], canonical[4])
    def interrupt(event):
        if event.get("evidence_completed") == 5:
            raise RuntimeError("synthetic interruption after five durable bundles")
    with pytest.raises(RuntimeError, match="interruption"):
        run(canonical, repo, store, source=source, progress=interrupt)
    assert repo.state()["status"] == "FAILED" and repo.state()["stage_progress"]["completed"] == 5
    hashes = {path.name: file_hash(path) for path in repo.directory.glob("S[012].json")}
    completed = {path.name: file_hash(path) for path in (repo.directory / "evidence").glob("*.checkpoint.json")}
    assert len(completed) == 5
    source.calls.clear()
    import competition_core.pipeline.evidence_bundle as module
    original, created = module.build_evidence, []
    def record(**kwargs):
        created.append(kwargs["candidate"]["symbol"])
        return original(**kwargs)
    monkeypatch.setattr(module, "build_evidence", record)
    state, _ = run(canonical, repo, store, source=source, technical=lambda *a, **kw: pytest.fail("must reuse S0–S2"))
    assert state["completed_through"] == "S3" and source.calls == [] and len(created) == 7
    assert hashes == {path.name: file_hash(path) for path in repo.directory.glob("S[012].json")}
    assert all(file_hash(repo.directory / "evidence" / name) == digest for name, digest in completed.items())


def test_modified_evidence_body_is_rejected_on_resume(canonical, tmp_path):
    repo, store = PipelineRepository(tmp_path, "tampered"), evidence_store(tmp_path / "core-store")
    run(canonical, repo, store)
    stage = repo.load_stage("S3")
    path = repo.directory / "evidence" / f"{stage['rows'][0]['evidence_id']}.json"
    path.write_text("{}", encoding="utf-8")  # Explicit corruption fixture.
    from competition_core.pipeline.errors import PipelineError
    with pytest.raises(PipelineError, match="hash mismatch"):
        run(canonical, repo, store)

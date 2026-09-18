"""Core stage integration and disk checkpoints with explicitly synthetic source data."""

from datetime import datetime
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from competition_core.pipeline.artifacts import atomic_bytes, file_hash
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.runner import run_screening
from competition_core.pipeline.sources import ResearchDataClient
from contracts.symbol import CanonicalSymbol
from tests.pipeline_fixtures import context, small_config, synthetic_panel, technical_rows

VIBE = Path(__file__).resolve().parents[3] / "Vibe-Trading"


class SyntheticSource:
    def __init__(self, panel, sessions):
        self.panel, self.sessions = panel, sessions
        self.calls = []

    def __getattr__(self, endpoint):
        def fetch(**request):
            self.calls.append((endpoint, dict(request)))
            if endpoint == "trade_cal":
                rows = [dict(exchange="SSE", cal_date=day.replace("-", ""), is_open=1, pretrade_date="") for day in self.sessions]
            elif endpoint == "index_daily":
                rows = [dict(ts_code="000300.SH", trade_date=day.replace("-", ""), close=100 + i) for i, day in enumerate(self.sessions)]
            else:
                day = datetime.strptime(request["trade_date"], "%Y%m%d").date().isoformat()
                data = self.panel.loc[self.panel.trade_date == day]
                rows = []
                for raw in data.to_dict("records"):
                    row = {"ts_code": raw["symbol"], "trade_date": request["trade_date"]}
                    if endpoint == "daily":
                        row.update({name: raw[f"{name}_raw"] for name in ("open", "high", "low", "close")})
                        row.update(vol=raw["volume_raw"], amount=raw["amount_raw"])
                    elif endpoint == "adj_factor":
                        row["adj_factor"] = raw["adj_factor"]
                    elif endpoint == "daily_basic":
                        row.update({name: raw[name] for name in ("turnover_rate", "volume_ratio", "total_mv", "circ_mv")})
                    elif endpoint == "stk_limit":
                        row.update(pre_close=raw["close_raw"], up_limit=raw["close_raw"] * 1.2, down_limit=raw["close_raw"] * .8)
                    else:
                        raise AssertionError(f"unexpected source request {endpoint}")
                    rows.append(row)
            offset = request.get("offset", 0)
            return pd.DataFrame(rows[offset:offset + 1000])
        return fetch


def wrapper(source):
    return ResearchDataClient(source, context(), observed_clock=lambda: datetime.fromisoformat("2026-09-02T18:00:00+08:00"))


def snapshot(symbols):
    members = [dict(ts_code=symbol, name="synthetic fixture", exchange=CanonicalSymbol.from_core(symbol).exchange,
                    is_eligible=True, exclusion_reasons=[]) for symbol in symbols]
    payload = dict(schema_version=1, competition_id="synthetic-only", as_of="2026-09-02", snapshot_phase="preopen",
                   market_data_reference_date="2026-09-01", members=members, sources=[], warnings=["SYNTHETIC TEST INPUT"])
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    return {"snapshot_id": digest, **payload}


@pytest.fixture(scope="module")
def canonical(tmp_path_factory):
    root = tmp_path_factory.mktemp("screening-synthetic-canonical")
    panel, sessions, symbols = synthetic_panel()
    source = SyntheticSource(panel, sessions)
    frozen = snapshot(symbols)
    dataset = CanonicalDataset(root, "synthetic-fixture")
    manifest = dataset.prepare(wrapper(source), context(), symbols, snapshot_id=frozen["snapshot_id"], sessions=120)
    return root, manifest, frozen, panel, sessions


def run(canonical, repo, *, technical=None, frozen=None, through="S2", source=None, progress=None):
    root, manifest, original, panel, sessions = canonical
    source = source or SyntheticSource(panel, sessions)
    if technical is None:
        technical = lambda frame, **kwargs: {"rows": technical_rows(frame, kwargs["sessions"], kwargs["symbols"]),
                                             "provenance": {"source": "synthetic-test-indicators"}}
    state = run_screening(dataset_root=root, manifest_path=root / manifest["manifest_path"], snapshot=frozen or original,
        context=context(), config=small_config(), repository=repo, source_client=wrapper(source), vibe_root=VIBE,
        technical_provider=technical, through_stage=through, progress=progress)
    return state, source


def test_s0_s1_s2_are_real_core_stages_zero_llm_reproducible_and_resume_cached(canonical, tmp_path):
    one, two = PipelineRepository(tmp_path, "one"), PipelineRepository(tmp_path, "two")
    first, source = run(canonical, one)
    second, _ = run(canonical, two)
    assert first["status"] == second["status"] == "PARTIAL"
    assert first["pending_stage"] == "S3" and not first["production_accepted"]
    for stage in ("S0", "S1", "S2"):
        a, b = one.load_stage(stage), two.load_stage(stage)
        assert a["semantic_hash"] == b["semantic_hash"]
        assert a["rows"] == b["rows"] and a["output_symbols"] == b["output_symbols"]
        assert a["summary"]["llm_calls"] == a["summary"]["agent_tool_calls"] == 0
    assert len(one.load_stage("S0")["output_symbols"]) == 15
    assert len(one.load_stage("S1")["output_symbols"]) == len(one.load_stage("S2")["output_symbols"]) == 12
    before = {stage: file_hash(one.directory / f"{stage}.json") for stage in ("S0", "S1", "S2")}
    source.calls.clear()
    def unavailable(*args, **kwargs):
        raise AssertionError("completed technical checkpoint must not run again")
    run(canonical, one, technical=unavailable, source=source)
    assert source.calls == []
    assert before == {stage: file_hash(one.directory / f"{stage}.json") for stage in before}


def test_single_technical_failure_leaves_s0_resumable(canonical, tmp_path):
    repo = PipelineRepository(tmp_path, "interrupted")
    def fail(*args, **kwargs):
        raise PipelineError("SOURCE_UNAVAILABLE", "synthetic technical failure")
    with pytest.raises(PipelineError, match="technical failure"):
        run(canonical, repo, technical=fail)
    assert repo.state()["status"] == "FAILED" and repo.load_stage("S0") is not None
    s0 = file_hash(repo.directory / "S0.json")
    state, _ = run(canonical, repo)
    assert state["completed_through"] == "S2" and file_hash(repo.directory / "S0.json") == s0


def test_s2_resume_reuses_s1_and_feature_checkpoint(canonical, tmp_path):
    repo = PipelineRepository(tmp_path, "continue")
    run(canonical, repo, through="S1")
    def fail(*args, **kwargs):
        raise AssertionError("feature checkpoint must be reused")
    state, _ = run(canonical, repo, technical=fail)
    assert state["completed_through"] == "S2"


def test_complete_resume_skips_panel_load_and_replays_checkpoint_progress(canonical, tmp_path, monkeypatch):
    repo = PipelineRepository(tmp_path, "complete-hot-resume")
    first, _ = run(canonical, repo, through="S2")
    before = repo.state()
    stage_hashes = {stage: file_hash(repo.directory / f"{stage}.json") for stage in ("S0", "S1", "S2")}

    def fail_load(*args, **kwargs):
        raise AssertionError("complete checkpoint recovery must not load the canonical panel")

    monkeypatch.setattr(CanonicalDataset, "load", fail_load)
    events = []
    second, source = run(canonical, repo, technical=lambda *args, **kwargs: pytest.fail("must reuse checkpoints"),
                         source=SyntheticSource(canonical[3], canonical[4]), progress=events.append)

    assert second["status"] == first["status"] == "PARTIAL"
    assert second["completed_through"] == first["completed_through"] == "S2"
    assert second["pending_stage"] == first["pending_stage"] == "S3"
    assert second["stages"] == before["stages"]
    assert stage_hashes == {stage: file_hash(repo.directory / f"{stage}.json") for stage in stage_hashes}
    assert source.calls == []
    assert events == [
        {"stage": stage, "checkpoint_reused": True, "count": len(repo.load_stage(stage)["output_symbols"])}
        for stage in ("S0", "S1", "S2")
    ]


def test_stale_or_altered_snapshot_rejected_before_stages(canonical, tmp_path):
    frozen = dict(canonical[2], stale=True)
    with pytest.raises(PipelineError, match="degraded"):
        run(canonical, PipelineRepository(tmp_path, "stale"), frozen=frozen)
    changed = json.loads(json.dumps(canonical[2]))
    changed["members"][0]["name"] = "tampered"
    with pytest.raises(PipelineError, match="content hash"):
        run(canonical, PipelineRepository(tmp_path, "tampered"), frozen=changed)


def test_replay_recomputes_from_frozen_supplement_inputs_without_refetch(canonical, tmp_path):
    one, replay = PipelineRepository(tmp_path, "original"), PipelineRepository(tmp_path, "replay")
    run(canonical, one)
    files = list((one.directory / "sources").glob("*.json"))
    assert len(files) == 6
    for path in files:
        atomic_bytes(replay.directory / "sources" / path.name, path.read_bytes(), immutable=True)
    source = SyntheticSource(canonical[3], canonical[4])
    run(canonical, replay, source=source)
    assert source.calls == []
    assert replay.load_stage("S2")["semantic_hash"] == one.load_stage("S2")["semantic_hash"]

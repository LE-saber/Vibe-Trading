"""Synthetic status-to-S6 binding/recovery checks; not real source/account E2E."""

from datetime import timedelta

import pytest

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status_runner import run_execution_status_check, read_run_execution_status
from competition_core.pipeline.portfolio_runner import run_portfolio_draft, read_portfolio
from contracts.symbol import CanonicalSymbol
from tests.integration import test_portfolio_pipeline as fixture
from tests.unit.test_execution_status import Source


def prepared(tmp_path, monkeypatch, *, declared=True, outside=False):
    original = fixture.inputs
    def inputs():
        params = original()
        for rows in (params["candidates"], params["reviews"]):
            for index, symbol in ((10, "300750.SZ"), (11, "688001.SH")):
                rows[index]["symbol"] = symbol
            for row in rows:
                row["identity"] = {"name": "合成样本", "new_listing_special_sessions": 5}
        return params
    with monkeypatch.context() as local:
        local.setattr(fixture, "inputs", inputs)
        repo, kwargs = fixture.setup(tmp_path, declared=declared, outside=outside)
    source = Source()
    symbols = repo.load_stage("S4")["output_symbols"]
    def listing(rows):
        return [{"ts_code": s, "symbol": s[:6], "name": "合成样本",
            "market": {"MAIN": "主板", "CHINEXT": "创业板", "STAR": "科创板"}[CanonicalSymbol.from_core(s).board],
            "exchange": CanonicalSymbol.from_core(s).exchange,
            "list_status": "L", "list_date": "20150101", "delist_date": None} for s in symbols] if rows else []
    source.changes["stock_basic"] = listing
    source.changes["stk_limit"] = lambda rows: [{"ts_code": s, "trade_date": "20260902",
        "pre_close": 10., "up_limit": 11., "down_limit": 9.} for s in symbols]
    kwargs.pop("trading_metadata")
    return repo, kwargs, source


def observe(repo, kwargs, source, status_id="synthetic-status"):
    return run_execution_status_check(repository=repo, client=source, source_name="explicit.synthetic",
        account=kwargs["account"], clock=kwargs["clock"], status_id=status_id)


def test_automatic_status_seals_source_proofs_without_mutating_research(tmp_path, monkeypatch):
    repo, kwargs, source = prepared(tmp_path, monkeypatch)
    before = {stage: file_hash(repo.directory / f"{stage}.json") for stage in ("S0", "S1", "S2", "S3", "S4", "S5")}
    observation = observe(repo, kwargs, source)
    assert observation["summary"]["complete_reference_count"] == 12
    assert not observation["execution_ready"]
    result = run_portfolio_draft(**kwargs, status_id="synthetic-status", draft_id="automatic")
    assert result["current_execution_ready"] and not result["production_accepted"]
    proofs = repo.load_stage("S6")["source_provenance"]
    assert any(p["path"].endswith("binding.json") for p in proofs)
    assert any("execution-status/synthetic-status/sources/limits" in p["path"] for p in proofs)
    assert before == {stage: file_hash(repo.directory / f"{stage}.json") for stage in before}
    with pytest.raises(PipelineError, match="sealed portfolio"):
        observe(repo, kwargs, source, "replacement")
    resumed = run_portfolio_draft(**kwargs, status_id="synthetic-status")
    assert resumed["current_execution_ready"]
    stale = read_portfolio(repository=repo, account=kwargs["account"],
        observed_at=kwargs["account"].as_of + timedelta(seconds=61))
    assert not stale["execution_ready"] and not stale["freshness"]["trading_status"]


@pytest.mark.parametrize("change", ["account", "raw_source", "observation", "binding", "expired"])
def test_status_binding_rejects_changed_or_expired_evidence(tmp_path, monkeypatch, change):
    repo, kwargs, source = prepared(tmp_path, monkeypatch)
    observation = observe(repo, kwargs, source)
    params = dict(repository=repo, status_id="synthetic-status", account=kwargs["account"], observed_at=kwargs["clock"]())
    directory = repo.directory / "execution-status/synthetic-status"
    if change == "account":
        params["account"] = AccountSnapshot.from_dict(kwargs["account"].to_dict() | {"available_cash": "1.00"})
    elif change == "expired":
        params["observed_at"] += timedelta(seconds=61)
    else:
        name = observation["source_provenance"][0]["path"] if change == "raw_source" else change + ".json"
        atomic_json(directory / name, {"tampered": True})
    with pytest.raises(PipelineError):
        read_run_execution_status(**params)
    assert repo.load_stage("S6") is None


def test_partial_source_is_not_ready_and_new_observation_can_recover(tmp_path, monkeypatch):
    repo, kwargs, source = prepared(tmp_path, monkeypatch)
    source.fail.add("suspend_d")
    assert observe(repo, kwargs, source)["status"] == "PARTIAL"
    failed = run_portfolio_draft(**kwargs, status_id="synthetic-status", draft_id="missing")
    assert failed["portfolio_status"] == "NOT_READY" and repo.load_stage("S6") is None
    original_hash = file_hash(repo.directory / "execution-status/synthetic-status/observation.json")
    source.fail.clear()
    assert observe(repo, kwargs, source, "recovery")["status"] == "READY"
    result = run_portfolio_draft(**kwargs, status_id="recovery", draft_id="repaired")
    assert result["current_execution_ready"]
    assert file_hash(repo.directory / "execution-status/synthetic-status/observation.json") == original_hash


def test_status_and_manual_metadata_are_exclusive(tmp_path, monkeypatch):
    repo, kwargs, source = prepared(tmp_path, monkeypatch)
    with pytest.raises(PipelineError, match="not both"):
        run_portfolio_draft(**kwargs, status_id="synthetic-status", trading_metadata={})
    assert not source.calls


def test_unknown_held_identity_does_not_guess_listing_period(tmp_path, monkeypatch):
    repo, kwargs, source = prepared(tmp_path, monkeypatch, outside=True)
    with pytest.raises(PipelineError, match="frozen special-period"):
        observe(repo, kwargs, source)
    assert not source.calls


def test_mid_seal_resume_keeps_automatic_status_proofs(tmp_path, monkeypatch):
    repo, kwargs, source = prepared(tmp_path, monkeypatch)
    observe(repo, kwargs, source)
    commit = repo.commit_stage
    def interrupted(stage, *args, **options):
        if stage == "S6":
            raise OSError("synthetic interruption")
        return commit(stage, *args, **options)
    monkeypatch.setattr(repo, "commit_stage", interrupted)
    with pytest.raises(OSError):
        run_portfolio_draft(**kwargs, status_id="synthetic-status", draft_id="interrupted")
    with pytest.raises(PipelineError, match="sealed portfolio"):
        observe(repo, kwargs, source, "cannot-replace")
    before = file_hash(repo.directory / "execution-status/synthetic-status/observation.json")
    monkeypatch.setattr(repo, "commit_stage", commit)
    assert run_portfolio_draft(**kwargs, status_id="synthetic-status")["current_execution_ready"]
    assert file_hash(repo.directory / "execution-status/synthetic-status/observation.json") == before

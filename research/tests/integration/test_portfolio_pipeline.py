"""Synthetic pipeline/declared-account branches; NEVER evidence of real E2E."""

from datetime import timedelta
from decimal import Decimal

import pytest

from competition_core.pipeline.account import AccountSnapshot, AccountPosition
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.realtime import run_realtime_check
from competition_core.pipeline.portfolio_runner import run_portfolio_draft, read_portfolio
from competition_core.pipeline.holding_review_consumer import disposition_for
from tests.unit.test_pipeline_portfolio import inputs, with_holdings
from tests.integration.test_realtime_pipeline import quotes


def setup(tmp_path, *, declared=False, outside=False):
    params = inputs()
    if outside:
        with_holdings(params, [AccountPosition("000001.SZ", 100, 100, Decimal("1000"))])
        params["trading_metadata"]["000001.SZ"] = dict(next(iter(params["trading_metadata"].values())))
    if declared:
        # Exercises this branch only. Data are still entirely synthetic test fixtures.
        params["account"] = AccountSnapshot.from_dict(params["account"].to_dict() | {"source_kind": "manual_user"})
    repo = PipelineRepository(tmp_path / "runs", "synthetic-only")
    manifest = tmp_path / "manifest.json"
    atomic_json(manifest, {"explicit_synthetic_fixture": True})
    ctx, config = params["context"], params["config"]
    repo.initialize({"context": ctx.to_dict(), "snapshot_id": params["snapshot_id"], "config": config.to_dict(),
        "config_hash": config.config_hash, "manifest_path": str(manifest), "manifest_hash": file_hash(manifest)})
    symbols = [c["symbol"] for c in params["candidates"]]
    for stage in ("S0", "S1", "S2", "S3", "S4", "S5"):
        source = params["reviews"] if stage == "S5" else params["candidates"]
        repo.start_stage(stage)
        repo.commit_stage(stage, {"stage": stage, "status": "SUCCESS", "input_symbols": symbols,
            "output_symbols": symbols, "rows": [row | {"selected": True} for row in source]},
            metadata=ctx.to_dict() | {"snapshot_id": params["snapshot_id"], "config_hash": config.config_hash}, elapsed=0)
    run_realtime_check(repository=repo, account=params["account"], check_id="synthetic-quote",
                       quote_source=quotes, clock=lambda: ctx.as_of)
    kwargs = {key: params[key] for key in ("account", "fees", "trading_metadata")}
    return repo, dict(repository=repo, check_id="synthetic-quote", clock=lambda: ctx.as_of, **kwargs)


def test_synthetic_attempt_never_seals_production_stages(tmp_path):
    repo, kwargs = setup(tmp_path)
    before = {s: file_hash(repo.directory / f"{s}.json") for s in ("S0", "S1", "S2", "S3", "S4", "S5")}
    result = run_portfolio_draft(**kwargs, draft_id="synthetic-draft")
    assert result["portfolio_status"] == "DRY_RUN" and not result["execution_ready"]
    assert repo.load_stage("S6") is None and repo.load_stage("S5_5") is None
    assert not (repo.directory / "portfolio.json").exists()
    assert before == {s: file_hash(repo.directory / f"{s}.json") for s in before}
    assert read_json(repo.directory / "portfolio-attempts/synthetic-draft/account.json")["source_kind"] == "synthetic_test"


def test_declared_account_branch_seals_and_current_read_downgrades_stale_quotes(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    result = run_portfolio_draft(**kwargs, draft_id="declared-fixture")
    assert result["portfolio_status"] == "DRAFT_READY" and result["current_execution_ready"]
    assert not result["production_accepted"]
    assert repo.load_stage("S6")["status"] == "SUCCESS"
    saved_hash = file_hash(repo.directory / "portfolio.json")
    stale = read_portfolio(repository=repo, observed_at=kwargs["account"].as_of + timedelta(seconds=61))
    assert not stale["execution_ready"] and stale["execution_ready_at_generation"]
    assert stale["historical_draft_only"]
    assert file_hash(repo.directory / "portfolio.json") == saved_hash


def test_declared_account_with_unreviewed_outside_holding_does_not_seal(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True, outside=True)

    result = run_portfolio_draft(**kwargs, draft_id="outside-holding")

    assert result["portfolio_status"] == "NOT_READY" and not result["execution_ready"]
    assert "MISSING_HOLDING_RESEARCH_COVERAGE:000001.SZ" in result["risk_checks"]["blocking_reasons"]
    assert repo.load_stage("S6") is None and not (repo.directory / "portfolio-seal.json").exists()


def test_validated_retain_consumption_is_frozen_and_sealed(tmp_path, monkeypatch):
    repo, kwargs = setup(tmp_path, declared=True, outside=True)
    source_hash = "b" * 64
    item = disposition_for(symbol="000001.SZ", verdict="KEEP",
                           attempt_id="holding-" + "a" * 32, status="SUCCESS")
    consumption = {"schema_version": "p10.holding_review_consumption.v1",
                   "account_hash": kwargs["account"].snapshot_hash,
                   "account_source_hash": source_hash, "index_hash": "c" * 64,
                   "dispositions": [item.to_dict()], "blocking_reasons": [],
                   "portfolio_authority": False}
    monkeypatch.setattr("competition_core.pipeline.portfolio_runner.validate_holding_review_index",
                        lambda **_: consumption)

    result = run_portfolio_draft(**kwargs, account_source_hash=source_hash,
                                 draft_id="validated-retain")

    row = next(item for item in result["rows"] if item["symbol"] == "000001.SZ")
    attempt = repo.directory / "portfolio-attempts" / "validated-retain"
    request = read_json(attempt / "request.json")
    assert result["portfolio_status"] == "DRAFT_READY" and result["current_execution_ready"]
    assert row["target_quantity"] == 100 and row["delta_quantity"] == 0
    assert read_json(attempt / "holding-review-consumption.json") == consumption
    assert request["holding_review_consumption_hash"] == content_hash(consumption)
    assert "holding-review-consumption.json" in read_json(repo.directory / "portfolio-seal.json")["files"]


def test_mid_seal_failure_resumes_without_quotes_or_ai_rerun(tmp_path, monkeypatch):
    repo, kwargs = setup(tmp_path, declared=True)
    original = repo.commit_stage
    def interrupted(stage, *args, **kwargs):
        if stage == "S6":
            raise OSError("explicit synthetic interruption")
        return original(stage, *args, **kwargs)
    monkeypatch.setattr(repo, "commit_stage", interrupted)
    with pytest.raises(OSError):
        run_portfolio_draft(**kwargs, draft_id="interrupted")
    gate_hash = file_hash(repo.directory / "S5_5.json")
    assert repo.load_stage("S6") is None and (repo.directory / "portfolio-seal.json").exists()
    monkeypatch.setattr(repo, "commit_stage", original)
    result = run_portfolio_draft(**kwargs)
    assert result["current_execution_ready"]
    assert file_hash(repo.directory / "S5_5.json") == gate_hash
    assert len(list((repo.directory / "portfolio-attempts").iterdir())) == 1


def test_stale_observation_rechecked_not_reused_as_pass(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    kwargs["clock"] = lambda: kwargs["account"].as_of + timedelta(seconds=61)
    result = run_portfolio_draft(**kwargs, draft_id="stale")
    assert result["portfolio_status"] == "NOT_READY"
    assert repo.load_stage("S6") is None and not result["execution_ready"]


def test_missing_fees_retry_preserves_failed_attempt(tmp_path):
    repo, kwargs = setup(tmp_path)
    first = run_portfolio_draft(**(kwargs | {"fees": None}), draft_id="missing")
    assert first["portfolio_status"] == "NOT_READY"
    result = run_portfolio_draft(**kwargs, draft_id="repaired")
    assert result["portfolio_status"] == "DRY_RUN"
    with pytest.raises(PipelineError, match="already used"):
        run_portfolio_draft(**kwargs, draft_id="missing")


def test_tampered_quote_receipt_and_account_hash_fail(tmp_path):
    repo, kwargs = setup(tmp_path)
    path = repo.directory / "realtime/synthetic-quote/response.json"
    atomic_json(path, {"tampered": True})
    with pytest.raises(PipelineError, match="hash mismatch"):
        run_portfolio_draft(**kwargs)
    repo, kwargs = setup(tmp_path / "second")
    changed = kwargs["account"].to_dict() | {"available_cash": "900000.00"}
    kwargs["account"] = AccountSnapshot.from_dict(changed)
    with pytest.raises(PipelineError, match="different account"):
        run_portfolio_draft(**kwargs)


def test_sealed_attempt_rejects_term_changes_and_result_tampering(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    run_portfolio_draft(**kwargs, draft_id="sealed")
    with pytest.raises(PipelineError, match="execution terms"):
        run_portfolio_draft(**(kwargs | {"fees": None}))
    atomic_json(repo.directory / "portfolio-attempts/sealed/result.json", {"corrupt": True})
    with pytest.raises(PipelineError, match="hash mismatch"):
        read_portfolio(repository=repo, observed_at=kwargs["account"].as_of)


def test_calculation_uses_second_actual_clock_and_never_backdates(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    times = iter([kwargs["account"].as_of, kwargs["account"].as_of + timedelta(seconds=61)])
    result = run_portfolio_draft(**(kwargs | {"clock": lambda: next(times)}), draft_id="late")
    assert result["portfolio_status"] == "NOT_READY" and repo.load_stage("S6") is None

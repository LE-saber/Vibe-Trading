"""Synthetic pipeline/declared-account branches; NEVER evidence of real E2E."""

from datetime import timedelta
from dataclasses import replace
from decimal import Decimal

import pytest

from competition_core.pipeline.account import AccountSnapshot, AccountPosition
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.realtime import run_realtime_check
from competition_core.pipeline.portfolio_runner import run_portfolio_draft, read_portfolio
from competition_core.pipeline.holding_review_consumer import disposition_for
from competition_core.research_tasks import ResearchTasks, worker_fingerprint
from competition_core.terminal_strategy import (
    CandidateObservation,
    LeaderboardEntry,
    TerminalEvaluationPersistence,
    TerminalEvaluationRequest,
    TerminalSourceHashes,
    TerminalSnapshot,
    TerminalStrategyConfig,
    TerminalStrategyService,
    load_terminal_strategy_config,
)
from competition_core.terminal_strategy.realtime_stage import load_terminal_realtime_stage
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


def test_terminal_evaluation_is_bound_as_non_executable_shadow_attempt(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    params = inputs()
    symbols = [row["symbol"] for row in params["candidates"]]
    realtime_stage = load_terminal_realtime_stage(repo, "synthetic-quote")
    stage_hashes = {name: repo.load_stage(name)["artifact_hash"] for name in ("S4", "S5")}
    stage_hashes["S5_5"] = realtime_stage["artifact_hash"]
    as_of = params["context"].as_of
    terminal_config = load_terminal_strategy_config()
    leaderboard = tuple(
        LeaderboardEntry(rank, Decimal("1000000.00") + Decimal(rank) * Decimal("1000.00"), as_of)
        for rank in range(1, 31)
    )
    base = TerminalEvaluationRequest(
        run_id=repo.run_id,
        snapshot=TerminalSnapshot(
            competition_id="runner-fixture", as_of=as_of, trade_date=params["context"].research_date,
            remaining_trade_dates=terminal_config.competition.remaining_trade_dates,
            own_total_assets=Decimal("1000000.00"), leaderboard=leaderboard,
            award_rank_cutoff=terminal_config.competition.award_rank_cutoff,
            award_rule_source=terminal_config.competition.award_rule_source, own_rank=34,
        ),
        award_line_history=(),
        candidates=(CandidateObservation(symbol="600001.SH", as_of=as_of,
                                         trade_date=params["context"].research_date),),
        core_symbols=tuple(symbols[:4]), config=terminal_config,
        source_hashes=TerminalSourceHashes(
            s4_hash="a" * 64, s5_hash="b" * 64, s5_5_hash="c" * 64,
            account_snapshot_hash="d" * 64, leaderboard_snapshot_hash="e" * 64,
            config_hash=terminal_config.config_hash, implementation_hash="f" * 64,
        ), observed_at=as_of,
    )
    source_hashes = TerminalSourceHashes(
        s4_hash=stage_hashes["S4"], s5_hash=stage_hashes["S5"], s5_5_hash=stage_hashes["S5_5"],
        account_snapshot_hash=kwargs["account"].snapshot_hash,
        leaderboard_snapshot_hash="b" * 64,
        config_hash=base.config.config_hash, implementation_hash="c" * 64,
    )
    request = replace(
        base,
        run_id=repo.run_id,
        snapshot=replace(base.snapshot, account_snapshot_hash=kwargs["account"].snapshot_hash),
        source_hashes=source_hashes,
        observed_at=kwargs["account"].as_of,
    )
    evaluation = TerminalStrategyService().evaluate(request)
    assert evaluation.status in {"BLOCKED", "UNAVAILABLE"} and evaluation.allocation_policy is None
    TerminalEvaluationPersistence(repo.directory.parent).persist(evaluation)

    result = run_portfolio_draft(
        **kwargs, terminal_evaluation_id=evaluation.evaluation_id, draft_id="terminal-shadow",
    )

    assert result["portfolio_status"] == "NOT_READY"
    assert result["execution_ready"] is False and result["production_accepted"] is False
    assert "TERMINAL_POLICY_SHADOW_ONLY" in result["risk_checks"]["blocking_reasons"]
    assert repo.load_stage("S5_5") is None
    assert repo.load_stage("S6") is None
    assert not (repo.directory / "portfolio-seal.json").exists()
    attempt = repo.directory / "portfolio-attempts" / "terminal-shadow"
    request_payload = read_json(attempt / "request.json")
    saved_result = read_json(attempt / "result.json")
    assert request_payload["terminal_evaluation_id"] == evaluation.evaluation_id
    assert request_payload["terminal_manifest_hash"]
    assert request_payload["terminal_source_hashes"]["s5_5_hash"] == stage_hashes["S5_5"]
    assert saved_result["summary"]["terminal_evaluation_id"] == evaluation.evaluation_id
    assert saved_result["policy"]["terminal_evaluation_binding"]["pipeline_config_hash"] == params["config"].config_hash


def test_terminal_evaluation_identifier_is_strict_and_does_not_create_attempt(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    with pytest.raises(PipelineError, match="terminal evaluation identifier"):
        run_portfolio_draft(**kwargs, terminal_evaluation_id="../escape", draft_id="terminal-invalid")
    assert not (repo.directory / "portfolio-attempts" / "terminal-invalid").exists()


def _terminal_shadow_evaluation(repo, kwargs):
    """Build one complete terminal evaluation bound to this synthetic run."""

    from tests.unit.test_terminal_strategy_allocation import _observation

    payload = load_terminal_strategy_config().to_dict()
    payload["competition"]["award_rank_cutoff"] = 30
    payload["competition"]["award_rule_source"] = "fixture:rules-v1"
    config = TerminalStrategyConfig.from_dict(payload)
    as_of = kwargs["clock"]()
    symbols = list(repo.load_stage("S4")["output_symbols"])
    observations = tuple(
        _observation(symbol, index=index, as_of=as_of, trade_date=as_of.date())
        for index, symbol in enumerate(symbols[4:])
    )
    leaderboard = tuple(
        LeaderboardEntry(
            rank,
            Decimal("1000000.00") + Decimal(rank) * Decimal("1000.00"),
            as_of,
        )
        for rank in range(1, 31)
    )
    realtime = load_terminal_realtime_stage(repo, "synthetic-quote")
    source_hashes = TerminalSourceHashes(
        s4_hash=repo.load_stage("S4")["artifact_hash"],
        s5_hash=repo.load_stage("S5")["artifact_hash"],
        s5_5_hash=realtime["artifact_hash"],
        account_snapshot_hash=kwargs["account"].snapshot_hash,
        leaderboard_snapshot_hash="b" * 64,
        config_hash=config.config_hash,
        implementation_hash="c" * 64,
    )
    request = TerminalEvaluationRequest(
        run_id=repo.run_id,
        snapshot=TerminalSnapshot(
            competition_id="synthetic-terminal-competition",
            as_of=as_of,
            trade_date=as_of.date(),
            remaining_trade_dates=config.competition.remaining_trade_dates,
            own_total_assets=kwargs["account"].current_total_asset,
            leaderboard=leaderboard,
            award_rank_cutoff=30,
            award_rule_source="fixture:rules-v1",
            own_rank=34,
            account_snapshot_hash=kwargs["account"].snapshot_hash,
            leaderboard_snapshot_hash="b" * 64,
        ),
        award_line_history=(),
        candidates=observations,
        core_symbols=tuple(symbols[:4]),
        config=config,
        source_hashes=source_hashes,
        observed_at=as_of,
        same_day_exit_unavailable_symbols=(),
    )
    evaluation = TerminalStrategyService().evaluate(request)
    assert evaluation.status == "COMPLETE"
    assert evaluation.allocation_policy is not None
    TerminalEvaluationPersistence(repo.directory.parent).persist(evaluation)
    return evaluation


def _import_complete_ninef_inputs(repo, kwargs):
    account = kwargs["account"].to_dict()
    fees = kwargs["fees"].to_dict()
    body = {
        "account": account,
        "fees": fees,
        "source_receipt": {
            "schema_version": "ninef.account.fetch.v2",
            "method": "GET_ONLY",
            "collection_consistent": True,
            "account_snapshot_hash": kwargs["account"].snapshot_hash,
            "fee_schedule_hash": content_hash(fees),
            "orders": {
                "complete": True,
                "count": 0,
                "open_count": 0,
                "items": [],
                "unknown_count": 0,
            },
        },
    }
    input_id = content_hash(body)
    atomic_json(repo.directory / f"execution-imports/{input_id}.json", body, immutable=True)
    atomic_json(repo.directory / "execution-inputs.json", {"id": input_id}, immutable=True)
    return input_id


def _prepare_real_terminal_shadow(tmp_path, monkeypatch, *, import_inputs=True, status=True):
    from tests.integration.test_execution_status_pipeline import observe, prepared

    repo, kwargs, source = prepared(tmp_path, monkeypatch, declared=True)
    if status:
        observe(repo, kwargs, source, status_id="synthetic-status")
    if import_inputs:
        _import_complete_ninef_inputs(repo, kwargs)
    tasks = ResearchTasks(repo.directory.parent, clock=kwargs["clock"])
    state = repo.state()
    task_request = {
        "schema_version": "competition.web-task.v1",
        "run_id": repo.run_id,
        "research_date": state["research_date"],
        "phase": state["phase"],
        "as_of": state["as_of"],
        "ai_review": True,
        "vibe_root": str(tasks.vibe_root),
        "code_hashes": worker_fingerprint(tasks.vibe_root),
    }
    atomic_json(repo.directory / "task-request.json",
                task_request | {"integrity_hash": content_hash(task_request)}, immutable=True)
    evaluation = _terminal_shadow_evaluation(repo, kwargs)
    return repo, kwargs, tasks, evaluation


def test_terminal_shadow_plan_runs_real_pipeline_and_never_seals(tmp_path, monkeypatch):
    from competition_core.research_portfolio import create_terminal_shadow_plan
    from competition_core.pipeline.execution_plan import create_execution_plan

    repo, kwargs, tasks, evaluation = _prepare_real_terminal_shadow(tmp_path, monkeypatch)
    before = {
        name: file_hash(repo.directory / name)
        for name in ("S5_5.json", "S6.json", "portfolio.json", "portfolio-seal.json")
        if (repo.directory / name).is_file()
    }

    projection = create_terminal_shadow_plan(
        tasks=tasks,
        run_id=repo.run_id,
        evaluation_id=evaluation.evaluation_id,
    )

    assert projection["schema_version"] == "competition.terminal-shadow-plan.v1"
    assert projection["evaluation_id"] == evaluation.evaluation_id
    assert projection["shadow_only"] is True
    assert projection["execution_ready"] is False
    assert projection["production_accepted"] is False
    assert projection["orders_submitted"] == 0
    attempt = repo.directory / "portfolio-attempts" / projection["attempt_id"]
    assert attempt.is_dir()
    request = read_json(attempt / "request.json")
    saved_result = read_json(attempt / "result.json")
    manifest = read_json(repo.directory / "terminal" / evaluation.evaluation_id / "decision-manifest.json")
    assert request["terminal_evaluation_id"] == evaluation.evaluation_id
    assert request["terminal_manifest_hash"] == manifest["manifest_hash"]
    assert request["terminal_allocation_policy_hash"] == manifest["allocation_policy_hash"]
    assert request["terminal_source_hashes"] == {
        key: manifest[key]
        for key in ("s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
                    "leaderboard_snapshot_hash", "config_hash", "implementation_hash")
    }
    assert saved_result["execution_ready"] is False
    assert saved_result["production_accepted"] is False
    assert saved_result["summary"]["terminal_evaluation_id"] == evaluation.evaluation_id
    assert not (repo.directory / "S5_5.json").exists()
    assert not (repo.directory / "S6.json").exists()
    assert not (repo.directory / "portfolio.json").exists()
    assert not (repo.directory / "portfolio-seal.json").exists()
    assert before == {}
    with pytest.raises(PipelineError):
        create_execution_plan(repo)


@pytest.mark.parametrize("missing", ["last_realtime_check", "last_execution_status"])
def test_terminal_shadow_plan_rejects_stale_or_missing_server_facts(tmp_path, monkeypatch, missing):
    from competition_core.research_portfolio import create_terminal_shadow_plan

    repo, kwargs, tasks, evaluation = _prepare_real_terminal_shadow(tmp_path, monkeypatch)
    with pytest.raises(PipelineError, match="latest evaluation"):
        create_terminal_shadow_plan(tasks=tasks, run_id=repo.run_id, evaluation_id="0" * 64)

    state = repo.state()
    state.pop(missing, None)
    atomic_json(repo.directory / "run.json", state)
    with pytest.raises(PipelineError, match="realtime"):
        create_terminal_shadow_plan(tasks=tasks, run_id=repo.run_id)


def test_terminal_shadow_plan_requires_complete_imported_account_receipt(tmp_path, monkeypatch):
    from competition_core.research_portfolio import create_terminal_shadow_plan

    repo, kwargs, tasks, evaluation = _prepare_real_terminal_shadow(
        tmp_path, monkeypatch, import_inputs=False
    )
    with pytest.raises(PipelineError, match="imported Ninef"):
        create_terminal_shadow_plan(tasks=tasks, run_id=repo.run_id)

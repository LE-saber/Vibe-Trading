"""Bind portfolio math to immutable research, quote and account evidence."""

from datetime import datetime
from pathlib import Path
import re
import time
import uuid

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json, confined_path
from competition_core.pipeline.account import assess_account
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.holding_review_consumer import validate_holding_review_index
from competition_core.pipeline.portfolio import build_portfolio, _metadata
from competition_core.pipeline.realtime import _inputs, _fingerprint as quote_fingerprint
from competition_core.pipeline.realtime_gate import evaluate_realtime_quotes
from competition_core.pipeline.time_context import SHANGHAI
from competition_core.terminal_strategy.persistence import (
    TerminalEvaluationPersistence,
    TerminalPersistenceError,
)
from competition_core.terminal_strategy.portfolio_bridge import validate_terminal_policy
from competition_core.terminal_strategy.models import TerminalModelError, TerminalSnapshot

ROOT = Path(__file__).resolve().parents[2]
_TERMINAL_EVALUATION_ID = re.compile(r"[0-9a-f]{64}\Z")


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", value):
        raise PipelineError("INVALID_ARGUMENTS", "invalid portfolio/check identifier")
    return value


def _terminal_evaluation_id(value):
    if not isinstance(value, str) or _TERMINAL_EVALUATION_ID.fullmatch(value) is None:
        raise PipelineError("INVALID_ARGUMENTS", "invalid terminal evaluation identifier")
    return value


def _terminal_store(repository):
    """Resolve the repository's sibling ``runs`` root without accepting a path."""

    return TerminalEvaluationPersistence(Path(repository.directory).resolve().parent)


def _terminal_binding(*, repository, evaluation_id, stages, context, account, observed_at):
    """Load and bind one immutable terminal evaluation to this pipeline run."""

    try:
        loaded = _terminal_store(repository).load(repository.run_id, evaluation_id)
    except TerminalPersistenceError as exc:
        raise PipelineError("STALE_DATA", "terminal evaluation integrity or identity check failed") from exc

    manifest = loaded["manifest"]
    artifacts = loaded["artifacts"]
    source = loaded["source_hashes"]
    s55 = repository.load_stage("S5_5")
    if s55 is None:
        raise PipelineError("NOT_READY", "terminal evaluation requires a current S5_5 stage")
    current_stage_hashes = {
        "s4_hash": stages["S4"]["artifact_hash"],
        "s5_hash": stages["S5"]["artifact_hash"],
        "s5_5_hash": s55["artifact_hash"],
    }
    for field, expected in current_stage_hashes.items():
        if source.get(field) != expected or manifest.get(field) != expected:
            raise PipelineError("STALE_DATA", "terminal evaluation is bound to different research stages")

    if account is None or source.get("account_snapshot_hash") != account.snapshot_hash:
        raise PipelineError("STALE_DATA", "terminal evaluation account snapshot differs")
    if manifest.get("account_snapshot_hash") != account.snapshot_hash:
        raise PipelineError("STALE_DATA", "terminal evaluation manifest account differs")

    snapshot_payload = artifacts.get("terminal-snapshot.json")
    try:
        snapshot = TerminalSnapshot.from_dict(snapshot_payload)
    except (TerminalModelError, TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "terminal evaluation snapshot is invalid") from exc
    if snapshot.trade_date != context.research_date:
        raise PipelineError("STALE_DATA", "terminal snapshot date differs from pipeline context")
    if snapshot.as_of > observed_at:
        raise PipelineError("STALE_DATA", "terminal snapshot is newer than portfolio observation")
    if snapshot.account_snapshot_hash != account.snapshot_hash:
        raise PipelineError("STALE_DATA", "terminal snapshot account differs")

    allocation = artifacts.get("allocation-policy.json")
    if not isinstance(allocation, dict):
        raise PipelineError("STALE_DATA", "terminal allocation artifact is invalid")
    allocation_hash = manifest.get("allocation_policy_hash")
    if allocation_hash is None:
        if allocation.get("available") is not False or allocation.get("policy") is not None:
            raise PipelineError("STALE_DATA", "terminal unavailable allocation envelope is invalid")
    else:
        policy_validation = validate_terminal_policy(
            allocation,
            indexed_symbols=stages["S4"]["input_symbols"],
            holding_symbols={position.symbol for position in account.positions},
            expected_config_hash=manifest.get("config_hash"),
            expected_trade_date=context.research_date,
            observed_at=observed_at,
        )
        if not policy_validation.valid or policy_validation.policy is None:
            raise PipelineError("STALE_DATA", "terminal allocation policy is invalid or stale")
        policy = policy_validation.policy
        if policy.policy_hash != allocation_hash:
            raise PipelineError("STALE_DATA", "terminal allocation policy hash differs")
        if policy.mode_decision_hash != manifest.get("mode_decision_hash"):
            raise PipelineError("STALE_DATA", "terminal mode decision binding differs")
        if policy.funnel_hash != manifest.get("candidate_funnel_hash"):
            raise PipelineError("STALE_DATA", "terminal funnel binding differs")
        if policy.config_hash != manifest.get("config_hash"):
            raise PipelineError("STALE_DATA", "terminal config binding differs")

    if manifest.get("config_hash") != source.get("config_hash"):
        raise PipelineError("STALE_DATA", "terminal config/source binding differs")
    mode = artifacts.get("mode-decision.json")
    terminal_funnel = artifacts.get("candidates-terminal.json")
    if not isinstance(mode, dict) or not isinstance(terminal_funnel, dict):
        raise PipelineError("STALE_DATA", "terminal mode/funnel artifacts are invalid")
    if manifest.get("mode_decision_hash") != content_hash(mode):
        raise PipelineError("STALE_DATA", "terminal mode decision hash differs")
    if manifest.get("candidate_funnel_hash") != terminal_funnel.get("funnel_hash"):
        raise PipelineError("STALE_DATA", "terminal funnel hash differs")

    return {
        "evaluation_id": evaluation_id,
        "manifest_hash": manifest["manifest_hash"],
        "manifest": manifest,
        "source_hashes": {key: source[key] for key in (
            "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
            "leaderboard_snapshot_hash", "config_hash", "implementation_hash",
        )},
        "allocation_policy": allocation,
        "allocation_policy_hash": allocation_hash,
        "mode_decision_hash": manifest["mode_decision_hash"],
        "funnel_hash": manifest["candidate_funnel_hash"],
        "terminal_config_hash": manifest["config_hash"],
    }


def _terminal_binding_payload(binding, *, pipeline_config_hash):
    return {
        "terminal_evaluation_id": binding["evaluation_id"],
        "terminal_manifest_hash": binding["manifest_hash"],
        "terminal_allocation_policy_hash": binding["allocation_policy_hash"],
        "terminal_mode_decision_hash": binding["mode_decision_hash"],
        "terminal_funnel_hash": binding["funnel_hash"],
        "terminal_config_hash": binding["terminal_config_hash"],
        "pipeline_config_hash": pipeline_config_hash,
        "terminal_source_hashes": binding["source_hashes"],
        # Keep the audit field names used by the terminal contract alongside
        # the explicit terminal_ prefix used to avoid pipeline collisions.
        "manifest_hash": binding["manifest_hash"],
        "allocation_policy_hash": binding["allocation_policy_hash"],
        "mode_decision_hash": binding["mode_decision_hash"],
        "funnel_hash": binding["funnel_hash"],
    }


def _bind_terminal_result(result, binding_payload):
    """Attach terminal provenance to both result projections without enabling it."""

    result["policy"] = dict(result.get("policy") or {})
    result["policy"]["terminal_evaluation_binding"] = dict(binding_payload)
    result["summary"] = dict(result.get("summary") or {})
    result["summary"].update(binding_payload, terminal_shadow_only=True)
    result["execution_ready"] = False
    result["production_accepted"] = False
    result["portfolio_status"] = "NOT_READY"
    return result


def _code():
    return quote_fingerprint() | {name: file_hash(ROOT / name) for name in (
        "competition_core/pipeline/portfolio.py", "competition_core/pipeline/portfolio_runner.py",
        "competition_core/pipeline/holding_review_consumer.py",
        "competition_core/pipeline/rebalance_quantity.py", "competition_core/pipeline/account.py",
        "competition_core/pipeline/instruments.py", "competition_core/pipeline/config.py",
        "competition_core/pipeline/execution_status.py", "competition_core/pipeline/execution_status_runner.py",
        "competition_core/pipeline/persistence.py", "competition_core/pipeline/artifacts.py")}


def _finish_seal(repository, seal, observed_at, account, account_source_hash):
    """Crash recovery commits saved bytes, never resizes against a new clock."""
    directory = confined_path(repository.directory, seal["directory"])
    for name, digest in seal["files"].items():
        if file_hash(confined_path(directory, name)) != digest:
            raise PipelineError("STALE_DATA", "portfolio seal input/result changed")
    result = read_json(directory / "result.json")
    gate = read_json(directory / "gate.json")
    request = read_json(directory / "request.json")
    consumption = read_json(directory / "holding-review-consumption.json")
    if request.get("holding_review_consumption_hash") != content_hash(consumption):
        raise PipelineError("STALE_DATA", "holding review consumption changed during sealing")
    if request["code_hashes"] != _code():
        raise PipelineError("STALE_DATA", "portfolio implementation changed during sealing")
    _, _, _, stages = _inputs(repository)
    if request["stage_hashes"] != {k: v["artifact_hash"] for k, v in stages.items()}:
        raise PipelineError("STALE_DATA", "research changed after portfolio sealing")
    provenance = [{"path": str((directory / name).relative_to(repository.directory)).replace("\\", "/"), "sha256": digest}
                  for name, digest in sorted(seal["files"].items())]
    provenance += request.get("execution_status_provenance", [])
    for proof in provenance:
        if file_hash(confined_path(repository.directory, proof["path"])) != proof["sha256"]:
            raise PipelineError("STALE_DATA", "execution reference proof changed during sealing")
    metadata = request["context"] | {"snapshot_id": request["snapshot_id"], "config_hash": request["config_hash"],
        "source": "frozen_research_market_runtime_explicit_account", "fallback_used": gate["meta"]["fallback_used"],
        "stale": gate["meta"]["stale"]}
    for stage, payload in (("S5_5", gate), ("S6", result)):
        payload = payload | {"source_provenance": payload.get("source_provenance", []) + provenance}
        saved = repository.recover_checkpoint(stage)
        if saved is None:
            repository.start_stage(stage)
            repository.commit_stage(stage, payload, metadata=metadata, elapsed=0)
        elif saved["semantic_hash"] != content_hash(payload):
            raise PipelineError("STALE_DATA", "sealed stage differs from portfolio transaction")
    atomic_json(repository.directory / "portfolio.json", result | {"source_provenance": provenance}, immutable=True)
    current = read_portfolio(repository=repository, observed_at=observed_at,
                             account=account, account_source_hash=account_source_hash)
    repository.update(status="SUCCESS", current_stage="S6", completed_through="S6", pending_stage=None,
                      execution_ready=current["current_execution_ready"], production_accepted=False, error=None)
    return current


def run_portfolio_draft(*, repository, check_id, account, fees, trading_metadata=None,
                        status_id=None, draft_id=None, clock=None, account_source_hash=None,
                        terminal_evaluation_id=None):
    """A new attempt may repair missing inputs without rerunning S0–S5.

    Only a real-source DRAFT_READY is sealed as S5_5/S6. Synthetic math and
    NOT_READY outcomes remain labeled attempts, never production checkpoints.
    """
    check_id = _id(check_id)
    if terminal_evaluation_id is not None:
        terminal_evaluation_id = _terminal_evaluation_id(terminal_evaluation_id)
    if status_id is not None:
        status_id = _id(status_id)
        if trading_metadata is not None:
            raise PipelineError("INVALID_ARGUMENTS", "choose automatic status ID or explicit metadata, not both")
    draft_id = _id(draft_id or f"draft-{uuid.uuid4().hex[:20]}")
    clock = clock or (lambda: datetime.now(SHANGHAI))
    with repository.lock():
        inputs, config, context, stages = _inputs(repository)
        observed_at = clock()
        if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None or observed_at < context.as_of:
            raise PipelineError("NOT_READY", "portfolio requires a valid observation clock")
        seal_path = repository.directory / "portfolio-seal.json"
        if seal_path.exists():
            if terminal_evaluation_id is not None:
                raise PipelineError("NOT_READY", "terminal shadow evaluation cannot resume a sealed portfolio")
            seal = read_json(seal_path)
            if seal.pop("integrity_hash", None) != content_hash(seal):
                raise PipelineError("STALE_DATA", "portfolio seal hash mismatch")
            if seal.get("status_id") != status_id:
                raise PipelineError("STALE_DATA", "sealed execution status ID differs")
            if status_id is not None:
                saved_dir = confined_path(repository.directory, seal["directory"])
                trading_metadata = read_json(saved_dir / "trading-metadata.json", expected_hash=seal["files"]["trading-metadata.json"])
            # Resume is explicit; a different account cannot replace sealed evidence.
            if seal["account_hash"] != (account.snapshot_hash if account else None) or seal["check_id"] != check_id:
                raise PipelineError("NOT_READY", "sealed draft inputs differ; retain the original run")
            if (seal["fee_hash"] != content_hash(fees.to_dict() if fees else None)
                    or seal["trading_metadata_hash"] != content_hash(trading_metadata)):
                raise PipelineError("STALE_DATA", "sealed execution terms differ")
            return _finish_seal(repository, seal, observed_at, account, account_source_hash)
        terminal_binding = None
        terminal_binding_payload = None
        if terminal_evaluation_id is not None:
            terminal_binding = _terminal_binding(
                repository=repository,
                evaluation_id=terminal_evaluation_id,
                stages=stages,
                context=context,
                account=account,
                observed_at=observed_at,
            )
            terminal_binding_payload = _terminal_binding_payload(
                terminal_binding, pipeline_config_hash=config.config_hash)
        status_proofs = []
        if status_id is not None:
            from competition_core.pipeline.execution_status_runner import read_run_execution_status
            status_report, status_proofs = read_run_execution_status(repository=repository, status_id=status_id,
                                                                     account=account, observed_at=observed_at)
            trading_metadata = status_report["metadata"]
        elif trading_metadata is None:
            trading_metadata = {}
        directory = repository.directory / "portfolio-attempts" / draft_id
        if directory.exists():
            raise PipelineError("NOT_READY", "portfolio attempt ID already used; choose a new ID")
        observation_dir = repository.directory / "realtime" / check_id
        observation = read_json(observation_dir / "observation.json")
        if observation.pop("integrity_hash", None) != content_hash(observation):
            raise PipelineError("STALE_DATA", "quote observation hash mismatch")
        response = read_json(observation_dir / "response.json", expected_hash=observation["response_hash"])
        request = read_json(observation_dir / "request.json", expected_hash=observation["request_hash"])
        if (request["context"] != context.to_dict() or request["snapshot_id"] != inputs["snapshot_id"]
                or request["config_hash"] != config.config_hash
                or request["stage_hashes"] != {k: v["artifact_hash"] for k, v in stages.items()}
                or request["code_hashes"] != quote_fingerprint()):
            raise PipelineError("STALE_DATA", "quote observation differs from frozen research/code")
        symbols = list(stages["S4"]["output_symbols"])
        holdings = {p.symbol for p in account.positions} if account else set()
        if set(request["symbols"]) != set(symbols) | holdings:
            raise PipelineError("IDENTITY_CONFLICT", "quotes must cover shortlist plus every current holding")
        if request.get("account_hash") is not None and request["account_hash"] != (account.snapshot_hash if account else None):
            raise PipelineError("STALE_DATA", "quote observation belongs to a different account snapshot")
        if account_source_hash is not None and not re.fullmatch(r"[a-f0-9]{64}", account_source_hash):
            raise PipelineError("INVALID_ACCOUNT_STATE", "invalid account source hash")
        holding_consumption = None
        if account is not None and account_source_hash is not None:
            holding_consumption = validate_holding_review_index(
                repository=repository, account=account, account_source_hash=account_source_hash)
        fingerprint = _code()
        atomic_json(directory / "account.json", account.to_dict() if account else None, immutable=True)
        atomic_json(directory / "fees.json", fees.to_dict() if fees else None, immutable=True)
        atomic_json(directory / "trading-metadata.json", trading_metadata, immutable=True)
        atomic_json(directory / "holding-review-consumption.json", holding_consumption, immutable=True)
        frozen = {"context": context.to_dict(), "snapshot_id": inputs["snapshot_id"], "config_hash": config.config_hash,
                  "stage_hashes": request["stage_hashes"], "code_hashes": fingerprint, "account_source_hash": account_source_hash,
                  "holding_review_consumption_hash": content_hash(holding_consumption),
                  "quote_request_hash": observation["request_hash"], "quote_response_hash": observation["response_hash"],
                  "observed_at": observed_at.isoformat(), "check_id": check_id, "draft_id": draft_id,
                  "status_id": status_id, "execution_status_provenance": status_proofs}
        if terminal_binding_payload is not None:
            frozen.update(terminal_binding_payload)
        atomic_json(directory / "request.json", frozen, immutable=True)
        # Embed the exact source receipt, not just a currently-live cache pointer.
        atomic_json(directory / "quote-response.json", response, immutable=True)
        repository.update(status="RUNNING", current_stage="S6", execution_ready=False, production_accepted=False)
        started = time.monotonic()
        try:
            raw = response.get("transport")
            all_quotes = raw["response"]["quotes"] if raw else []
            benchmark_symbol = config.section("screening")["benchmark_symbol"]
            benchmark_rows = [row for row in all_quotes if row.get("symbol") == benchmark_symbol]
            if len(benchmark_rows) > 1:
                raise PipelineError("IDENTITY_CONFLICT", "saved quote response contains duplicate benchmark rows")
            benchmark_quote = benchmark_rows[0] if benchmark_rows else None
            quotes = [row for row in all_quotes if row.get("symbol") != benchmark_symbol]
            candidate_by = {r["symbol"]: r for r in stages["S4"]["rows"]}
            identities = {r["symbol"]: r.get("identity", {}) for r in stages["S0"]["rows"]}
            candidate_by = {s: row | {"name": identities.get(s, {}).get("name", row.get("name"))}
                            for s, row in candidate_by.items()}
            observed_at = clock()
            if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None or observed_at < datetime.fromisoformat(frozen["observed_at"]):
                raise PipelineError("NOT_READY", "portfolio observation clock moved backwards")
            result = build_portfolio(candidates=[candidate_by[s] for s in symbols], reviews=stages["S5"]["rows"],
                quotes=quotes, account=account, fees=fees, trading_metadata=trading_metadata, context=context,
                config=config, snapshot_id=inputs["snapshot_id"], observed_at=observed_at,
                benchmark_quote=benchmark_quote, holding_dispositions=holding_consumption,
                terminal_policy=(terminal_binding["allocation_policy"] if terminal_binding is not None else None))
            if terminal_binding_payload is not None:
                result = _bind_terminal_result(result, terminal_binding_payload)
                if result.get("execution_ready") or result.get("production_accepted"):
                    raise PipelineError("NOT_READY", "terminal shadow portfolio cannot be execution-ready")
            gate = evaluate_realtime_quotes(symbols=request["symbols"], quotes=quotes, context=context, config=config,
                                            snapshot_id=inputs["snapshot_id"], observed_at=observed_at,
                                            benchmark_quote=benchmark_quote)
            if _code() != fingerprint:
                raise PipelineError("STALE_DATA", "portfolio code changed during calculation")
            atomic_json(directory / "gate.json", gate, immutable=True)
            atomic_json(directory / "result.json", result, immutable=True)
            atomic_json(directory / "timing.json", {"elapsed_seconds": round(time.monotonic() - started, 6)}, immutable=True)
            if result["execution_ready"] and terminal_binding_payload is None:
                seal = {"directory": str(directory.relative_to(repository.directory)).replace("\\", "/"),
                        "account_hash": account.snapshot_hash, "check_id": check_id, "status_id": status_id,
                        "fee_hash": content_hash(fees.to_dict()), "trading_metadata_hash": content_hash(trading_metadata),
                        "files": {name: file_hash(directory / name) for name in (
                            "request.json", "account.json", "fees.json", "trading-metadata.json",
                            "holding-review-consumption.json", "quote-response.json", "gate.json", "result.json")}}
                atomic_json(seal_path, seal | {"integrity_hash": content_hash(seal)}, immutable=True)
                return _finish_seal(repository, seal, clock(), account, account_source_hash)
            repository.update(status="PARTIAL", completed_through="S5", pending_stage="S6",
                              last_portfolio_attempt=draft_id, execution_ready=False, error=None)
            return result
        except Exception as exc:
            error = {"code": exc.code if isinstance(exc, PipelineError) else "PORTFOLIO_DRAFT_FAILED",
                     "exception_type": type(exc).__name__}
            atomic_json(directory / "failure.json", error, immutable=True)
            repository.update(status="PARTIAL", pending_stage="S6", execution_ready=False, error=error)
            raise


def read_portfolio(*, repository, observed_at, account=None, account_source_hash=None):
    """Read saved targets; current readiness requires explicitly rechecked inputs.

    A clock alone cannot prove unchanged account balances/positions/open orders.
    Missing or changed account evidence leaves the saved draft observation-only.
    This validates caller-supplied evidence, not broker authenticity or fills.
    """
    _, config, context, _ = _inputs(repository)
    stage = repository.load_stage("S6")
    if stage is None:
        raise PipelineError("NOT_READY", "no sealed portfolio draft")
    result = read_json(repository.directory / "portfolio.json")
    if content_hash(result) != stage["semantic_hash"]:
        raise PipelineError("STALE_DATA", "published portfolio differs from sealed S6")
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise PipelineError("NOT_READY", "explicit current observation clock required")
    gate = repository.load_stage("S5_5")
    regenerated = evaluate_realtime_quotes(symbols=gate["input_symbols"],
        quotes=[r["quote"] for r in gate["rows"] if r.get("quote")], context=context,
        config=config, snapshot_id=stage["snapshot_id"], observed_at=observed_at)
    required = {r["symbol"] for r in result["rows"] if r["current_quantity"] or r["selected"]}
    fresh = required <= set(regenerated["output_symbols"])
    seal = read_json(repository.directory / "portfolio-seal.json")
    if seal.pop("integrity_hash", None) != content_hash(seal):
        raise PipelineError("STALE_DATA", "portfolio seal hash mismatch")
    directory = confined_path(repository.directory, seal["directory"])
    request = read_json(directory / "request.json", expected_hash=seal["files"]["request.json"])
    metadata = read_json(directory / "trading-metadata.json", expected_hash=seal["files"]["trading-metadata.json"])
    metadata_fresh = all(_metadata(metadata.get(s), symbol=s, context=context, observed_at=observed_at,
                                  max_age_seconds=config.section("realtime")["max_quote_age_seconds"]) is None
                         for s in required)
    implementation_current = request["code_hashes"] == _code()
    assessment = assess_account(account, context=context, config=config, observed_at=observed_at)
    account_matches = account is not None and account.snapshot_hash == seal["account_hash"]
    source_matches = (request.get("account_source_hash") is None
                      or request["account_source_hash"] == account_source_hash)
    account_current = assessment["account_eligible"] and account_matches and source_matches
    account_reasons = list(assessment["reasons"])
    if account is not None and not account_matches:
        account_reasons.append("ACCOUNT_CHANGED_SINCE_DRAFT")
    if not source_matches:
        account_reasons.append("ACCOUNT_SOURCE_NOT_REVALIDATED")
    current = (result["execution_ready"] and fresh and metadata_fresh and implementation_current
               and account_current
               and observed_at.astimezone(SHANGHAI).date() == context.research_date)
    return result | {"execution_ready": current, "execution_ready_at_generation": result["execution_ready"],
                     "current_execution_ready": current, "freshness_checked_at": observed_at.isoformat(),
                     "current_account_revalidation": {"matches_sealed_snapshot": account_matches,
                         "source_hash_matches": source_matches, "reasons": account_reasons},
                     "historical_draft_only": not current, "freshness": {
                         "quotes": fresh, "trading_status": metadata_fresh, "implementation": implementation_current,
                         "account": account_current}}

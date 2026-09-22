"""Historical UI portfolio projection and explicit manual-review receipts, never orders."""

from datetime import datetime
from collections import Counter
import re
import uuid

from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, file_hash, read_json, content_hash, confined_path
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.portfolio_runner import read_portfolio
from competition_core.pipeline.time_context import SHANGHAI
from competition_core.research_tasks import checked_directory, task_lease, lease_busy

_CHECKS = ("blocking_reasons", "marked_asset", "cash_reserve", "required_buy_cash", "projected_net_sell_proceeds",
           "unfilled_sale_proceeds_spent", "cash_remaining", "available_cash_without_sale_fills", "cash_ratio",
           "estimated_fees", "target_invested", "max_single_weight", "role_values", "constraints_relaxed")
_ROW = ("symbol", "selected", "rank", "role", "current_quantity", "sellable_quantity", "target_quantity",
        "delta_quantity", "target_weight", "order_price", "reference_price", "reference_time", "side",
        "keep_reasons", "reject_reasons", "reasons", "missing_fields", "estimated_fees", "ai_verdict",
        "holding_disposition", "realtime_gate")
_TERMINAL_ID = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")
_SHADOW_ROW = ("symbol", "selected", "role", "current_quantity", "sellable_quantity",
               "target_quantity", "delta_quantity", "target_weight", "reasons", "reject_reasons")


def portfolio_view(runs_root, run_id, observed_at):
    from competition_core.pipeline_views import get_run_view, get_stage_rows
    # Verifies confined paths and sealed stage/source hashes before repository use.
    view = get_run_view(runs_root, run_id, observed_at)
    directory = checked_directory(runs_root, run_id, existing=False)
    repository = PipelineRepository(runs_root, run_id)
    sealed = repository.load_stage("S6")
    if sealed is not None:
        result = read_portfolio(repository=repository, observed_at=observed_at)
        return {"run_id": run_id, "portfolio_status": result["portfolio_status"],
            "portfolio_hash": file_hash(directory / "portfolio.json"), "rows": get_stage_rows(runs_root, run_id, "S6")["rows"],
            "risk_checks": {key: result["risk_checks"][key] for key in _CHECKS if key in result["risk_checks"]},
            "current_execution_ready": False, "execution_ready_at_generation": result["execution_ready_at_generation"],
            "historical_draft_only": True, "manual_confirmation_requires_current_inputs": True,
            "production_accepted": False, "context": view["context"]}
    state = repository.state()
    draft_id = state.get("last_portfolio_attempt")
    if draft_id is None:
        return {"run_id": run_id, "portfolio_status": "WAITING", "rows": [], "risk_checks": {},
                "current_execution_ready": False, "production_accepted": False, "context": view["context"]}
    # Identifier syntax and confinement are independent of the mutable state pointer.
    from competition_core.pipeline.portfolio_runner import _id
    folder = confined_path(directory, f"portfolio-attempts/{_id(draft_id)}")
    result = read_json(confined_path(folder, "result.json"))
    request = read_json(confined_path(folder, "request.json"))
    if request["context"] != view["context"] or request["snapshot_id"] != view["snapshot_id"]:
        raise PipelineError("STALE_DATA", "portfolio attempt belongs to different research")
    # Failed attempts are diagnostics, not sealed/verified target allocations.
    return {"run_id": run_id, "portfolio_status": "NOT_READY", "attempt_id": draft_id,
        "rows": [{key: row[key] for key in _ROW if key in row and key not in {"target_quantity", "delta_quantity", "target_weight", "order_price"}}
                 for row in result.get("rows", [])[:50]],
        "risk_checks": {"blocking_reasons": result.get("risk_checks", {}).get("blocking_reasons", []),
                        "realtime_gate_counts_at_observation": dict(Counter(row.get("realtime_gate", "UNKNOWN") for row in result.get("rows", []))),
                        "unsealed_attempt_diagnostic": True},
        "current_execution_ready": False, "production_accepted": False, "context": view["context"]}


def create_terminal_shadow_plan(*, tasks, run_id, evaluation_id=None):
    """Create a bounded terminal portfolio attempt; never create execution output."""

    directory = checked_directory(tasks.root, run_id)
    if tasks.status(run_id).get("worker_live"):
        raise PipelineError("TASK_BUSY", "wait until the research worker has finished")
    lock_path = directory / ".task-worker.lock"
    if lock_path.is_symlink():
        raise PipelineError("STALE_DATA", "task lock path is invalid")
    with task_lease(lock_path, create=True):
        repository = PipelineRepository(tasks.root, run_id)
        if repository.load_stage("S6") is not None or (directory / "portfolio-seal.json").exists():
            raise PipelineError("NOT_READY", "terminal shadow plan cannot replace a sealed portfolio")
        from competition_core.terminal_strategy.api_views import latest_persisted
        latest = latest_persisted(tasks.root, run_id)
        manifest = latest["manifest"]
        latest_id = manifest.get("evaluation_id")
        if not isinstance(latest_id, str) or _TERMINAL_ID.fullmatch(latest_id) is None:
            raise PipelineError("STALE_DATA", "latest terminal evaluation identity is invalid")
        if evaluation_id is not None:
            if not isinstance(evaluation_id, str) or _TERMINAL_ID.fullmatch(evaluation_id) is None:
                raise PipelineError("INVALID_ARGUMENTS", "invalid terminal evaluation identifier")
            if evaluation_id != latest_id:
                raise PipelineError("STALE_DATA", "terminal plan must use the latest evaluation")
        if manifest.get("status") != "COMPLETE":
            raise PipelineError("NOT_READY", "terminal evaluation is not complete")
        allocation = latest["artifacts"].get("allocation-policy.json")
        allocation_hash = manifest.get("allocation_policy_hash")
        if (not isinstance(allocation, dict) or allocation.get("available") is False
                or not isinstance(allocation_hash, str) or _TERMINAL_ID.fullmatch(allocation_hash) is None
                or allocation.get("policy_hash") != allocation_hash
                or allocation.get("execution_ready") is not False):
            raise PipelineError("NOT_READY", "terminal allocation policy is unavailable")

        state = repository.state()
        check_id, status_id = state.get("last_realtime_check"), state.get("last_execution_status")
        if (not isinstance(check_id, str) or _RUN_ID.fullmatch(check_id) is None
                or not isinstance(status_id, str) or _RUN_ID.fullmatch(status_id) is None):
            raise PipelineError("NOT_READY", "latest realtime and execution status observations are required")

        # A shadow plan must use immutable imported account evidence.  The
        # API helper validates the Ninef receipt; the worker helper supplies
        # the exact content-addressed account/fee source hash for the runner.
        from competition_core.research_api import _imported_terminal_account_view
        from competition_core.research_worker import _execution_inputs
        imported_view = _imported_terminal_account_view(tasks, run_id)
        if imported_view is None:
            raise PipelineError("NOT_READY", "complete imported Ninef account evidence is required")
        account, fees, account_source_hash = _execution_inputs(directory)
        if account is None or fees is None or not isinstance(account_source_hash, str):
            raise PipelineError("NOT_READY", "complete execution inputs are required")
        if imported_view.get("account_snapshot_hash") != account.snapshot_hash:
            raise PipelineError("STALE_DATA", "imported account evidence changed")

        from competition_core.pipeline.portfolio_runner import run_portfolio_draft
        s55_path = directory / "S5_5.json"
        s55_before = file_hash(s55_path) if s55_path.is_file() and not s55_path.is_symlink() else None
        draft_id = f"terminal-{uuid.uuid4().hex}"
        result = run_portfolio_draft(
            repository=repository, check_id=check_id, status_id=status_id,
            account=account, fees=fees, account_source_hash=account_source_hash,
            clock=tasks.clock, draft_id=draft_id, terminal_evaluation_id=latest_id,
        )
        if (result.get("execution_ready") is not False
                or result.get("production_accepted") is not False
                or repository.load_stage("S6") is not None
                or (directory / "portfolio-seal.json").exists()):
            raise PipelineError("STALE_DATA", "terminal shadow plan crossed an execution boundary")
        s55_after = file_hash(s55_path) if s55_path.is_file() and not s55_path.is_symlink() else None
        if s55_after != s55_before:
            raise PipelineError("STALE_DATA", "terminal shadow plan changed S5_5")

        attempt = confined_path(directory, f"portfolio-attempts/{draft_id}")
        request = read_json(confined_path(attempt, "request.json"))
        if (request.get("terminal_evaluation_id") != latest_id
                or request.get("terminal_manifest_hash") != manifest.get("manifest_hash")
                or request.get("terminal_allocation_policy_hash") != allocation_hash):
            raise PipelineError("STALE_DATA", "terminal shadow binding is incomplete")
        checks = result.get("risk_checks") if isinstance(result.get("risk_checks"), dict) else {}
        bounded_checks = {key: checks[key] for key in _CHECKS if key in checks}
        bounded_checks.update({"terminal_policy_shadow_only": True,
                               "blocking_reasons": list(checks.get("blocking_reasons", []))})
        rows = []
        for row in result.get("rows", [])[:50]:
            if isinstance(row, dict):
                rows.append({key: row[key] for key in _SHADOW_ROW if key in row})
        source = latest["source_hashes"]
        return {
            "schema_version": "competition.terminal-shadow-plan.v1",
            "run_id": run_id, "evaluation_id": latest_id, "attempt_id": draft_id,
            "status": "SHADOW", "portfolio_status": result.get("portfolio_status", "NOT_READY"),
            "mode": allocation.get("mode"),
            "allocation": {key: allocation.get(key) for key in (
                "core_symbols", "aggressive_symbols", "core_budget", "aggressive_budget",
                "cash_target", "total_position_count", "max_single_weight",
                "same_day_exit_unavailable")},
            "rows": rows, "risk_checks": bounded_checks,
            "binding": {
                "terminal_evaluation_id": latest_id,
                "terminal_manifest_hash": manifest.get("manifest_hash"),
                "allocation_policy_hash": allocation_hash,
                "mode_decision_hash": manifest.get("mode_decision_hash"),
                "funnel_hash": manifest.get("candidate_funnel_hash"),
                "source_hashes": {key: source.get(key) for key in (
                    "s4_hash", "s5_hash", "s5_5_hash", "account_snapshot_hash",
                    "leaderboard_snapshot_hash", "config_hash", "implementation_hash")},
                "account_import_hash": account_source_hash,
                "check_id": check_id, "status_id": status_id,
            },
            "shadow_only": True, "execution_ready": False,
            "production_accepted": False, "orders_submitted": 0,
        }


def confirm_review(*, tasks, run_id, portfolio_hash, account, fees, reviewed, observed_at=None):
    """Explicit user resubmission/revalidation; records review without submitting orders."""
    if reviewed is not True:
        raise PipelineError("INVALID_ARGUMENTS", "explicit review acknowledgement is required")
    observed_at = observed_at or datetime.now(SHANGHAI)
    directory = checked_directory(tasks.root, run_id)
    account, fees = AccountSnapshot.from_dict(account), FeeSchedule.from_dict(fees)
    if tasks.status(run_id)["worker_live"]:
        raise PipelineError("TASK_BUSY", "wait until the research worker has finished")
    with task_lease(directory / ".task-worker.lock"):
        from competition_core.pipeline_views import get_run_view
        get_run_view(tasks.root, run_id, observed_at)
        repository = PipelineRepository(tasks.root, run_id)
        with repository.lock():
            if portfolio_hash != file_hash(confined_path(directory, "portfolio.json")):
                raise PipelineError("STALE_DATA", "the reviewed portfolio differs from the saved draft")
            source_hash = content_hash({"account": account.to_dict(), "fees": fees.to_dict()})
            result = read_portfolio(repository=repository, observed_at=observed_at,
                                    account=account, account_source_hash=source_hash)
            if not result["current_execution_ready"]:
                raise PipelineError("NOT_READY", "current account/quotes/status do not support confirmation")
            if result["fee_schedule"] != fees.to_dict():
                raise PipelineError("STALE_DATA", "competition costs changed since the draft")
            confirmation_id = uuid.uuid4().hex
            receipt = {"run_id": run_id, "confirmation_id": confirmation_id, "portfolio_hash": portfolio_hash,
                "reviewed_at": observed_at.isoformat(), "account_hash": account.snapshot_hash,
                "account_source_hash": source_hash, "kind": "MANUAL_REVIEW_ONLY", "orders_submitted": 0,
                "production_accepted": False, "execution_ready": False,
                "must_revalidate_before_any_order": True}
            atomic_json(directory / "manual-confirmations" / f"{confirmation_id}.json", receipt, immutable=True)
            return receipt

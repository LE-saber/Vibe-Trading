"""Historical UI portfolio projection and explicit manual-review receipts, never orders."""

from datetime import datetime
from collections import Counter
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

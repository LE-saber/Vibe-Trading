"""Synthetic manual-review receipts; no actual account or order acceptance."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.portfolio_runner import run_portfolio_draft
from competition_core.research_portfolio import portfolio_view, confirm_review
from competition_core.research_tasks import task_lease
from tests.integration.test_portfolio_pipeline import setup


def sealed(tmp_path):
    repo, kwargs = setup(tmp_path, declared=True)
    body = {"account": kwargs["account"].to_dict(), "fees": kwargs["fees"].to_dict()}
    run_portfolio_draft(**kwargs, draft_id="synthetic-seal", account_source_hash=content_hash(body))
    atomic_json(repo.directory / "task-request.json", {"synthetic": True})
    with task_lease(repo.directory / ".task-worker.lock", create=True):
        pass
    tasks = SimpleNamespace(root=repo.directory.parent, status=lambda run: {"worker_live": False})
    return repo, kwargs, body, tasks


def test_get_portfolio_is_historical_and_never_implies_current_readiness(tmp_path):
    repo, kwargs, body, tasks = sealed(tmp_path)
    result = portfolio_view(tasks.root, repo.run_id, kwargs["account"].as_of)
    assert result["execution_ready_at_generation"] and not result["current_execution_ready"]
    assert result["historical_draft_only"] and len(result["rows"]) == 12
    assert result["portfolio_hash"] == file_hash(repo.directory / "portfolio.json")
    assert "account_assessment" not in result and "source_provenance" not in result


def test_current_explicit_confirmation_records_review_but_no_orders(tmp_path):
    repo, kwargs, body, tasks = sealed(tmp_path)
    result = confirm_review(tasks=tasks, run_id=repo.run_id, **body, reviewed=True,
        portfolio_hash=file_hash(repo.directory / "portfolio.json"), observed_at=kwargs["account"].as_of)
    assert result["kind"] == "MANUAL_REVIEW_ONLY"
    assert result["orders_submitted"] == 0 and not result["execution_ready"] and not result["production_accepted"]
    assert (repo.directory / "manual-confirmations" / (result["confirmation_id"] + ".json")).exists()


@pytest.mark.parametrize("change", ["hash", "account", "fees", "age", "acknowledgement"])
def test_confirmation_rejects_changed_stale_or_unacknowledged_draft(tmp_path, change):
    repo, kwargs, body, tasks = sealed(tmp_path)
    params = dict(tasks=tasks, run_id=repo.run_id, **body, reviewed=True,
        portfolio_hash=file_hash(repo.directory / "portfolio.json"), observed_at=kwargs["account"].as_of)
    if change == "hash":
        params["portfolio_hash"] = "0" * 64
    elif change == "account":
        params["account"] = body["account"] | {"available_cash": "1.00"}
    elif change == "fees":
        params["fees"] = body["fees"] | {"commission_rate": "0.002"}
    elif change == "age":
        params["observed_at"] += timedelta(seconds=61)
    else:
        params["reviewed"] = False
    with pytest.raises(PipelineError):
        confirm_review(**params)
    assert not (repo.directory / "manual-confirmations").exists()


def test_failed_attempt_is_diagnostic_without_executable_quantities(tmp_path):
    repo, kwargs = setup(tmp_path)
    run_portfolio_draft(**(kwargs | {"fees": None}), draft_id="missing-fees")
    result = portfolio_view(repo.directory.parent, repo.run_id, kwargs["account"].as_of)
    assert result["portfolio_status"] == "NOT_READY" and not result["current_execution_ready"]
    assert "MISSING_COMPETITION_FEE_SCHEDULE" in result["risk_checks"]["blocking_reasons"]
    assert all("target_quantity" not in row and "delta_quantity" not in row for row in result["rows"])

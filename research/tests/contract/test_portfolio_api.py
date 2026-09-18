from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from competition_core.api import app, get_workflow_repository
from competition_core.storage.workflow import WorkflowRepository
from contracts.portfolio import PortfolioState


def test_portfolio_api_is_read_only_and_reports_latest_state(tmp_path) -> None:
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    repository.put_portfolio(
        PortfolioState(
            as_of=datetime(2026, 9, 1, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            cash=1_000_000,
            positions=(),
            total_assets=1_000_000,
            initial_capital=1_000_000,
            source="postclose_mark_to_market",
        )
    )
    app.dependency_overrides[get_workflow_repository] = lambda: repository
    try:
        with TestClient(app) as client:
            response = client.get("/portfolio/state")
            method = client.post("/portfolio/state", json={})
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["total_assets"] == 1_000_000
    assert method.status_code == 405


def test_portfolio_api_reports_missing_account_state_structured(tmp_path) -> None:
    repository = WorkflowRepository(tmp_path / "state.sqlite")
    app.dependency_overrides[get_workflow_repository] = lambda: repository
    try:
        with TestClient(app) as client:
            response = client.get("/portfolio/state")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {
        "code": "MISSING_ACCOUNT_STATE",
        "message": "portfolio state is not available",
        "details": {"resource": "portfolio_state"},
    }

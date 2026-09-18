"""Loopback execution API contract; fake provider only, no external writes."""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
import pytest

from competition_core.api import app
from competition_core.execution.service import ExecutionService
from competition_core.execution.store import ExecutionStore
from competition_core.research_api import (
    get_execution_service,
    get_execution_supervisor,
    get_tasks,
)
from competition_core.research_tasks import ResearchTasks
from tests.pipeline_fixtures import context
from tests.unit.test_execution_service import FakeProvider, NOW, sealed_plan


class Supervisor:
    def __init__(self):
        self.started = []

    def start(self, batch_id):
        self.started.append(batch_id)


SERVICE_TOKEN = "synthetic-core-service-token-0001"
HEADERS = {"X-Competition-Request": "1", "X-Competition-Service-Token": SERVICE_TOKEN}


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPETITION_CORE_SERVICE_TOKEN", SERVICE_TOKEN)
    tasks = ResearchTasks(tmp_path / "runs", launch=lambda *args, **kwargs: None, clock=lambda: context().as_of)
    service_store = ExecutionStore(tmp_path / "execution.sqlite")
    provider = FakeProvider(service_store)
    service = ExecutionService(service_store, provider, writes_enabled=True)
    supervisor = Supervisor()
    app.dependency_overrides[get_tasks] = lambda: tasks
    app.dependency_overrides[get_execution_service] = lambda: service
    app.dependency_overrides[get_execution_supervisor] = lambda: supervisor
    with TestClient(app) as client:
        yield client, tasks, service, provider, supervisor
    app.dependency_overrides.clear()


def test_controlled_run_plan_is_registered_without_browser_payload(api, monkeypatch):
    client, tasks, service, _, _ = api
    run_id = "run-one"
    (tasks.root / run_id).mkdir(parents=True)
    (tasks.root / run_id / "task-request.json").write_text("{}", encoding="utf-8")
    plan = sealed_plan()
    monkeypatch.setattr("competition_core.pipeline.execution_plan.create_execution_plan", lambda repository: plan)
    response = client.post(f"/pipeline/runs/{run_id}/execution-plan", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["plan_hash"] == plan["plan_hash"]
    assert service.store.plan_document(plan["plan_id"])["plan"] == plan


def test_plan_approval_start_and_batch_views_are_separate_actions(api):
    client, _, service, provider, supervisor = api
    plan = sealed_plan(); service.register_plan(plan)
    expiry = datetime.now(timezone.utc) + timedelta(minutes=5)
    approval = client.post(
        f"/pipeline/execution/plans/{plan['plan_id']}/approve", headers=HEADERS,
        json={"expected_plan_hash": plan["plan_hash"],
              "expires_at": expiry.isoformat()},
    )
    assert approval.status_code == 200 and approval.json()["orders_submitted"] == 0
    approval_id = approval.json()["approval"]["approval_id"]
    started = client.post(f"/pipeline/execution/approvals/{approval_id}/start", headers=HEADERS)
    assert started.status_code == 202
    batch_id = started.json()["batch_id"]
    assert supervisor.started == [batch_id] and provider.submit_calls == 0
    assert client.get(f"/pipeline/execution/batches/{batch_id}").json()["state"] == "QUEUED"
    assert client.get("/pipeline/execution/batches").json()["batches"][0]["id"] == batch_id


def test_execution_mutations_require_explicit_same_origin_header(api):
    client, _, service, provider, _ = api
    plan = sealed_plan(); service.register_plan(plan)
    response = client.post(f"/pipeline/execution/plans/{plan['plan_id']}/approve", json={
        "expected_plan_hash": plan["plan_hash"],
        "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
    })
    assert response.status_code == 403
    assert provider.submit_calls == 0


def test_execution_mutations_reject_missing_or_wrong_service_token(api):
    client, _, service, provider, _ = api
    plan = sealed_plan(); service.register_plan(plan)
    body = {"expected_plan_hash": plan["plan_hash"],
            "expires_at": (NOW + timedelta(minutes=5)).isoformat()}
    missing = client.post(f"/pipeline/execution/plans/{plan['plan_id']}/approve",
                          headers={"X-Competition-Request": "1"}, json=body)
    wrong = client.post(f"/pipeline/execution/plans/{plan['plan_id']}/approve",
                        headers={"X-Competition-Request": "1",
                                 "X-Competition-Service-Token": "wrong" * 8}, json=body)
    assert missing.status_code == wrong.status_code == 403
    assert provider.submit_calls == 0


def test_stop_is_not_cancel(api):
    client, _, service, provider, _ = api
    plan = sealed_plan(); service.register_plan(plan)
    approval = service.approve(plan["plan_id"], plan["plan_hash"], "operator",
                               expires_at=NOW + timedelta(minutes=5), now=NOW)
    batch = service.start(approval.approval_id, now=NOW)
    response = client.post(f"/pipeline/execution/batches/{batch}/stop", headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["orders_cancelled"] == 0
    assert provider.submit_calls == provider.cancel_calls == 0

"""Local-only asynchronous API contracts; no real network/model requests."""

from datetime import timedelta

from fastapi.testclient import TestClient
import pytest

from competition_core.api import app
from competition_core.pipeline.artifacts import atomic_json, read_json
from competition_core.research_api import get_ninef_account_reader, get_ninef_leaderboard_reader, get_tasks
from competition_core.research_tasks import ResearchTasks
from tests.pipeline_fixtures import context


SERVICE_TOKEN = "synthetic-core-service-token-0001"
HEADERS = {"X-Competition-Request": "1", "X-Competition-Service-Token": SERVICE_TOKEN}


class Child:
    def poll(self):
        return None


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPETITION_CORE_SERVICE_TOKEN", SERVICE_TOKEN)
    calls = []
    tasks = ResearchTasks(tmp_path / "runs", launch=lambda *a, **k: calls.append((a,k)) or Child(), clock=lambda: context().as_of)
    app.dependency_overrides[get_tasks] = lambda: tasks
    with TestClient(app) as client:
        yield client, tasks, calls
    app.dependency_overrides.pop(get_tasks)


def body():
    return {"research_date": context().research_date.isoformat(), "phase": "intraday",
            "as_of": context().as_of.isoformat(), "ai_review": True}


def test_start_returns_accepted_job_without_waiting_for_research(client):
    client, tasks, calls = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    assert result.status_code == 202 and len(calls) == 1
    assert result.json()["worker_live"] and not result.json()["current_execution_ready"]
    listing = client.get("/pipeline/runs").json()
    assert listing["total"] == 1 and listing["runs"][0]["program_owned"]
    assert str(tasks.root) not in result.text


def test_list_and_detail_converge_to_matching_terminal_attempt(client):
    client, tasks, calls = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    run_id = result.json()["run_id"]
    directory = tasks.root / run_id
    tasks._children.pop(run_id)
    attempt_id = read_json(directory / "task-state.json")["attempt_id"]
    atomic_json(directory / "task-attempts" / f"{attempt_id}.json", {
        "attempt_id": attempt_id,
        "status": "FAILED",
        "active_phase": "PREPARE",
        "updated_at": context().as_of.isoformat(),
        "execution_ready": False,
        "error": {
            "code": "NON_TRADING_DAY",
            "message": "research date is not an open SSE trading day",
            "failed_step": "PREPARE",
            "retryable": False,
            "diagnostic_id": "diag-" + "c" * 32,
        },
    }, immutable=True)

    detail = client.get(f"/pipeline/runs/{run_id}").json()["task"]
    listing = client.get("/pipeline/runs").json()["runs"][0]

    assert detail["status"] == listing["status"] == "FAILED"
    assert detail["error"]["code"] == listing["error"]["code"] == "NON_TRADING_DAY"
    assert detail["state_source"] == "task_attempt"


def test_event_stream_replays_from_cursor_and_then_closes(client):
    client, tasks, _ = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    run_id = result.json()["run_id"]
    tasks._children.pop(run_id)

    response = client.get(f"/pipeline/runs/{run_id}/events", headers={"Last-Event-ID": "0"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "id: 1\nevent: competition.progress\n" in response.text
    assert '"kind":"QUEUED"' in response.text


def test_event_stream_rejects_conflicting_cursors(client):
    client, tasks, _ = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    run_id = result.json()["run_id"]
    tasks._children.pop(run_id)

    response = client.get(f"/pipeline/runs/{run_id}/events?after=1", headers={"Last-Event-ID": "0"})

    assert response.status_code == 422
    assert response.json()["code"] == "INVALID_ARGUMENTS"


def test_cancel_is_an_explicit_research_only_action(client):
    client, tasks, _ = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    run_id = result.json()["run_id"]
    denied = client.post(f"/pipeline/runs/{run_id}/cancel")
    accepted = client.post(f"/pipeline/runs/{run_id}/cancel", headers=HEADERS)
    assert denied.status_code == 403
    assert accepted.status_code == 202 and accepted.json()["cancel_requested"] is True
    attempt = read_json(tasks.root / run_id / "task-state.json")["attempt_id"]
    request = read_json(tasks.root / run_id / "cancel-requests" / f"{attempt}.json")
    assert request["scope"] == "RESEARCH_ONLY" and request["broker_orders_affected"] is False


@pytest.mark.parametrize("headers", [{}, {"X-Competition-Request":"1", "Origin":"https://malicious.example"},
                                      {"X-Competition-Request":"1", "Host":"malicious.example"}])
def test_cross_origin_or_implicit_write_never_starts_task(client, headers):
    client, tasks, calls = client
    assert client.post("/pipeline/runs", json=body(), headers=headers).status_code == 403
    assert not calls and not tasks.root.exists()


def test_direct_core_write_rejects_missing_or_wrong_proxy_token(client):
    client, tasks, calls = client
    missing = client.post("/pipeline/runs", json=body(), headers={"X-Competition-Request": "1"})
    wrong = client.post("/pipeline/runs", json=body(), headers={
        "X-Competition-Request": "1", "X-Competition-Service-Token": "wrong" * 8})
    assert missing.status_code == wrong.status_code == 403
    assert not calls and not tasks.root.exists()


def test_input_error_does_not_echo_account_or_secrets(client):
    client, _, calls = client
    result = client.post("/pipeline/runs", json=body() | {"token":"private-marker"}, headers=HEADERS)
    assert result.status_code == 422 and "private-marker" not in result.text and not calls


@pytest.mark.parametrize("value", ["true", 1, None])
def test_ai_authorization_requires_json_boolean(client, value):
    client, _, calls = client
    response = client.post("/pipeline/runs", json=body() | {"ai_review": value}, headers=HEADERS)
    assert response.status_code == 422 and not calls


def test_remote_peer_cannot_use_pipeline_api(client):
    _, _, calls = client
    with TestClient(app, client=("192.0.2.20", 50000)) as remote:
        assert remote.get("/pipeline/runs").status_code == 403
    assert not calls


def test_ninef_account_import_is_an_explicit_local_action(client, monkeypatch):
    client, tasks, calls = client
    result = client.post("/pipeline/runs", json=body(), headers=HEADERS)
    run_id = result.json()["run_id"]
    calls[0][0][0]  # The research worker remains synthetic in this contract test.
    tasks._children[run_id].result = 0
    monkeypatch.setattr(tasks, "import_ninef_execution_inputs", lambda value: {
        "run_id": value, "source_kind": "competition_api", "as_of": context().as_of.isoformat(),
        "position_count": 4, "pending_order_count": 12, "orders_submitted": 0,
    })
    denied = client.post(f"/pipeline/runs/{run_id}/execution-inputs/ninef", json={})
    assert denied.status_code == 403
    accepted = client.post(f"/pipeline/runs/{run_id}/execution-inputs/ninef", json={},
                           headers=HEADERS)
    assert accepted.status_code == 200
    assert accepted.json()["pending_order_count"] == 12
    assert "available_cash" not in accepted.text and "token" not in accepted.text


def test_authenticated_frontend_views_keep_account_and_leaderboard_reads_separate(client):
    client, _, calls = client
    app.dependency_overrides[get_ninef_account_reader] = lambda: lambda: {
        "schema_version": "competition.account-view.v1", "account": {"available_cash": "123.00"},
        "orders": {"complete": True, "count": 2, "open_count": 1, "items": []},
        "read_only": True, "orders_submitted": 0,
    }
    app.dependency_overrides[get_ninef_leaderboard_reader] = lambda: lambda: {
        "schema_version": "ninef.leaderboard.v1", "competition_id": "fixture",
        "entries": [], "own": {"rank": 88}, "read_only": True, "orders_submitted": 0,
    }
    try:
        account = client.get("/pipeline/ninef/account")
        leaderboard = client.get("/pipeline/ninef/leaderboard")
    finally:
        app.dependency_overrides.pop(get_ninef_account_reader)
        app.dependency_overrides.pop(get_ninef_leaderboard_reader)
    assert account.status_code == leaderboard.status_code == 200
    assert account.json()["orders"]["open_count"] == 1
    assert leaderboard.json()["own"]["rank"] == 88
    assert not calls

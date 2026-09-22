"""Read-only terminal API contracts with synthetic pipeline and account views."""

from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from competition_core.api import app
from competition_core.pipeline.artifacts import atomic_json
from competition_core.pipeline import persistence as pipeline_persistence
from competition_core.research_api import (
    get_ninef_account_reader,
    get_ninef_leaderboard_reader,
    get_tasks,
)
from competition_core.research_tasks import ResearchTasks
from competition_core.terminal_strategy import TerminalEvaluationPersistence, TerminalStrategyService

# Existing terminal unit fixture modules intentionally use their historical
# short imports, so make that fixture directory available for this contract
# module without changing production import paths.
_UNIT_DIR = Path(__file__).resolve().parents[1] / "unit"
if str(_UNIT_DIR) not in sys.path:
    sys.path.insert(0, str(_UNIT_DIR))

from test_terminal_strategy_pipeline_adapter import _inputs as candidate_inputs
from test_terminal_strategy_runtime import _account, _config
from test_terminal_strategy_service import _request


SERVICE_TOKEN = "synthetic-core-service-token-0001"
HEADERS = {
    "X-Competition-Request": "1",
    "X-Competition-Service-Token": SERVICE_TOKEN,
}
SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 9, 22, 13, 15, tzinfo=SHANGHAI)


def _task(tasks, run_id: str):
    directory = tasks.root / run_id
    directory.mkdir(parents=True, exist_ok=True)
    atomic_json(directory / "task-request.json", {"run_id": run_id}, immutable=True)
    return directory


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPETITION_CORE_SERVICE_TOKEN", SERVICE_TOKEN)
    tasks = ResearchTasks(tmp_path / "runs", clock=lambda: AS_OF)
    app.dependency_overrides[get_tasks] = lambda: tasks
    with TestClient(app) as value:
        yield value, tasks
    app.dependency_overrides.pop(get_tasks, None)
    app.dependency_overrides.pop(get_ninef_account_reader, None)
    app.dependency_overrides.pop(get_ninef_leaderboard_reader, None)


def _persisted(client, tasks):
    run_id = "terminal-fixture-run"
    _task(tasks, run_id)
    result = TerminalStrategyService().evaluate(_request())
    TerminalEvaluationPersistence(tasks.root).persist(result)
    return run_id, result


def test_get_views_are_hash_verified_and_read_only(client):
    client, tasks = client
    run_id, result = _persisted(client, tasks)

    decision = client.get(f"/pipeline/runs/{run_id}/terminal/decision")
    assert decision.status_code == 200
    assert decision.json()["evaluation_id"] == result.evaluation_id
    assert decision.json()["execution_ready"] is False
    assert decision.json()["orders_submitted"] == 0

    candidates = client.get(f"/pipeline/runs/{run_id}/terminal/candidates?tier=terminal")
    assert candidates.status_code == 200
    assert candidates.json()["tier"] == "terminal"
    history = client.get(f"/pipeline/runs/{run_id}/terminal/history?limit=1")
    assert history.status_code == 200 and len(history.json()["history"]) == 1
    status = client.get("/pipeline/terminal/status")
    assert status.status_code == 200
    assert any(row["run_id"] == run_id for row in status.json()["runs"])


def test_cutoff_null_evaluate_is_auditable_unavailable_and_readers_only(client, monkeypatch):
    client, tasks = client
    run_id = "terminal-evaluate-fixture"
    _task(tasks, run_id)
    values = candidate_inputs()
    calls = {"account": 0, "leaderboard": 0, "stages": []}

    class VerifiedRepository:
        def __init__(self, root, value):
            assert Path(root) == tasks.root
            assert value == run_id

        def load_stage(self, stage):
            calls["stages"].append(stage)
            return values[{"S4": "s4_stage", "S5": "s5_stage", "S5_5": "s5_5_stage"}[stage]]

    monkeypatch.setattr(pipeline_persistence, "PipelineRepository", VerifiedRepository)

    def account_reader():
        calls["account"] += 1
        return _account()

    def leaderboard_reader():
        calls["leaderboard"] += 1
        from test_terminal_strategy_snapshot_adapter import _leaderboard_view
        return _leaderboard_view()

    app.dependency_overrides[get_ninef_account_reader] = lambda: account_reader
    app.dependency_overrides[get_ninef_leaderboard_reader] = lambda: leaderboard_reader
    response = client.post(
        f"/pipeline/runs/{run_id}/terminal/evaluate",
        json={"same_day_exit_unavailable_symbols": ["600001.SH"]},
        headers=HEADERS,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "UNAVAILABLE"
    assert body["mode"] == "UNAVAILABLE"
    assert body["execution_ready"] is False and body["orders_submitted"] == 0
    assert body["forecast"]["available"] is False
    assert calls["account"] == calls["leaderboard"] == 1
    assert calls["stages"] == ["S4", "S5", "S5_5"]


def test_evaluate_uses_latest_verified_realtime_observation_when_s5_5_is_uncommitted(
    client, monkeypatch,
):
    client, tasks = client
    run_id = "terminal-realtime-fallback"
    _task(tasks, run_id)
    values = candidate_inputs()
    calls = {"stage": None}

    class ObservationRepository:
        def __init__(self, root, value):
            assert Path(root) == tasks.root and value == run_id
            self.run_id = value

        def load_stage(self, stage):
            if stage == "S5_5":
                return None
            return values[{"S4": "s4_stage", "S5": "s5_stage"}[stage]]

        def state(self):
            return {"last_realtime_check": "verified-observation"}

    def projected(repository, check_id):
        calls["stage"] = (repository.run_id, check_id)
        return values["s5_5_stage"]

    monkeypatch.setattr(pipeline_persistence, "PipelineRepository", ObservationRepository)
    monkeypatch.setattr(
        "competition_core.terminal_strategy.realtime_stage.load_terminal_realtime_stage",
        projected,
    )
    app.dependency_overrides[get_ninef_account_reader] = lambda: _account

    def leaderboard_reader():
        from test_terminal_strategy_snapshot_adapter import _leaderboard_view
        return _leaderboard_view()

    app.dependency_overrides[get_ninef_leaderboard_reader] = lambda: leaderboard_reader
    response = client.post(f"/pipeline/runs/{run_id}/terminal/evaluate", json={}, headers=HEADERS)

    assert response.status_code == 200, response.text
    assert calls["stage"] == (run_id, "verified-observation")
    assert response.json()["execution_ready"] is False


def test_evaluate_requires_explicit_write_auth_and_forbids_extra_fields(client):
    client, tasks = client
    run_id = "terminal-auth-fixture"
    _task(tasks, run_id)
    assert client.post(f"/pipeline/runs/{run_id}/terminal/evaluate", json={}).status_code == 403
    response = client.post(
        f"/pipeline/runs/{run_id}/terminal/evaluate",
        json={"remaining_trade_dates": ["2026-09-22"]},
        headers=HEADERS,
    )
    assert response.status_code == 422


def test_invalid_tier_evaluation_id_and_traversal_are_rejected(client):
    client, tasks = client
    run_id, result = _persisted(client, tasks)
    assert client.get(f"/pipeline/runs/{run_id}/terminal/candidates?tier=bad").status_code == 422
    assert client.get(f"/pipeline/runs/{run_id}/terminal/decision?evaluation_id=../escape").status_code == 422
    assert client.get(f"/pipeline/runs/{run_id}/terminal/decision?evaluation_id={'0' * 64}").status_code == 404
    assert client.get(f"/pipeline/runs/{run_id}/terminal/decision?evaluation_id={result.evaluation_id}").status_code == 200


def test_tampered_or_partial_terminal_evaluation_returns_conflict(client):
    client, tasks = client
    run_id, result = _persisted(client, tasks)
    directory = tasks.root / run_id / "terminal" / result.evaluation_id
    (directory / "mode-decision.json").write_text('{"tampered":true}\n', encoding="utf-8")
    response = client.get(f"/pipeline/runs/{run_id}/terminal/decision?evaluation_id={result.evaluation_id}")
    assert response.status_code == 409


def test_remote_get_is_rejected(client):
    _, tasks = client
    _task(tasks, "terminal-remote-fixture")
    with TestClient(app, client=("192.0.2.20", 50000)) as remote:
        assert remote.get("/pipeline/terminal/status").status_code == 403

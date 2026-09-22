"""Read-only terminal API contracts with synthetic pipeline and account views."""

from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from competition_core.api import app
from competition_core.pipeline.artifacts import atomic_json, content_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline import persistence as pipeline_persistence
from competition_core.research_api import (
    _imported_terminal_account_view,
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
from test_pipeline_portfolio import inputs as portfolio_inputs
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


def _write_execution_import(tasks, run_id, *, receipt=True):
    directory = tasks.root / run_id
    account_view = _account()
    fees = portfolio_inputs()["fees"]
    body = {
        "account": account_view["account"],
        "fees": fees.to_dict(),
    }
    if receipt:
        body["source_receipt"] = {
            "schema_version": "ninef.account.fetch.v2",
            "method": "GET_ONLY",
            "collection_consistent": True,
            "account_snapshot_hash": content_hash(body["account"]),
            "fee_schedule_hash": content_hash(fees.to_dict()),
            "orders": {"complete": True, "count": 0, "open_count": 0, "items": [], "unknown_count": 0},
        }
    input_id = content_hash(body)
    atomic_json(directory / "execution-imports" / f"{input_id}.json", body, immutable=True)
    atomic_json(directory / "execution-inputs.json", {"id": input_id}, immutable=True)
    return directory, input_id


def test_terminal_imported_account_view_skips_live_fallback(client):
    _, tasks = client
    run_id = "terminal-imported-account"
    _task(tasks, run_id)
    directory, _ = _write_execution_import(tasks, run_id)
    view = _imported_terminal_account_view(tasks, run_id)
    assert view["schema_version"] == "competition.account-view.v1"
    assert view["read_only"] is True and view["orders_submitted"] == 0
    assert view["account_snapshot_hash"] == content_hash(view["account"])
    assert view["orders"]["unknown_count"] == 0
    assert view["frozen_cash"] == "0.00"
    assert directory.joinpath("execution-inputs.json").is_file()


def test_terminal_import_without_receipt_returns_live_fallback(client):
    _, tasks = client
    run_id = "terminal-account-fallback"
    _task(tasks, run_id)
    _write_execution_import(tasks, run_id, receipt=False)
    assert _imported_terminal_account_view(tasks, run_id) is None


def _patch_terminal_evaluate_fixture(monkeypatch, tasks, run_id):
    inputs = candidate_inputs()
    values = {"S4": inputs["s4_stage"], "S5": inputs["s5_stage"], "S5_5": inputs["s5_5_stage"]}
    monkeypatch.setattr(
        "competition_core.research_api._terminal_stage_bundle",
        lambda supplied_tasks, supplied_run: (tasks.root / run_id, values),
    )
    from test_terminal_strategy_snapshot_adapter import _leaderboard_view
    app.dependency_overrides[get_ninef_leaderboard_reader] = lambda: _leaderboard_view


def test_terminal_evaluate_prefers_verified_imported_account(client, monkeypatch):
    client, tasks = client
    run_id = "terminal-imported-evaluate"
    _task(tasks, run_id)
    _write_execution_import(tasks, run_id, receipt=True)
    imported = _imported_terminal_account_view(tasks, run_id)
    _patch_terminal_evaluate_fixture(monkeypatch, tasks, run_id)

    def live_reader_must_not_run():
        pytest.fail("terminal evaluation used live account despite a verified import")

    app.dependency_overrides[get_ninef_account_reader] = lambda: live_reader_must_not_run
    response = client.post(f"/pipeline/runs/{run_id}/terminal/evaluate", json={}, headers=HEADERS)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["snapshot"]["account_snapshot_hash"] == imported["account_snapshot_hash"]
    assert body["execution_ready"] is False and body["orders_submitted"] == 0


def test_terminal_evaluate_without_receipt_uses_live_account_fallback(client, monkeypatch):
    client, tasks = client
    run_id = "terminal-live-account-fallback"
    _task(tasks, run_id)
    _write_execution_import(tasks, run_id, receipt=False)
    _patch_terminal_evaluate_fixture(monkeypatch, tasks, run_id)
    calls = {"account": 0}

    def live_reader():
        calls["account"] += 1
        return _account()

    app.dependency_overrides[get_ninef_account_reader] = lambda: live_reader
    response = client.post(f"/pipeline/runs/{run_id}/terminal/evaluate", json={}, headers=HEADERS)

    assert response.status_code == 200, response.text
    assert calls["account"] == 1
    assert response.json()["execution_ready"] is False


@pytest.mark.parametrize("tamper", ["pointer", "import", "receipt"])
def test_terminal_import_tampering_fails_closed(client, tamper):
    _, tasks = client
    run_id = f"terminal-account-tamper-{tamper}"
    _task(tasks, run_id)
    directory, input_id = _write_execution_import(tasks, run_id)
    if tamper == "pointer":
        atomic_json(directory / "execution-inputs.json", {"id": "f" * 64}, immutable=False)
    elif tamper == "import":
        path = directory / "execution-imports" / f"{input_id}.json"
        atomic_json(path, {"corrupt": True}, immutable=False)
    else:
        path = directory / "execution-imports" / f"{input_id}.json"
        payload = read_json(path)
        payload["source_receipt"]["account_snapshot_hash"] = "0" * 64
        # Keep the old pointer deliberately: content-addressed verification must fail first.
        atomic_json(path, payload, immutable=False)
    with pytest.raises(PipelineError):
        _imported_terminal_account_view(tasks, run_id)


def test_terminal_plan_auth_schema_and_bounded_shadow_projection(client, monkeypatch):
    client, tasks = client
    run_id = "terminal-plan-contract"
    _task(tasks, run_id)

    assert client.post(f"/pipeline/runs/{run_id}/terminal/plan", json={}).status_code == 403
    extra = client.post(
        f"/pipeline/runs/{run_id}/terminal/plan",
        json={"check_id": "client-must-not-supply"},
        headers=HEADERS,
    )
    assert extra.status_code == 422
    invalid = client.post(
        f"/pipeline/runs/{run_id}/terminal/plan",
        json={"evaluation_id": "../escape"},
        headers=HEADERS,
    )
    assert invalid.status_code == 422

    evaluation_id = "a" * 64
    calls = {}

    def fake_plan(**kwargs):
        calls.update(kwargs)
        return {
            "schema_version": "competition.terminal-shadow-plan.v1",
            "run_id": run_id,
            "evaluation_id": evaluation_id,
            "attempt_id": "terminal-attempt",
            "status": "SHADOW",
            "shadow_only": True,
            "execution_ready": False,
            "production_accepted": False,
            "orders_submitted": 0,
        }

    monkeypatch.setattr(
        "competition_core.research_portfolio.create_terminal_shadow_plan", fake_plan
    )
    response = client.post(
        f"/pipeline/runs/{run_id}/terminal/plan",
        json={"evaluation_id": evaluation_id},
        headers=HEADERS,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["schema_version"] == "competition.terminal-shadow-plan.v1"
    assert body["shadow_only"] is True
    assert body["execution_ready"] is False
    assert body["production_accepted"] is False
    assert body["orders_submitted"] == 0
    assert calls == {"tasks": tasks, "run_id": run_id, "evaluation_id": evaluation_id}

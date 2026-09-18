"""Program-owned task control with synthetic workers; no actual stock/AI requests."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from competition_core.pipeline.artifacts import atomic_json, read_json, content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.account import AccountPosition, AccountSnapshot
from competition_core.research_tasks import ResearchTasks, task_lease, worker_fingerprint
from competition_core.research_tasks import ai_authorized, task_request
from competition_core.research_events import read_task_events
from competition_core import research_worker as worker
from tests.pipeline_fixtures import context
from tests.unit.test_pipeline_portfolio import inputs


class Child:
    def __init__(self):
        self.result = None
    def poll(self):
        return self.result


def prepared(tmp_path, monkeypatch, *, ai_review=True):
    calls, children = [], []
    def launch(*args, **kwargs):
        calls.append((args, kwargs))
        child = Child()
        children.append(child)
        return child
    tasks = ResearchTasks(tmp_path / "runs", clock=lambda: context().as_of, launch=launch)
    report = tasks.create(research_date=context().research_date, phase=context().phase,
                           as_of=context().as_of, ai_review=ai_review)
    return tasks, report["run_id"], calls, children


def test_creation_is_async_explicit_and_single_worker(tmp_path, monkeypatch):
    tasks, run_id, calls, children = prepared(tmp_path, monkeypatch)
    assert tasks.status(run_id)["worker_live"] and not tasks.status(run_id)["resume_available"]
    command = calls[0][0][0]
    assert command[1].endswith("research-task-worker.py") and "--run-id" in command
    assert "shell" not in calls[0][1] and calls[0][1]["creationflags"] >= 0
    assert not (tasks.root / run_id / "run.json").exists()
    with pytest.raises(PipelineError, match="another research"):
        tasks.create(research_date=context().research_date, phase="intraday", as_of=context().as_of, ai_review=True)
    children[0].result = 1
    assert tasks.status(run_id)["status"] == "INTERRUPTED"
    assert tasks.status(run_id)["resume_available"]
    original = (tasks.root / run_id / "task-request.json").read_bytes()
    tasks.resume(run_id)
    assert len(calls) == 2 and original == (tasks.root / run_id / "task-request.json").read_bytes()


def test_research_cancel_is_attempt_bound_and_stops_at_a_safe_boundary(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    attempt = read_json(directory / "task-state.json")["attempt_id"]

    requested = tasks.cancel(run_id)
    children[0].result = 0
    result = worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
                                 clock=tasks.clock, client_factory=lambda: pytest.fail("cancel before source"))

    assert requested["cancel_requested"] is True
    assert result["status"] == "CANCELLED" and result["execution_ready"] is False
    saved = read_json(directory / "cancel-requests" / f"{attempt}.json")
    assert saved["scope"] == "RESEARCH_ONLY" and saved["broker_orders_affected"] is False
    assert not (directory / "run.json").exists()
    with pytest.raises(PipelineError, match="not running"):
        tasks.cancel(run_id)


def test_restart_uses_os_lease_not_persisted_status(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    restarted = ResearchTasks(tasks.root, clock=lambda: context().as_of)
    directory = tasks.root / run_id
    with task_lease(directory / ".task-worker.lock"):
        assert restarted.status(run_id)["worker_live"]
        with pytest.raises(PipelineError, match="lease"):
            restarted.resume(run_id)
    assert restarted.status(run_id)["status"] == "INTERRUPTED"


def test_stopped_worker_uses_matching_terminal_attempt_without_mutating_task_state(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    children[0].result = 1
    state_path = directory / "task-state.json"
    saved = read_json(state_path)
    original = state_path.read_bytes()
    terminal = {
        "attempt_id": saved["attempt_id"],
        "status": "FAILED",
        "active_phase": "PREPARE",
        "updated_at": context().as_of.isoformat(),
        "execution_ready": False,
        "error": {
            "code": "NON_TRADING_DAY",
            "message": "research date is not an open SSE trading day",
            "failed_step": "PREPARE",
            "retryable": False,
            "diagnostic_id": "diag-" + "a" * 32,
            "exception_type": "PipelineError",
        },
    }
    atomic_json(directory / "task-attempts" / f"{saved['attempt_id']}.json", terminal, immutable=True)

    status = tasks.status(run_id)

    assert status["status"] == "FAILED" and status["state_source"] == "task_attempt"
    assert status["error"] == {
        "code": "NON_TRADING_DAY",
        "message": "research date is not an open SSE trading day",
        "failed_step": "PREPARE",
        "retryable": False,
        "diagnostic_id": "diag-" + "a" * 32,
    }
    assert state_path.read_bytes() == original


def test_stopped_worker_ignores_attempt_not_bound_to_current_state(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    children[0].result = 1
    atomic_json(directory / "task-attempts" / f"{'b' * 32}.json", {
        "attempt_id": "b" * 32,
        "status": "FAILED",
        "error": {"code": "RESEARCH_TASK_FAILED"},
    }, immutable=True)

    status = tasks.status(run_id)

    assert status["status"] == "INTERRUPTED"
    assert status["state_source"] == "task_state"


def test_changed_implementation_prevents_dispatch_instead_of_overwriting_run(tmp_path, monkeypatch):
    from competition_core import research_tasks
    tasks, run_id, calls, children = prepared(tmp_path, monkeypatch)
    children[0].result = 0
    monkeypatch.setattr(research_tasks, "worker_fingerprint", lambda root: {"changed": "implementation"})
    assert not tasks.status(run_id)["resume_available"]
    assert tasks.status(run_id)["resume_blocked_reason"] == "IMPLEMENTATION_CHANGED_CREATE_NEW_TASK"
    with pytest.raises(PipelineError, match="implementation changed"):
        tasks.resume(run_id)
    assert len(calls) == 1


def test_later_ai_approval_is_bound_without_mutating_original_task(tmp_path, monkeypatch):
    tasks, run_id, calls, children = prepared(tmp_path, monkeypatch, ai_review=False)
    directory = tasks.root / run_id
    original = (directory / "task-request.json").read_bytes()
    children[0].result = 0
    with pytest.raises(PipelineError):
        tasks.authorize_ai(run_id, authorized="true")
    assert not ai_authorized(directory, task_request(directory))
    tasks.authorize_ai(run_id, authorized=True)
    assert len(calls) == 2 and tasks.status(run_id)["ai_review"]
    assert (directory / "task-request.json").read_bytes() == original
    approval = read_json(directory / "ai-authorization.json")
    approval["request_hash"] = "0" * 64
    atomic_json(directory / "ai-authorization.json", approval)
    with pytest.raises(PipelineError, match="authorization"):
        ai_authorized(directory, task_request(directory))


def test_preparation_failure_freezes_snapshot_before_dataset_and_reuses_it(tmp_path, monkeypatch):
    from types import SimpleNamespace
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    request = task_request(directory)
    calls = []
    snapshot = {"snapshot_id": "explicit-synthetic"}
    def execute(args, **kwargs):
        calls.append(args)
        kwargs["snapshot_ready"](snapshot)
        raise PipelineError("SOURCE_UNAVAILABLE", "synthetic dataset failure")
    fake = SimpleNamespace(_parser=lambda: SimpleNamespace(parse_args=lambda args: args), _execute=execute)
    monkeypatch.setattr(worker.importlib.util, "spec_from_file_location", lambda *a: SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None)))
    monkeypatch.setattr(worker.importlib.util, "module_from_spec", lambda spec: fake)
    for _ in range(2):
        with pytest.raises(PipelineError, match="synthetic dataset"):
            worker.prepare_dataset(request, directory, object(), tasks.clock)
    assert "--snapshot" not in calls[0] and "--snapshot" in calls[1]
    assert read_json(directory / "task-universe.json") == snapshot
    atomic_json(directory / "task-universe.json", {"snapshot_id": "changed"})
    with pytest.raises(PipelineError):
        worker.prepare_dataset(request, directory, object(), tasks.clock)
    assert len(calls) == 2


def test_preparation_reuses_published_point_in_time_universe(tmp_path, monkeypatch):
    from types import SimpleNamespace
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    request = task_request(directory)
    published = tmp_path / "data/canonical/snapshots" / "universe-2026-09-02-preopen.json"
    published.parent.mkdir(parents=True)
    snapshot = {"snapshot_id": "published-point-in-time"}
    atomic_json(published, snapshot, immutable=True)
    calls = []
    def execute(args, **kwargs):
        calls.append(args)
        kwargs["snapshot_ready"](snapshot)
        return {"ok": True, "dataset": {"manifest_path": str(tmp_path / "manifest.json")}}
    fake = SimpleNamespace(
        _parser=lambda: SimpleNamespace(parse_args=lambda args: args),
        _execute=execute,
        _read_snapshot=lambda path: read_json(path),
    )
    monkeypatch.setattr(worker, "ROOT", tmp_path)
    monkeypatch.setattr(worker.importlib.util, "spec_from_file_location", lambda *a: SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None)))
    monkeypatch.setattr(worker.importlib.util, "module_from_spec", lambda spec: fake)

    result = worker.prepare_dataset(request, directory, object(), tasks.clock)

    assert "--snapshot" in calls[0]
    assert calls[0][calls[0].index("--snapshot") + 1] == str(directory / "task-universe.json")
    assert read_json(directory / "task-universe.json") == snapshot
    binding = read_json(directory / "task-universe-binding.json")
    assert binding["request_hash"] == content_hash(request)
    assert result["snapshot_path"] == str(directory / "task-universe.json")


@pytest.mark.parametrize("identifier", ["missing", "../escape", "a/b", "C:\\outside"])
def test_reads_never_create_or_escape_directories(tmp_path, identifier):
    tasks = ResearchTasks(tmp_path / "runs")
    before = list(tmp_path.rglob("*"))
    with pytest.raises(PipelineError):
        tasks.status(identifier)
    assert list(tmp_path.rglob("*")) == before


def test_execution_import_is_explicit_immutable_private_and_never_ready(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    data = inputs()
    with pytest.raises(PipelineError, match="during"):
        tasks.import_execution_inputs(run_id, account=data["account"].to_dict(), fees=data["fees"].to_dict())
    children[0].result = 0
    imported = tasks.import_execution_inputs(run_id, account=data["account"].to_dict(), fees=data["fees"].to_dict())
    assert imported["source_kind"] == "synthetic_test" and not imported["execution_ready"]
    assert "available_cash" not in str(imported) and "positions" not in str(tasks.status(run_id))
    path = tasks.root / run_id / "execution-imports" / f"{imported['input_id']}.json"
    assert content_hash(read_json(path)) == imported["input_id"]
    with pytest.raises(PipelineError):
        tasks.import_execution_inputs(run_id, account={}, fees={})


def test_ninef_import_is_read_only_sanitized_and_uses_the_same_strict_contract(tmp_path, monkeypatch):
    from dataclasses import replace
    from adapters.ninef.account import NinefAccountFetch
    from competition_core.pipeline.account import AccountSnapshot

    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    children[0].result = 0
    data = inputs()
    account_payload = data["account"].to_dict() | {
        "source": "ninef.competition_api.readonly",
        "source_kind": "competition_api",
    }
    account = AccountSnapshot.from_dict(account_payload)
    fees = replace(data["fees"], source="ninef explicit fee rule")
    receipt = {"schema_version": "ninef.account.fetch.v1", "response_hashes": {"assets": "a" * 64}}
    monkeypatch.setattr("adapters.ninef.account.resolve_ninef_token", lambda: "private-fixture-token")
    monkeypatch.setattr("adapters.ninef.account.fetch_ninef_account",
                        lambda token: NinefAccountFetch(account, fees, receipt))

    result = tasks.import_ninef_execution_inputs(run_id)

    assert result["provider"] == "ninef" and result["position_count"] == 0
    assert result["pending_order_count"] == 0 and result["orders_submitted"] == 0
    saved = read_json(tasks.root / run_id / "execution-imports" / f"{result['input_id']}.json")
    assert saved["source_receipt"] == receipt
    assert "private-fixture-token" not in str(saved) and "available_cash" not in str(result)


def test_future_and_historical_creation_never_launches(tmp_path):
    tasks = ResearchTasks(tmp_path / "runs", clock=lambda: context().as_of,
        launch=lambda *a, **k: pytest.fail("invalid cutoff must not spawn"))
    for at in (context().as_of + timedelta(seconds=1), context().as_of - timedelta(days=1)):
        with pytest.raises(PipelineError):
            tasks.create(research_date=at.date(), phase="intraday", as_of=at, ai_review=True)


def test_worker_failure_records_no_provider_message(tmp_path, monkeypatch):
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    attempt = read_json(directory / "task-state.json")["attempt_id"]
    def fail():
        raise RuntimeError("token=do-not-log-this-provider-secret")
    with pytest.raises(RuntimeError):
        worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
                            clock=tasks.clock, client_factory=fail)
    state = read_json(directory / "task-state.json")
    assert state["status"] == "FAILED" and state["error"]["code"] == "RESEARCH_TASK_FAILED"
    assert state["error"]["message"] == "research task failed unexpectedly"
    assert state["error"]["failed_step"] == "PREPARE"
    assert state["error"]["retryable"] is False
    assert state["error"]["diagnostic_id"].startswith("diag-")
    assert "do-not-log" not in (directory / "task-state.json").read_text()
    assert "do-not-log" not in (directory / "task-attempts" / f"{attempt}.json").read_text()


def test_worker_pipeline_error_serializes_safe_diagnostics(tmp_path, monkeypatch):
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch)
    directory = tasks.root / run_id
    attempt = read_json(directory / "task-state.json")["attempt_id"]

    def fail_prepare(*args):
        raise PipelineError(
            "NON_TRADING_DAY",
            "2026-09-05 is not an open SSE trading day; token=do-not-persist",
            provider_body="Authorization: do-not-persist",
        )

    with pytest.raises(PipelineError, match="2026-09-05"):
        worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
                            clock=tasks.clock, client_factory=lambda: object(), prepare=fail_prepare)

    state = read_json(directory / "task-state.json")
    error = state["error"]
    assert error["code"] == "NON_TRADING_DAY"
    assert error["message"] == "research date is not an open SSE trading day"
    assert error["failed_step"] == "PREPARE" and error["retryable"] is False
    assert error["diagnostic_id"].startswith("diag-")
    saved = (directory / "task-attempts" / f"{attempt}.json").read_text()
    assert "2026-09-05 is not an open" not in saved
    assert "do-not-persist" not in saved
    assert not (directory / "task-universe.json").exists()
    assert not (directory / "task-prepared.json").exists()
    assert not (directory / "S0.json").exists()


def test_worker_historical_unprepared_task_does_not_request_data(tmp_path, monkeypatch):
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch)
    attempt = read_json(tasks.root / run_id / "task-state.json")["attempt_id"]
    with pytest.raises(PipelineError, match="historical preparation"):
        worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
            clock=lambda: context().as_of + timedelta(days=1), client_factory=lambda: pytest.fail("no future source call"))


@pytest.mark.parametrize("ai_review", [True, False])
def test_worker_stage_order_and_missing_account_is_not_substituted(tmp_path, monkeypatch, ai_review):
    tasks, run_id, _, _ = prepared(tmp_path, monkeypatch, ai_review=ai_review)
    directory = tasks.root / run_id
    attempt = read_json(directory / "task-state.json")["attempt_id"]
    calls = []
    snapshot, manifest = tmp_path / "snapshot.json", tmp_path / "manifest.json"
    atomic_json(snapshot, {"snapshot_id": "synthetic"})
    atomic_json(manifest, {"context": context().to_dict(), "source": "explicit.synthetic"})
    class Repo:
        def __init__(self, *args):
            self.s5 = False
        def load_stage(self, stage):
            return None if stage == "S5" and not self.s5 else {"status": "SUCCESS"}
    monkeypatch.setattr(worker, "PipelineRepository", Repo)
    monkeypatch.setattr(worker, "run_screening", lambda **kw: calls.append("screen"))
    def reviews(**kw):
        calls.append("ai")
        assert kw["news_context"]["status"] == "EMPTY_VERIFIED"
        kw["repository"].s5 = True
    monkeypatch.setattr(worker, "run_reviews", reviews)
    def status(**kw):
        assert kw["account"] is None
        calls.append("status")
    monkeypatch.setattr(worker, "run_execution_status_check", status)
    def quotes(**kw):
        assert kw["account"] is None
        calls.append("quotes")
    monkeypatch.setattr(worker, "run_realtime_check", quotes)
    def portfolio(**kw):
        assert kw["account"] is None and kw["fees"] is None
        calls.append("portfolio")
        return {"execution_ready": False}
    monkeypatch.setattr(worker, "run_portfolio_draft", portfolio)
    def prepare(*args):
        calls.append("prepare")
        return {"snapshot_path": str(snapshot), "manifest_path": str(manifest), "dataset_root": str(tmp_path)}
    report = worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
        clock=tasks.clock, client_factory=lambda: object(), prepare=prepare,
        news_fetcher=lambda **kw: calls.append("news") or {"status": "EMPTY_VERIFIED"})
    assert report["status"] == "PARTIAL" and not report["execution_ready"]
    expected = ["prepare", "screen"] + (["news", "ai", "status", "quotes", "portfolio"] if ai_review else [])
    assert calls == expected
    assert (directory / "task-prepared.json").exists()


def test_worker_uses_frozen_import_for_automatic_holding_review(tmp_path, monkeypatch):
    tasks, run_id, _, children = prepared(tmp_path, monkeypatch)
    children[0].result = 0
    base = inputs()
    account = AccountSnapshot.from_dict(base["account"].to_dict() | {
        "cash_balance": "999000.00", "available_cash": "999000.00",
        "positions": [AccountPosition("600519.SH", 100, 100, Decimal("1000.00")).to_dict()],
    })
    imported = tasks.import_execution_inputs(run_id, account=account.to_dict(), fees=base["fees"].to_dict())
    directory = tasks.root / run_id
    attempt = read_json(directory / "task-state.json")["attempt_id"]
    snapshot, manifest = tmp_path / "snapshot.json", tmp_path / "manifest.json"
    atomic_json(snapshot, {"snapshot_id": "synthetic", "members": []})
    atomic_json(manifest, {"context": context().to_dict(), "source": "explicit.synthetic",
                           "session_dates": ["2026-08-26", "2026-08-27", "2026-08-28",
                                             "2026-08-31", "2026-09-01"]})
    calls = []

    class Repo:
        def __init__(self, *args):
            self.s5 = False
        def load_stage(self, stage):
            return None if stage == "S5" and not self.s5 else {"status": "SUCCESS"}

    class Dataset:
        def __init__(self, *args):
            pass
        def load(self, path):
            calls.append("panel")
            return "frozen-panel"

    monkeypatch.setattr(worker, "PipelineRepository", Repo)
    def screen(**kw):
        calls.append("screen")
        kw["progress"]({"stage": "S2", "count": 3, "elapsed_seconds": 1.25})
    monkeypatch.setattr(worker, "run_screening", screen)
    def reviews(**kw):
        calls.append("ai")
        kw["repository"].s5 = True
    monkeypatch.setattr(worker, "run_reviews", reviews)
    def holdings(**kw):
        calls.append("holdings")
        assert kw["account"].snapshot_hash == account.snapshot_hash
        assert kw["account_source_hash"] == imported["input_id"]
        assert kw["panel"] == "frozen-panel"
        kw["progress"]({"stage": "S5", "holding_review_completed": 1,
                         "holding_review_total": 2})
    monkeypatch.setattr(worker, "run_holding_reviews", holdings)
    monkeypatch.setattr("competition_core.pipeline.dataset.CanonicalDataset", Dataset)
    monkeypatch.setattr(worker, "run_execution_status_check", lambda **kw: calls.append("status"))
    monkeypatch.setattr(worker, "run_realtime_check", lambda **kw: calls.append("quotes"))
    monkeypatch.setattr(worker, "run_portfolio_draft",
                        lambda **kw: calls.append("portfolio") or {"execution_ready": False})

    result = worker.execute_task(runs_root=tasks.root, run_id=run_id, attempt_id=attempt,
        clock=tasks.clock, client_factory=lambda: object(),
        prepare=lambda *args: {"snapshot_path": str(snapshot), "manifest_path": str(manifest),
                               "dataset_root": str(tmp_path)},
        news_fetcher=lambda **kw: {"status": "EMPTY_VERIFIED"})

    assert result["status"] == "PARTIAL"
    assert calls == ["screen", "ai", "panel", "holdings", "status", "quotes", "portfolio"]
    progress = [event for event in read_task_events(directory)["events"] if event["kind"] == "PROGRESS"]
    assert {event["elapsed_seconds"] for event in progress if "elapsed_seconds" in event} == {1.25}
    holding = next(event for event in progress if event.get("total_count") == 2)
    assert holding["completed_count"] == 1 and holding["total_count"] == 2

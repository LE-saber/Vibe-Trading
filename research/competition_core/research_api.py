"""Local UI API. Bounded projections and asynchronous program-owned jobs."""

import asyncio
from datetime import date, datetime
from functools import wraps
import os
from pathlib import Path
import secrets
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import SHANGHAI
from competition_core.research_tasks import ResearchTasks, checked_directory


def local_request(request: Request):
    peers = {"127.0.0.1", "::1", "localhost", "testclient"}
    hosts = {"127.0.0.1", "::1", "localhost", "testserver"}
    if not request.client or request.client.host not in peers or request.url.hostname not in hosts:
        from fastapi import HTTPException
        raise HTTPException(403, "Research API is loopback-only")
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        origin = request.headers.get("origin")
        expected = f"{request.url.scheme}://{request.url.netloc}"
        if request.headers.get("x-competition-request") != "1" or (origin and origin != expected):
            from fastapi import HTTPException
            raise HTTPException(403, "Explicit same-origin research action required")
        expected_token = os.getenv("COMPETITION_CORE_SERVICE_TOKEN", "")
        supplied_token = request.headers.get("x-competition-service-token", "")
        if len(expected_token) < 32 or not secrets.compare_digest(supplied_token, expected_token):
            from fastapi import HTTPException
            raise HTTPException(403, "Authenticated research proxy required")


router = APIRouter(prefix="/pipeline", dependencies=[Depends(local_request)])
_manager = ResearchTasks()


def get_tasks():
    return _manager


def get_ninef_account_view():
    from adapters.ninef.account import NinefAccountAdapterError, fetch_ninef_account, resolve_ninef_token
    try:
        token = resolve_ninef_token()
    except NinefAccountAdapterError as exc:
        raise PipelineError("NOT_CONFIGURED", "Ninef read-only credential is not configured") from exc
    try:
        fetched = fetch_ninef_account(token)
    except NinefAccountAdapterError as exc:
        raise PipelineError("SOURCE_UNAVAILABLE", "Ninef read-only account is unavailable") from exc
    finally:
        token = None
    account = fetched.snapshot.to_dict()
    return {"schema_version": "competition.account-view.v1", "account": account,
            "frozen_cash": str(fetched.snapshot.cash_balance - fetched.snapshot.available_cash),
            "orders": fetched.receipt["orders"], "collection_consistent": True,
            "fee_evidence_available": fetched.fee_schedule is not None,
            "orders_submitted": 0, "read_only": True}


def get_ninef_leaderboard_view():
    from adapters.ninef.account import NinefAccountAdapterError, resolve_ninef_token
    from adapters.ninef.leaderboard import fetch_ninef_leaderboard
    try:
        token = resolve_ninef_token()
    except NinefAccountAdapterError as exc:
        raise PipelineError("NOT_CONFIGURED", "Ninef read-only credential is not configured") from exc
    try:
        return fetch_ninef_leaderboard(token).to_dict() | {"orders_submitted": 0, "read_only": True}
    except NinefAccountAdapterError as exc:
        raise PipelineError("SOURCE_UNAVAILABLE", "Ninef leaderboard is unavailable") from exc
    finally:
        token = None


def get_ninef_account_reader():
    return get_ninef_account_view


def get_ninef_leaderboard_reader():
    return get_ninef_leaderboard_view


def safe_api(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except PipelineError as exc:
            status = {"NOT_FOUND": 404, "INVALID_ARGUMENTS": 422, "TASK_BUSY": 409,
                      "STALE_DATA": 409, "NOT_READY": 409}.get(exc.code, 503)
            return JSONResponse(status_code=status, content={"code": exc.code, "message": "Research request could not be completed", "details": {}})
        except (ValueError, TypeError, KeyError):
            return JSONResponse(status_code=422, content={"code": "INVALID_ARGUMENTS", "message": "Invalid research input", "details": {}})
        except Exception as exc:
            from competition_core.execution.service import ExecutionGateError
            if isinstance(exc, ExecutionGateError):
                status = 422 if exc.code in {"INVALID_ARGUMENTS", "INVALID_PLAN"} else 409
                return JSONResponse(status_code=status, content={"code": exc.code, "message": exc.message, "details": {}})
            return JSONResponse(status_code=503, content={"code": "NOT_READY", "message": "Research evidence or service is unavailable", "details": {}})
    return wrapped


class StartResearch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    research_date: date
    phase: str
    as_of: datetime
    ai_review: StrictBool


class ExecutionInputs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: dict
    fees: dict


class ReviewConfirmation(ExecutionInputs):
    portfolio_hash: str
    reviewed: StrictBool


class AIApproval(BaseModel):
    model_config = ConfigDict(extra="forbid")
    authorized: StrictBool


class ExecutionApprovalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_plan_hash: str
    expires_at: datetime


class UnknownResolutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    broker_order_id: str
    evidence_ref: str


class TerminalEvaluateRequest(BaseModel):
    """Explicit, bounded input for a read-only terminal evaluation.

    The client cannot supply competition dates, award rules, a mode override,
    prices, quantities, or any order payload.  ``previous_mode`` is only the
    state-machine continuity fact and is checked against the latest persisted
    evaluation before it reaches the runtime.
    """

    model_config = ConfigDict(extra="forbid")
    core_symbols: list[StrictStr] | None = None
    same_day_exit_unavailable_symbols: list[StrictStr] | None = None
    previous_mode: StrictStr | None = None
    downgrade_confirmation_count: StrictInt = Field(default=0, ge=0, le=10)


def get_execution_service():
    from competition_core.execution.runtime import get_execution_service as factory
    return factory()


def get_execution_supervisor():
    from competition_core.execution.runtime import get_execution_supervisor as factory
    return factory()


@router.post("/runs", status_code=202)
@safe_api
def start_research(body: StartResearch, tasks=Depends(get_tasks)):
    return tasks.create(**body.model_dump())


@router.get("/runs")
@safe_api
def list_runs(limit: int = Query(50, ge=1, le=100), tasks=Depends(get_tasks)):
    from competition_core.pipeline.artifacts import read_json
    rows = []
    if tasks.root.exists():
        for directory in tasks.root.iterdir():
            if not directory.is_dir() or directory.is_symlink():
                continue
            try:
                checked_directory(tasks.root, directory.name, existing=False)
                state_path = directory / "run.json"
                task_path = directory / "task-request.json"
                from competition_core.pipeline.artifacts import confined_path
                path = confined_path(directory, "run.json" if state_path.exists() else "task-request.json")
                if not path.exists():
                    continue
                state = read_json(path)
                task = tasks.status(directory.name) if task_path.is_file() else None
                rows.append({"run_id": directory.name,
                    "research_date": task["research_date"] if task else state.get("research_date"),
                    "phase": task["phase"] if task else state.get("phase"),
                    "updated_at": (task.get("updated_at") if task else None) or state.get("updated_at", state.get("created_at")),
                    "status": task["status"] if task else state.get("status"),
                    "active_phase": task.get("active_phase") if task else state.get("current_stage"),
                    "error": task.get("error") if task else None,
                    "program_owned": task is not None, "current_execution_ready": False})
            except (ValueError, OSError):
                continue
    rows.sort(key=lambda row: str(row["updated_at"] or ""), reverse=True)
    return {"runs": rows[:limit], "total": len(rows), "production_accepted": False}


@router.get("/ninef/account")
@safe_api
def ninef_account(reader=Depends(get_ninef_account_reader)):
    return reader()


@router.get("/ninef/leaderboard")
@safe_api
def ninef_leaderboard(reader=Depends(get_ninef_leaderboard_reader)):
    return reader()


@router.get("/daily-workflow")
@safe_api
def daily_workflow():
    from competition_core.daily_workflow import load_daily_workflow_config
    policy = load_daily_workflow_config()
    now = datetime.now(SHANGHAI)
    return {
        "schema_version": "competition.daily-workflow-view.v1",
        "as_of": now.isoformat(),
        "current_phase": policy.phase_at(now),
        "policy": policy.to_dict(),
        "execution_ready": False,
        "approval_required": True,
        "orders_submitted": 0,
    }


def _terminal_stage_bundle(tasks, run_id: str):
    """Load only checked, repository-verified terminal source stages."""

    from competition_core.pipeline.persistence import PipelineRepository

    directory = checked_directory(tasks.root, run_id)
    repository = PipelineRepository(directory.parent, run_id)
    stages = {}
    for stage in ("S4", "S5"):
        value = repository.load_stage(stage)
        if value is None:
            raise PipelineError("NOT_READY", "terminal source stage is unavailable", stage=stage)
        stages[stage] = value
    s5_5 = repository.load_stage("S5_5")
    if s5_5 is None:
        check_id = repository.state().get("last_realtime_check")
        if not isinstance(check_id, str):
            raise PipelineError("NOT_READY", "terminal realtime observation is unavailable", stage="S5_5")
        from competition_core.terminal_strategy.realtime_stage import load_terminal_realtime_stage
        s5_5 = load_terminal_realtime_stage(repository, check_id)
    stages["S5_5"] = s5_5
    return directory, stages


def _terminal_now(tasks) -> datetime:
    clock = getattr(tasks, "clock", None)
    value = clock() if callable(clock) else datetime.now(SHANGHAI)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise PipelineError("INVALID_ARGUMENTS", "terminal evaluation time must be timezone-aware")
    return value


def _latest_terminal_for_input(tasks, run_id: str):
    from competition_core.terminal_strategy.api_views import latest_persisted

    try:
        return latest_persisted(tasks.root, run_id)
    except PipelineError as exc:
        if exc.code == "NOT_FOUND":
            return None
        raise


def _previous_terminal_mode(tasks, run_id: str, requested: str | None):
    from competition_core.terminal_strategy import TerminalMode

    latest = _latest_terminal_for_input(tasks, run_id)
    latest_mode = None
    if latest is not None:
        latest_mode = latest["artifacts"]["mode-decision.json"].get("mode")
        try:
            latest_mode = TerminalMode(latest_mode)
        except (TypeError, ValueError) as exc:
            raise PipelineError("STALE_DATA", "latest terminal mode is invalid") from exc
    if requested is None:
        return latest_mode
    try:
        requested_mode = TerminalMode(requested)
    except (TypeError, ValueError) as exc:
        raise PipelineError("INVALID_ARGUMENTS", "previous terminal mode is invalid") from exc
    if latest_mode is None or requested_mode is not latest_mode:
        raise PipelineError("INVALID_ARGUMENTS", "previous terminal mode does not match latest evaluation")
    return requested_mode


@router.post("/runs/{run_id}/terminal/evaluate")
@safe_api
def terminal_evaluate(
    run_id: str,
    body: TerminalEvaluateRequest,
    tasks=Depends(get_tasks),
    account_reader=Depends(get_ninef_account_reader),
    leaderboard_reader=Depends(get_ninef_leaderboard_reader),
):
    """Evaluate terminal strategy facts without exposing any write capability."""

    from competition_core.terminal_strategy import load_terminal_strategy_config
    from competition_core.terminal_strategy.api_views import decision_projection
    from competition_core.terminal_strategy.runtime import run_terminal_runtime

    _, stages = _terminal_stage_bundle(tasks, run_id)
    config = load_terminal_strategy_config()
    previous_mode = _previous_terminal_mode(tasks, run_id, body.previous_mode)
    account_view = account_reader()
    leaderboard_view = leaderboard_reader()
    runtime = run_terminal_runtime(
        run_id,
        stages["S4"],
        stages["S5"],
        stages["S5_5"],
        account_view,
        leaderboard_view,
        _terminal_now(tasks),
        config,
        tasks.root,
        previous_mode=previous_mode,
        downgrade_confirmation_count=body.downgrade_confirmation_count,
        same_day_exit_unavailable_symbols=body.same_day_exit_unavailable_symbols,
        core_symbols=body.core_symbols,
    )
    if runtime.evaluation is None or runtime.path is None:
        if runtime.diagnostics.persistence_error:
            raise PipelineError("STALE_DATA", "terminal evaluation could not be immutably persisted")
        code = "NOT_READY" if runtime.status == "BLOCKED" else "SOURCE_UNAVAILABLE"
        raise PipelineError(code, "terminal evaluation is unavailable")
    response = decision_projection(runtime.evaluation)
    response["diagnostics"] = {
        "blockers": list(runtime.diagnostics.blockers),
        "reasons": list(runtime.diagnostics.reasons),
    }
    return response


@router.get("/runs/{run_id}/terminal/decision")
@safe_api
def terminal_decision(
    run_id: str,
    evaluation_id: str | None = Query(None),
    tasks=Depends(get_tasks),
):
    checked_directory(tasks.root, run_id)
    from competition_core.terminal_strategy.api_views import load_persisted, persisted_decision_projection
    return persisted_decision_projection(load_persisted(tasks.root, run_id, evaluation_id))


@router.get("/runs/{run_id}/terminal/candidates")
@safe_api
def terminal_candidates(
    run_id: str,
    tier: str = Query("watch"),
    evaluation_id: str | None = Query(None),
    tasks=Depends(get_tasks),
):
    checked_directory(tasks.root, run_id)
    from competition_core.terminal_strategy.api_views import load_persisted, persisted_candidates_projection
    loaded = load_persisted(tasks.root, run_id, evaluation_id)
    return persisted_candidates_projection(loaded, tier)


@router.get("/runs/{run_id}/terminal/history")
@safe_api
def terminal_history(
    run_id: str,
    limit: int = Query(20, ge=1, le=100),
    tasks=Depends(get_tasks),
):
    checked_directory(tasks.root, run_id)
    from competition_core.terminal_strategy.api_views import persisted_history
    return {
        "schema_version": "competition.terminal-history-view.v1",
        "run_id": run_id,
        "history": persisted_history(tasks.root, run_id, limit),
        "execution_ready": False,
        "orders_submitted": 0,
    }


@router.get("/terminal/status")
@safe_api
def terminal_status(tasks=Depends(get_tasks)):
    from competition_core.terminal_strategy.api_views import status_projection
    return status_projection(tasks.root)


@router.get("/runs/{run_id}")
@safe_api
def get_run(run_id: str, tasks=Depends(get_tasks)):
    from competition_core.pipeline_views import get_run_view
    directory = checked_directory(tasks.root, run_id, existing=False)
    task = tasks.status(run_id) if (directory / "task-request.json").is_file() else None
    if not (directory / "run.json").exists():
        if task is None:
            raise PipelineError("NOT_FOUND", "research run does not exist")
        return {"task": task, "research": None, "production_accepted": False}
    return {"task": task, "research": get_run_view(tasks.root, run_id, datetime.now(SHANGHAI)), "production_accepted": False}


@router.get("/runs/{run_id}/events")
@safe_api
def run_events(run_id: str, request: Request, after: int | None = Query(None, ge=0),
               last_event_id: int | None = Header(None, alias="Last-Event-ID", ge=0),
               tasks=Depends(get_tasks)):
    from competition_core.research_events import read_task_events, sse_frame
    directory = checked_directory(tasks.root, run_id)
    if after is not None and last_event_id is not None and after != last_event_id:
        raise PipelineError("INVALID_ARGUMENTS", "conflicting research event cursor")
    cursor = after if after is not None else (last_event_id or 0)
    read_task_events(directory, after=cursor, limit=1)

    async def stream():
        nonlocal cursor
        while True:
            batch = read_task_events(directory, after=cursor, limit=100)
            for event in batch["events"]:
                cursor = event["event_seq"]
                yield sse_frame(event)
            status = tasks.status(run_id)
            if not batch["has_more"] and not status["worker_live"]:
                break
            if await request.is_disconnected():
                break
            if not batch["events"]:
                yield ": keepalive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-store", "X-Accel-Buffering": "no",
    })


@router.get("/runs/{run_id}/stages")
@safe_api
def get_stages(run_id: str, tasks=Depends(get_tasks)):
    from competition_core.pipeline_views import get_run_view
    return get_run_view(tasks.root, run_id, datetime.now(SHANGHAI))


@router.get("/runs/{run_id}/candidates")
@safe_api
def candidates(run_id: str, stage: str = "S4", offset: int = Query(0, ge=0),
               limit: int = Query(100, ge=1, le=200), selected_only: bool = False, tasks=Depends(get_tasks)):
    from competition_core.pipeline_views import get_stage_rows
    return get_stage_rows(tasks.root, run_id, stage, offset=offset, limit=limit, selected_only=selected_only)


@router.get("/runs/{run_id}/evidence/{symbol}")
@safe_api
def stock_evidence(run_id: str, symbol: str, tasks=Depends(get_tasks)):
    from competition_core.pipeline_views import get_stock_view
    return get_stock_view(tasks.root, run_id, symbol)


@router.get("/runs/{run_id}/artifacts")
@safe_api
def artifacts(run_id: str, tasks=Depends(get_tasks)):
    from competition_core.pipeline_views import list_public_artifacts
    return list_public_artifacts(tasks.root, run_id)


@router.post("/runs/{run_id}/resume", status_code=202)
@safe_api
def resume(run_id: str, tasks=Depends(get_tasks)):
    return tasks.resume(run_id)


@router.post("/runs/{run_id}/cancel", status_code=202)
@safe_api
def cancel(run_id: str, tasks=Depends(get_tasks)):
    return tasks.cancel(run_id)


@router.post("/runs/{run_id}/execution-inputs")
@safe_api
def execution_inputs(run_id: str, body: ExecutionInputs, tasks=Depends(get_tasks)):
    return tasks.import_execution_inputs(run_id, **body.model_dump())


@router.post("/runs/{run_id}/execution-inputs/ninef")
@safe_api
def ninef_execution_inputs(run_id: str, tasks=Depends(get_tasks)):
    return tasks.import_ninef_execution_inputs(run_id)


@router.post("/runs/{run_id}/ai-review", status_code=202)
@safe_api
def authorize_ai(run_id: str, body: AIApproval, tasks=Depends(get_tasks)):
    return tasks.authorize_ai(run_id, **body.model_dump())


@router.get("/runs/{run_id}/portfolio")
@safe_api
def portfolio(run_id: str, tasks=Depends(get_tasks)):
    from competition_core.research_portfolio import portfolio_view
    return portfolio_view(tasks.root, run_id, datetime.now(SHANGHAI))


@router.post("/runs/{run_id}/confirm")
@safe_api
def confirm(run_id: str, body: ReviewConfirmation, tasks=Depends(get_tasks)):
    from competition_core.research_portfolio import confirm_review
    return confirm_review(tasks=tasks, run_id=run_id, **body.model_dump())


@router.post("/runs/{run_id}/execution-plan")
@safe_api
def create_controlled_execution_plan(run_id: str, tasks=Depends(get_tasks),
                                     service=Depends(get_execution_service)):
    """Register only the immutable plan rebuilt from this controlled run root."""
    from competition_core.pipeline.execution_plan import create_execution_plan
    from competition_core.pipeline.persistence import PipelineRepository
    directory = checked_directory(tasks.root, run_id)
    plan = create_execution_plan(PipelineRepository(directory.parent, run_id))
    service.register_plan(plan)
    return plan


@router.get("/runs/{run_id}/execution-plan")
@safe_api
def controlled_execution_plan(run_id: str, tasks=Depends(get_tasks),
                              service=Depends(get_execution_service)):
    from competition_core.pipeline.execution_plan import read_execution_plan
    from competition_core.pipeline.persistence import PipelineRepository
    directory = checked_directory(tasks.root, run_id)
    plan = read_execution_plan(PipelineRepository(directory.parent, run_id))
    service.register_plan(plan)
    return plan


@router.post("/execution/plans/{plan_id}/approve")
@safe_api
def approve_execution_plan(plan_id: str, body: ExecutionApprovalRequest,
                           service=Depends(get_execution_service)):
    approval = service.approve(plan_id, body.expected_plan_hash, "vibe_local_operator",
                               expires_at=body.expires_at)
    return {"schema_version": "competition.execution.approval.v1", "approval": approval.to_dict(),
            "orders_submitted": 0}


@router.post("/execution/approvals/{approval_id}/start", status_code=202)
@safe_api
def start_execution_batch(approval_id: str, service=Depends(get_execution_service),
                          supervisor=Depends(get_execution_supervisor)):
    batch_id = service.start(approval_id)
    supervisor.start(batch_id)
    return {"schema_version": "competition.execution.start.v1", "batch_id": batch_id,
            "state": service.snapshot(batch_id)["state"]}


@router.get("/execution/batches")
@safe_api
def execution_batches(limit: int = Query(30, ge=1, le=100),
                      service=Depends(get_execution_service)):
    return {"schema_version": "competition.execution.batches.v1",
            "batches": service.store.list_batches(limit)}


@router.get("/execution/batches/{batch_id}")
@safe_api
def execution_batch(batch_id: str, service=Depends(get_execution_service)):
    return service.snapshot(batch_id)


@router.get("/execution/batches/{batch_id}/events")
@safe_api
def execution_batch_events(batch_id: str, after: int = Query(0, ge=0),
                           service=Depends(get_execution_service)):
    service.snapshot(batch_id)
    return {"schema_version": "competition.execution.events.v1",
            "events": service.store.events(batch_id, after=after)}


@router.post("/execution/batches/{batch_id}/stop", status_code=202)
@safe_api
def stop_execution_batch(batch_id: str, service=Depends(get_execution_service)):
    service.stop(batch_id)
    return {"batch_id": batch_id, "stop_requested": True, "orders_cancelled": 0}


@router.post("/execution/batches/{batch_id}/intents/{intent_id}/cancel", status_code=202)
@safe_api
def cancel_execution_intent(batch_id: str, intent_id: str,
                            service=Depends(get_execution_service)):
    service.request_cancel(batch_id, intent_id)
    service.process_cancel(batch_id, intent_id)
    return service.snapshot(batch_id)


@router.post("/execution/batches/{batch_id}/intents/{intent_id}/resolve")
@safe_api
def resolve_execution_intent(batch_id: str, intent_id: str, body: UnknownResolutionRequest,
                             service=Depends(get_execution_service)):
    resolution = service.resolve_unknown(batch_id, intent_id, body.broker_order_id,
                                         body.evidence_ref, "vibe_local_operator")
    return {"schema_version": "competition.execution.resolution.v1",
            "resolution": resolution.to_dict()}

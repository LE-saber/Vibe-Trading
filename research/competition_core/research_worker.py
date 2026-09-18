"""Single-process coordinator calling existing deterministic/AI/realtime functions."""

from datetime import date, datetime
import importlib.util
import os
from pathlib import Path
import re
import threading
import uuid

from adapters.tushare.client import configured_tushare_client
from adapters.ninef.account import NinefAccountAdapterError, resolve_ninef_token
from adapters.ninef.news import fetch_ninef_news
from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json, confined_path
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.execution_status_runner import run_execution_status_check
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.portfolio import FeeSchedule
from competition_core.pipeline.portfolio_runner import run_portfolio_draft
from competition_core.pipeline.realtime import run_realtime_check
from competition_core.pipeline.reviews import run_holding_reviews, run_reviews
from competition_core.pipeline.runner import run_screening
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from competition_core.research_tasks import ROOT, ai_authorized, checked_directory, task_lease, task_request, worker_fingerprint
from competition_core.research_events import append_task_event


_SAFE_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
_SAFE_FAILED_STEPS = frozenset({
    "PREPARE", "S0_S4", "S5", "S5_5", "EXECUTION_REFERENCES", "S6", "UNKNOWN",
})
_SAFE_ERROR_MESSAGES = {
    "NON_TRADING_DAY": "research date is not an open SSE trading day",
    "INVALID_ARGUMENTS": "research task input is invalid",
    "NOT_READY": "research task is not ready",
    "STALE_DATA": "research task inputs are stale or changed",
    "TASK_BUSY": "research task is busy",
    "SOURCE_UNAVAILABLE": "research source is unavailable",
    "DATA_COVERAGE_INSUFFICIENT": "research data coverage is insufficient",
    "NOT_CONFIGURED": "research service is not configured",
    "RESEARCH_TASK_FAILED": "research task failed unexpectedly",
    "INTERNAL_ERROR": "research task failed unexpectedly",
}
_RETRYABLE_ERROR_CODES = frozenset({"SOURCE_UNAVAILABLE", "TIMEOUT"})


class _ResearchCancelled(Exception):
    pass


def safe_error_record(exc, *, failed_step):
    """Build a bounded task error without persisting exception text or details."""
    code = exc.code if isinstance(exc, PipelineError) else "RESEARCH_TASK_FAILED"
    if not isinstance(code, str) or _SAFE_ERROR_CODE.fullmatch(code) is None:
        code = "RESEARCH_TASK_FAILED"
    exception_type = type(exc).__name__
    if not isinstance(exception_type, str) or _SAFE_ERROR_CODE.fullmatch(exception_type.upper()) is None:
        exception_type = "Exception"
    return {
        "code": code,
        "message": _SAFE_ERROR_MESSAGES.get(code, "research task could not be completed"),
        "failed_step": failed_step if failed_step in _SAFE_FAILED_STEPS else "UNKNOWN",
        "retryable": code in _RETRYABLE_ERROR_CODES,
        "diagnostic_id": f"diag-{uuid.uuid4().hex}",
        "exception_type": exception_type,
    }


def prepare_dataset(request, directory, client, clock):
    """Reuse the already-tested preparation body, with no process-global env edits."""
    spec = importlib.util.spec_from_file_location("web_canonical_preparation", ROOT / "scripts/prepare-canonical.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    arguments = ["--date", request["research_date"], "--phase", request["phase"],
        "--as-of", request["as_of"], "--sessions", str(request["config"]["dataset"]["warmup_sessions"]),
        "--output-root", str(ROOT / "data/canonical")]
    snapshot_path = confined_path(directory, "task-universe.json")
    binding_path = confined_path(directory, "task-universe-binding.json")
    def freeze_snapshot(snapshot):
        atomic_json(snapshot_path, dict(snapshot), immutable=True)
        if not binding_path.exists():
            binding = {"request_hash": content_hash(request), "snapshot_hash": file_hash(snapshot_path)}
            atomic_json(binding_path, binding | {"integrity_hash": content_hash(binding)}, immutable=True)
    if not snapshot_path.exists() and not binding_path.exists():
        universe_phase = "postclose" if request["phase"] == "postclose" else "preopen"
        published = ROOT / "data/canonical/snapshots" / f"universe-{request['research_date']}-{universe_phase}.json"
        if published.is_file():
            freeze_snapshot(module._read_snapshot(published))
    if snapshot_path.exists() or binding_path.exists():
        binding = read_json(binding_path)
        if (binding.pop("integrity_hash", None) != content_hash(binding)
                or binding.get("request_hash") != content_hash(request)):
            raise PipelineError("STALE_DATA", "prepared Universe binding differs from task")
        read_json(snapshot_path, expected_hash=binding["snapshot_hash"])
        arguments.extend(["--snapshot", str(snapshot_path)])
    args = module._parser().parse_args(arguments)
    report = module._execute(args, client_factory=lambda: client, dataset_class=None, now_clock=clock,
                             secrets=[], snapshot_ready=freeze_snapshot)
    if not report["ok"]:
        raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "canonical dataset did not pass its coverage gate")
    return {"snapshot_path": str(snapshot_path), "manifest_path": report["dataset"]["manifest_path"],
            "dataset_root": str(ROOT / "data/canonical")}


def _execution_inputs(directory):
    pointer_path = confined_path(directory, "execution-inputs.json")
    if not pointer_path.exists():
        return None, None, None
    pointer = read_json(pointer_path).get("id")
    if not isinstance(pointer, str) or not re.fullmatch(r"[0-9a-f]{64}", pointer):
        raise PipelineError("STALE_DATA", "invalid execution import identity")
    path = confined_path(directory, f"execution-imports/{pointer}.json")
    body = read_json(path, expected_hash=pointer)
    return AccountSnapshot.from_dict(body["account"]), FeeSchedule.from_dict(body["fees"]), file_hash(path)


def execute_task(*, runs_root, run_id, attempt_id, clock=None, client_factory=None, prepare=None,
                 news_fetcher=None):
    clock = clock or (lambda: datetime.now(SHANGHAI))
    if not isinstance(attempt_id, str) or not re.fullmatch(r"[0-9a-f]{32}", attempt_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid worker attempt identifier")
    directory = checked_directory(runs_root, run_id)
    with task_lease(confined_path(Path(runs_root).resolve(), ".research-worker.lock"), create=True), task_lease(confined_path(directory, ".task-worker.lock")):
        request = task_request(directory)
        state_path = confined_path(directory, "task-state.json")
        queued = read_json(state_path)
        if queued.get("attempt_id") != attempt_id:
            raise PipelineError("NOT_READY", "worker attempt was superseded before it acquired the lease")
        started_at = clock()
        state = {"attempt_id": attempt_id, "status": "RUNNING", "execution_ready": False,
                 "worker_pid": os.getpid(), "worker_started_at": started_at.isoformat()}
        def emit_event(**values):
            try:
                append_task_event(directory, run_id=run_id, attempt_id=attempt_id,
                                  event_time=clock(), **values)
            except OSError:
                # Progress transport is observational and must not alter research results.
                pass
        def update(**values):
            previous_phase = state.get("active_phase")
            state.update(values, updated_at=clock().isoformat())
            atomic_json(state_path, state)
            current_phase = state.get("active_phase") or "UNKNOWN"
            if current_phase != previous_phase:
                emit_event(kind="STAGE_STARTED", stage=current_phase)
            if "progress" in values:
                item = values["progress"]
                emit_event(kind="PROGRESS", stage=item.get("stage", current_phase),
                    completed_count=item.get("completed", item.get("evidence_completed",
                        item.get("holding_review_completed"))),
                    total_count=item.get("total", item.get("evidence_total",
                        item.get("holding_review_total"))),
                    output_count=item.get("count", item.get("output_count")),
                    elapsed_seconds=item.get("elapsed_seconds"),
                    checkpoint_reused=item.get("checkpoint_reused"))
            if values.get("status") in {"FAILED", "PARTIAL", "DRAFT_READY", "COMPLETED", "CANCELLED"}:
                emit_event(kind="TERMINAL", stage=current_phase,
                           error_code=(values.get("error") or {}).get("code"))
        def progress(event):
            update(progress={key: event[key] for key in (
                "stage", "count", "completed", "total", "evidence_completed",
                "evidence_total", "holding_review_completed", "holding_review_total",
                "elapsed_seconds", "checkpoint_reused",
            ) if key in event})
        def cancel_point():
            path = confined_path(directory, f"cancel-requests/{attempt_id}.json")
            if not path.is_file():
                return
            request = read_json(path)
            integrity = request.pop("integrity_hash", None)
            if (integrity != content_hash(request) or request.get("attempt_id") != attempt_id
                    or request.get("scope") != "RESEARCH_ONLY"
                    or request.get("broker_orders_affected") is not False):
                raise PipelineError("STALE_DATA", "research cancellation request is invalid")
            raise _ResearchCancelled()
        heartbeat_stop = threading.Event()
        def heartbeat():
            while not heartbeat_stop.wait(5):
                emit_event(kind="HEARTBEAT", stage=state.get("active_phase") or "UNKNOWN")
        heartbeat_thread = threading.Thread(target=heartbeat, name=f"research-heartbeat-{attempt_id[:8]}", daemon=True)
        heartbeat_thread.start()
        try:
            update(active_phase="PREPARE", error=None)
            cancel_point()
            if request["code_hashes"] != worker_fingerprint(request["vibe_root"]):
                raise PipelineError("STALE_DATA", "worker implementation/config differs from submitted task")
            if (directory / "portfolio-seal.json").exists():
                # Recover the original seal transaction, never acquire new sources.
                seal = read_json(directory / "portfolio-seal.json")
                if seal.pop("integrity_hash", None) != content_hash(seal):
                    raise PipelineError("STALE_DATA", "portfolio seal integrity mismatch")
                saved = confined_path(directory, seal["directory"])
                account = AccountSnapshot.from_dict(read_json(saved / "account.json", expected_hash=seal["files"]["account.json"]))
                fees = FeeSchedule.from_dict(read_json(saved / "fees.json", expected_hash=seal["files"]["fees.json"]))
                original = read_json(saved / "request.json", expected_hash=seal["files"]["request.json"])
                run_portfolio_draft(repository=PipelineRepository(runs_root, run_id), check_id=seal["check_id"],
                    status_id=seal.get("status_id"), account=account, fees=fees,
                    account_source_hash=original.get("account_source_hash"), clock=clock)
                update(status="DRAFT_READY", active_phase="S6", error=None)
                return state
            client = None
            def source():
                nonlocal client
                if client is None:
                    token_path = Path(request["vibe_root"]) / "Tushare-token.txt"
                    client = (client_factory() if client_factory else
                              configured_tushare_client(token_file=token_path if token_path.exists() else None))
                return client
            class LazySource:
                def __getattr__(self, endpoint):
                    return lambda **params: getattr(source(), endpoint)(**params)
            prepared_path = confined_path(directory, "task-prepared.json")
            if prepared_path.exists():
                prepared = read_json(prepared_path)
                if prepared.pop("integrity_hash", None) != content_hash(prepared):
                    raise PipelineError("STALE_DATA", "prepared task input hash mismatch")
            else:
                if date.fromisoformat(request["research_date"]) != clock().astimezone(SHANGHAI).date():
                    raise PipelineError("NOT_READY", "historical preparation requires a previously frozen snapshot")
                prepared = (prepare or prepare_dataset)(request, directory, source(), clock)
                prepared.update(snapshot_hash=file_hash(prepared["snapshot_path"]),
                                manifest_hash=file_hash(prepared["manifest_path"]))
                atomic_json(prepared_path, prepared | {"integrity_hash": content_hash(prepared)}, immutable=True)
            cancel_point()
            snapshot = read_json(prepared["snapshot_path"], expected_hash=prepared["snapshot_hash"])
            manifest = read_json(prepared["manifest_path"], expected_hash=prepared["manifest_hash"])
            data = manifest["context"]
            if any(data[key] != request[key] for key in ("research_date", "phase", "as_of")):
                raise PipelineError("STALE_DATA", "prepared context differs from requested cutoff")
            context = ResearchContext(date.fromisoformat(data["research_date"]), data["phase"],
                date.fromisoformat(data["market_data_reference_date"]), datetime.fromisoformat(data["as_of"]))
            config = PipelineConfig.from_dict(request["config"])
            account, fees, source_hash = _execution_inputs(directory)
            repository = PipelineRepository(runs_root, run_id)
            research_source = ResearchDataClient(LazySource(), context)
            update(active_phase="S0_S4")
            run_screening(dataset_root=Path(prepared["dataset_root"]), manifest_path=Path(prepared["manifest_path"]),
                snapshot=snapshot, context=context, config=config, repository=repository,
                source_client=research_source, vibe_root=Path(request["vibe_root"]),
                through_stage="S4", progress=progress)
            cancel_point()
            if repository.load_stage("S4") is None or repository.load_stage("S4")["status"] != "SUCCESS":
                raise PipelineError("NOT_READY", "screening did not produce a complete S4 shortlist")
            if not ai_authorized(directory, request):
                update(status="PARTIAL", active_phase="S5", error={"code": "AI_REVIEW_NOT_AUTHORIZED"})
                return state
            update(active_phase="S5")
            news_context = None
            if repository.load_stage("S5") is None:
                try:
                    news_context = (news_fetcher(cutoff=context.as_of, clock=clock) if news_fetcher else
                                    fetch_ninef_news(resolve_ninef_token(), cutoff=context.as_of, clock=clock))
                except NinefAccountAdapterError as exc:
                    raise PipelineError("SOURCE_UNAVAILABLE", "verified Ninef news is unavailable for S5") from exc
                run_reviews(repository=repository, progress=progress, news_context=news_context)
            else:
                news_path = confined_path(directory, "news-context.json")
                if news_path.exists():
                    news_context = read_json(news_path)
            if account is not None and account.positions:
                if news_context is None:
                    raise PipelineError("NOT_READY", "holding reviews require the frozen S5 news context")
                from competition_core.pipeline.dataset import CanonicalDataset
                panel = CanonicalDataset(Path(prepared["dataset_root"]), manifest["source"]).load(
                    Path(prepared["manifest_path"]))
                run_holding_reviews(repository=repository, account=account, account_source_hash=source_hash,
                    snapshot=snapshot, panel=panel, sessions=manifest["session_dates"], context=context,
                    config=config, source_client=research_source, source_name=manifest["source"],
                    news_context=news_context, progress=progress)
            cancel_point()
            if context.phase != "intraday" or context.research_date != clock().astimezone(SHANGHAI).date():
                update(status="PARTIAL", active_phase="S5_5", error={"code": "CURRENT_INTRADAY_EXECUTION_REQUIRED"})
                return state
            update(active_phase="EXECUTION_REFERENCES")
            status_id, check_id = f"status-{attempt_id}", f"quote-{attempt_id}"
            run_execution_status_check(repository=repository, client=source(), source_name=manifest["source"],
                                       account=account, status_id=status_id, clock=clock)
            cancel_point()
            update(active_phase="S5_5")
            run_realtime_check(repository=repository, account=account, check_id=check_id, clock=clock)
            cancel_point()
            update(active_phase="S6")
            portfolio = run_portfolio_draft(repository=repository, check_id=check_id, status_id=status_id,
                draft_id=f"draft-{attempt_id}", account=account, fees=fees, account_source_hash=source_hash, clock=clock)
            update(status="DRAFT_READY" if portfolio["execution_ready"] else "PARTIAL",
                   error=None if portfolio["execution_ready"] else {"code": "PORTFOLIO_NOT_READY"})
            return state
        except _ResearchCancelled:
            state.update(status="CANCELLED", error=None, execution_ready=False,
                         updated_at=clock().isoformat())
            atomic_json(state_path, state)
            emit_event(kind="TERMINAL", stage=state.get("active_phase") or "UNKNOWN",
                       error_code="RESEARCH_CANCELLED")
            return state
        except Exception as exc:
            failure = safe_error_record(exc, failed_step=state.get("active_phase"))
            state.update(status="FAILED", error=failure, updated_at=clock().isoformat())
            try:
                atomic_json(state_path, state)
            except Exception:
                # The immutable attempt record below remains the read-only recovery fact
                # when a transient state-file sharing conflict prevents this write.
                pass
            emit_event(kind="FAILED", stage=state.get("active_phase") or "UNKNOWN",
                       error_code=failure["code"], retryable=failure["retryable"],
                       diagnostic_id=failure["diagnostic_id"])
            raise
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1)
            atomic_json(confined_path(directory, f"task-attempts/{attempt_id}.json"), state, immutable=True)

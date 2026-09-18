"""Local program-owned jobs. HTTP submits intent, never runs an Agent tool chain."""

from contextlib import contextmanager
from datetime import datetime
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import uuid

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json, confined_path
from competition_core.pipeline.config import load_pipeline_config
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.time_context import SHANGHAI
from competition_core.research_events import append_task_event

ROOT = Path(__file__).resolve().parents[1]


def checked_directory(root, run_id, *, existing=True):
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}", run_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid research task identifier")
    root = Path(root).resolve()
    target = root / run_id
    if target.resolve() != target or not target.resolve().is_relative_to(root):
        raise PipelineError("INVALID_ARGUMENTS", "research directory is not a confined direct child")
    if existing and not confined_path(target, "task-request.json").is_file():
        raise PipelineError("NOT_FOUND", "program-owned task does not exist")
    return target


@contextmanager
def task_lease(path, *, create=False):
    """Separate from Repository's stage writer lock; released on process death."""
    descriptor = os.open(path, os.O_RDWR | (os.O_CREAT if create else 0))
    acquired = False
    try:
        if os.fstat(descriptor).st_size == 0:
            if not create:
                raise PipelineError("STALE_DATA", "worker lock file is invalid")
            os.write(descriptor, b"0")
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError as exc:
            raise PipelineError("TASK_BUSY", "research task has an active worker") from exc
        yield
    finally:
        if acquired:
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def lease_busy(directory):
    path = confined_path(directory, ".task-worker.lock")
    if not path.exists():
        return False
    try:
        with task_lease(path):
            return False
    except PipelineError as exc:
        if exc.code == "TASK_BUSY":
            return True
        raise


def worker_fingerprint(vibe_root):
    from competition_core.pipeline.runner import code_fingerprint
    names = ("competition_core/research_tasks.py", "competition_core/research_worker.py",
             "scripts/research-task-worker.py", "scripts/prepare-canonical.py",
             "config/competition.yaml", "config/pipeline-config.yaml", "adapters/tushare/client.py",
             "adapters/ninef/account.py")
    return code_fingerprint(Path(vibe_root)) | {name: file_hash(ROOT / name) for name in names}


def task_request(directory):
    data = read_json(confined_path(directory, "task-request.json"))
    if data.pop("integrity_hash", None) != content_hash(data):
        raise PipelineError("STALE_DATA", "task request integrity mismatch")
    return data


def ai_authorized(directory, request):
    if request["ai_review"]:
        return True
    path = confined_path(directory, "ai-authorization.json")
    if not path.exists():
        return False
    approval = read_json(path)
    if (approval.pop("integrity_hash", None) != content_hash(approval)
            or approval.get("request_hash") != content_hash(request)
            or approval.get("authorized") is not True):
        raise PipelineError("STALE_DATA", "AI authorization is not bound to this task")
    return True


_TERMINAL_TASK_STATUSES = frozenset({"FAILED", "PARTIAL", "DRAFT_READY", "COMPLETED", "CANCELLED"})


def _current_attempt_state(directory, saved, *, worker_live):
    """Prefer the immutable current-attempt fact after its worker has stopped."""
    if worker_live or saved.get("status") not in {"RUNNING", "QUEUED"}:
        return saved, "task_state"
    attempt_id = saved.get("attempt_id")
    if not isinstance(attempt_id, str) or re.fullmatch(r"[0-9a-f]{32}", attempt_id) is None:
        return saved, "task_state"
    path = confined_path(directory, f"task-attempts/{attempt_id}.json")
    if not path.is_file():
        return saved, "task_state"
    attempt = read_json(path)
    if attempt.get("attempt_id") != attempt_id or attempt.get("status") not in _TERMINAL_TASK_STATUSES:
        return saved, "task_state"
    return attempt, "task_attempt"


def _public_task_error(value):
    """Project only bounded diagnostics written by the worker safety boundary."""
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    if not isinstance(code, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code) is None:
        code = "RESEARCH_TASK_FAILED"
    result = {"code": code}
    message = value.get("message")
    if isinstance(message, str) and 0 < len(message) <= 160:
        result["message"] = message
    failed_step = value.get("failed_step")
    if failed_step in {"PREPARE", "S0_S4", "S5", "S5_5", "EXECUTION_REFERENCES", "S6", "UNKNOWN"}:
        result["failed_step"] = failed_step
    if type(value.get("retryable")) is bool:
        result["retryable"] = value["retryable"]
    diagnostic_id = value.get("diagnostic_id")
    if isinstance(diagnostic_id, str) and re.fullmatch(r"diag-[0-9a-f]{32}", diagnostic_id):
        result["diagnostic_id"] = diagnostic_id
    return result


class ResearchTasks:
    """One active job per service; immutable intent and explicit account imports.

    Missing handles after service restart are not trusted from saved RUNNING text:
    the OS worker lease determines whether a resume can be dispatched safely.
    """

    def __init__(self, runs_root=None, *, vibe_root=None, launch=None, clock=None):
        self.root = Path(runs_root or ROOT / "runs").resolve()
        self.vibe_root = Path(vibe_root or ROOT).resolve()
        self.clock = clock or (lambda: datetime.now(SHANGHAI))
        self._launch = launch or subprocess.Popen
        self._children = {}
        self._mutex = threading.RLock()

    def create(self, *, research_date, phase, as_of, ai_review):
        now = self.clock()
        if (type(ai_review) is not bool or phase not in {"preopen", "intraday", "postclose"}
                or not isinstance(as_of, datetime) or as_of.utcoffset() is None or as_of > now
                or as_of.astimezone(SHANGHAI).date() != research_date
                or research_date != now.astimezone(SHANGHAI).date()):
            raise PipelineError("INVALID_ARGUMENTS", "new research requires an explicit current-date cutoff, phase and AI choice")
        with self._mutex:
            self._capacity()
            run_id = "web-" + uuid.uuid4().hex[:20]
            directory = checked_directory(self.root, run_id, existing=False)
            config = load_pipeline_config()
            request = {"schema_version": "competition.web-task.v1", "run_id": run_id,
                "research_date": research_date.isoformat(), "phase": phase, "as_of": as_of.isoformat(),
                "ai_review": ai_review, "config": config.to_dict(), "vibe_root": str(self.vibe_root),
                "code_hashes": worker_fingerprint(self.vibe_root), "created_at": now.isoformat()}
            atomic_json(directory / "task-request.json", request | {"integrity_hash": content_hash(request)}, immutable=True)
            with task_lease(directory / ".task-worker.lock", create=True):
                pass
            self._dispatch(run_id)
            return self.status(run_id)

    def _capacity(self):
        # One warm-up/research worker at a time avoids concurrent full-market scans.
        self._children = {key: child for key, child in self._children.items() if child.poll() is None}
        if self._children:
            raise PipelineError("TASK_BUSY", "another research task is running")
        if self.root.exists():
            for directory in self.root.iterdir():
                if directory.is_dir() and not directory.is_symlink() and (directory / "task-request.json").is_file():
                    if lease_busy(checked_directory(self.root, directory.name)):
                        raise PipelineError("TASK_BUSY", "another research worker still holds its lease")

    def _dispatch(self, run_id):
        directory = checked_directory(self.root, run_id)
        attempt_id = uuid.uuid4().hex
        atomic_json(confined_path(directory, "task-state.json"), {"status": "QUEUED", "attempt_id": attempt_id,
                    "updated_at": self.clock().isoformat(), "execution_ready": False})
        append_task_event(directory, run_id=run_id, attempt_id=attempt_id,
                          event_time=self.clock(), kind="QUEUED")
        try:
            child = self._launch([sys.executable, str(ROOT / "scripts/research-task-worker.py"),
                "--runs-root", str(self.root), "--run-id", run_id, "--attempt-id", attempt_id],
                cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=os.environ | {"PYTHONDONTWRITEBYTECODE": "1"},
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            self._children[run_id] = child
        except Exception as exc:
            atomic_json(confined_path(directory, "task-state.json"), {"status": "FAILED", "attempt_id": attempt_id,
                "error": {"code": "WORKER_START_FAILED", "exception_type": type(exc).__name__},
                "updated_at": self.clock().isoformat(), "execution_ready": False})
            append_task_event(directory, run_id=run_id, attempt_id=attempt_id,
                              event_time=self.clock(), kind="FAILED",
                              error_code="WORKER_START_FAILED", retryable=False)
            raise PipelineError("WORKER_START_FAILED", "could not start the research worker") from exc

    def resume(self, run_id):
        with self._mutex:
            directory = checked_directory(self.root, run_id)
            request = task_request(directory)
            if request["code_hashes"] != worker_fingerprint(request["vibe_root"]):
                raise PipelineError("STALE_DATA", "task implementation changed; retain this run and create a new task")
            self._capacity()
            with task_lease(confined_path(directory, ".task-worker.lock")):
                if (directory / "S6.json").exists():
                    raise PipelineError("NOT_READY", "sealed portfolios require historical inspection, not new market observations")
            self._dispatch(run_id)
            return self.status(run_id)

    def cancel(self, run_id):
        """Request cooperative research cancellation; this is never an order cancel."""
        with self._mutex:
            directory = checked_directory(self.root, run_id)
            current = self.status(run_id)
            if not current["worker_live"]:
                raise PipelineError("NOT_READY", "research worker is not running")
            state = read_json(confined_path(directory, "task-state.json"))
            attempt_id = state.get("attempt_id")
            if not isinstance(attempt_id, str) or re.fullmatch(r"[0-9a-f]{32}", attempt_id) is None:
                raise PipelineError("STALE_DATA", "research attempt identity is invalid")
            request = {"attempt_id": attempt_id, "requested_at": self.clock().isoformat(),
                       "scope": "RESEARCH_ONLY", "broker_orders_affected": False}
            atomic_json(confined_path(directory, f"cancel-requests/{attempt_id}.json"),
                        request | {"integrity_hash": content_hash(request)}, immutable=True)
            return current | {"cancel_requested": True}

    def status(self, run_id):
        directory = checked_directory(self.root, run_id)
        request = task_request(directory)
        state_path = confined_path(directory, "task-state.json")
        saved = read_json(state_path) if state_path.exists() else {}
        with self._mutex:
            child = self._children.get(run_id)
            live = (child is not None and child.poll() is None) or lease_busy(directory)
        projected, state_source = _current_attempt_state(directory, saved, worker_live=live)
        attempt_id = projected.get("attempt_id")
        cancel_requested = (isinstance(attempt_id, str)
            and confined_path(directory, f"cancel-requests/{attempt_id}.json").is_file())
        stored_status = projected.get("status", "PENDING")
        interrupted = not live and stored_status in {"RUNNING", "QUEUED"}
        unchanged = request["code_hashes"] == worker_fingerprint(request["vibe_root"])
        return {"run_id": run_id, "research_date": request["research_date"], "phase": request["phase"],
            "as_of": request["as_of"], "ai_review": ai_authorized(directory, request),
            "status": "INTERRUPTED" if interrupted else stored_status, "worker_live": live,
            "active_phase": projected.get("active_phase"), "progress": projected.get("progress"),
            "updated_at": projected.get("updated_at"), "state_source": state_source,
            "error": _public_task_error(projected.get("error")),
            "worker_pid": projected.get("worker_pid") if live else None,
            "worker_started_at": projected.get("worker_started_at"),
            "cancel_requested": cancel_requested and live,
            "resume_available": not live and unchanged and not (directory / "S6.json").exists(),
            "resume_blocked_reason": None if unchanged else "IMPLEMENTATION_CHANGED_CREATE_NEW_TASK",
            "production_accepted": False, "current_execution_ready": False}

    def authorize_ai(self, run_id, *, authorized):
        if authorized is not True:
            raise PipelineError("INVALID_ARGUMENTS", "explicit AI authorization is required")
        with self._mutex:
            directory = checked_directory(self.root, run_id)
            request = task_request(directory)
            self._capacity()
            with task_lease(confined_path(directory, ".task-worker.lock")):
                if confined_path(directory, "portfolio-seal.json").exists():
                    raise PipelineError("NOT_READY", "sealed research cannot request more AI work")
                if not ai_authorized(directory, request):
                    approval = {"request_hash": content_hash(request), "authorized": True,
                                "observed_at": self.clock().isoformat()}
                    atomic_json(confined_path(directory, "ai-authorization.json"),
                                approval | {"integrity_hash": content_hash(approval)}, immutable=True)
            return self.resume(run_id)

    def import_execution_inputs(self, run_id, *, account, fees, source_receipt=None):
        from competition_core.pipeline.account import AccountSnapshot
        from competition_core.pipeline.portfolio import FeeSchedule
        account = AccountSnapshot.from_dict(account)
        fees = FeeSchedule.from_dict(fees)
        with self._mutex:
            directory = checked_directory(self.root, run_id)
            if self.status(run_id)["worker_live"] or (directory / "portfolio-seal.json").exists():
                raise PipelineError("TASK_BUSY", "cannot change inputs during a worker or after sealing")
            with task_lease(confined_path(directory, ".task-worker.lock")):
                body = {"account": account.to_dict(), "fees": fees.to_dict()}
                if source_receipt is not None:
                    if not isinstance(source_receipt, dict):
                        raise PipelineError("INVALID_ARGUMENTS", "execution input receipt must be structured")
                    body["source_receipt"] = source_receipt
                digest = content_hash(body)
                atomic_json(confined_path(directory, f"execution-imports/{digest}.json"), body, immutable=True)
                atomic_json(confined_path(directory, "execution-inputs.json"), {"id": digest})
        return {"run_id": run_id, "input_id": digest, "source_kind": account.source_kind,
                "as_of": account.as_of.isoformat(), "execution_ready": False, "orders_submitted": 0}

    def import_ninef_execution_inputs(self, run_id):
        """Read current account/fees from Ninef; never expose or persist the token."""
        directory = checked_directory(self.root, run_id)
        with self._mutex:
            if self.status(run_id)["worker_live"] or (directory / "portfolio-seal.json").exists():
                raise PipelineError("TASK_BUSY", "cannot change inputs during a worker or after sealing")
        from adapters.ninef.account import (
            NinefAccountAdapterError,
            fetch_ninef_account,
            resolve_ninef_token,
        )
        try:
            token = resolve_ninef_token()
        except NinefAccountAdapterError as exc:
            raise PipelineError("NOT_CONFIGURED", "Ninef read-only account credential is not configured") from exc
        try:
            fetched = fetch_ninef_account(token)
        except NinefAccountAdapterError as exc:
            raise PipelineError("SOURCE_UNAVAILABLE", "Ninef read-only account snapshot is unavailable") from exc
        finally:
            token = None
        if fetched.fee_schedule is None:
            raise PipelineError("NOT_READY", "Ninef has no current-day fee evidence")
        result = self.import_execution_inputs(
            run_id,
            account=fetched.snapshot.to_dict(),
            fees=fetched.fee_schedule.to_dict(),
            source_receipt=fetched.receipt,
        )
        return result | {
            "provider": "ninef",
            "position_count": len(fetched.snapshot.positions),
            "pending_order_count": fetched.snapshot.pending_order_count,
        }

"""Authenticated loopback API and same-origin UI. No raw buy/sell endpoint."""
from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import Field, StrictBool
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .execution import GateError
from .models import Contract, canonical, now_utc
from .service import Workbench
from .store import ProcessLock


@dataclass(frozen=True)
class WebSettings:
    password: str
    demo: bool = True
    start_worker: bool = True


class Login(Contract):
    password: str = Field(min_length=1, max_length=256)


class ApproveRequest(Contract):
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    acknowledge: StrictBool


class AcknowledgeRequest(Contract):
    acknowledge: StrictBool


class ResolveUnknownRequest(Contract):
    broker_order_id: str = Field(min_length=1, max_length=160, pattern=r"^[\w.:-]+$")
    evidence_ref: str = Field(min_length=1, max_length=240)
    acknowledge: StrictBool


def create_app(workbench: Workbench, settings: WebSettings) -> FastAPI:
    if not settings.demo and len(settings.password) < 12:
        raise ValueError("Local-account UI requires a password of at least 12 characters.")
    if settings.demo != (workbench.provider.capabilities().environment == "DEMO"):
        raise ValueError("Web mode and provider environment differ.")
    process_lock = ProcessLock(workbench.store.path.with_suffix(".process.lock"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        process_lock.acquire()
        try:
            workbench.store.recover()
            if settings.start_worker:
                workbench.start_worker()
            yield
        finally:
            workbench.close()
            process_lock.release()

    app = FastAPI(title="Vibe-Trading", version="0.1.0", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "[::1]"])
    failures: dict[str, list[float]] = {}

    def session(request: Request) -> dict | None:
        token = request.cookies.get("vibe_session", "")
        if not token:
            return None
        hashed = hashlib.sha256(token.encode()).hexdigest()
        with workbench.store.connect() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE hash=? AND expires_at>?",
                               (hashed, now_utc().isoformat())).fetchone()
        return dict(row) if row else None

    def response(body, status=200):
        return JSONResponse(json.loads(canonical(body)), status_code=status)

    @app.middleware("http")
    async def security(request: Request, call_next):
        path = request.url.path
        if path.startswith("/api/"):
            if request.method not in ("GET", "HEAD"):
                expected_origin = str(request.base_url).rstrip("/")
                if request.headers.get("origin") != expected_origin or request.headers.get("x-vibe-client") != "web":
                    return response({"code": "ORIGIN_REJECTED", "detail": "Same-origin requests only."}, 403)
                try:
                    size = int(request.headers.get("content-length", "0"))
                except ValueError:
                    return response({"code": "INVALID_CONTENT_LENGTH"}, 400)
                if size < 0 or size > 8192:
                    return response({"code": "REQUEST_TOO_LARGE"}, 413)
            if path != "/api/login":
                auth = session(request)
                if not auth:
                    return response({"code": "AUTH_REQUIRED", "detail": "Sign in to the local workbench."}, 401)
                if request.method not in ("GET", "HEAD") and not secrets.compare_digest(
                        request.headers.get("x-csrf-token", ""), auth["csrf"]):
                    return response({"code": "CSRF_REJECTED", "detail": "Refresh authenticated session."}, 403)
                request.state.auth = auth
        try:
            result = await call_next(request)
        except Exception:
            # Catch before ServerErrorMiddleware can re-raise a provider exception
            # into Uvicorn logs. Raw exception text may contain secret headers.
            result = response({"code": "LOCAL_PROVIDER_ERROR", "detail": "Operation failed; inspect locally. No raw provider error is exposed."}, 503)
        result.headers["X-Content-Type-Options"] = "nosniff"
        result.headers["Referrer-Policy"] = "no-referrer"
        result.headers["Cache-Control"] = "no-store"
        result.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; "
            "img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        return result

    @app.exception_handler(GateError)
    async def gate_error(request: Request, exc: GateError):
        return response({"code": exc.code, "detail": exc.detail}, 409)

    @app.exception_handler(KeyError)
    async def not_found(request: Request, exc: KeyError):
        return response({"code": "NOT_FOUND", "detail": "Resource not found."}, 404)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return response({"code": "INVALID_REQUEST", "detail": "Request schema validation failed."}, 422)

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception):
        return response({"code": "LOCAL_PROVIDER_ERROR", "detail": "Operation failed; inspect locally."}, 503)

    @app.post("/api/login")
    def login(body: Login, request: Request):
        ip = request.client.host if request.client else "local"
        current = time.monotonic()
        failures[ip] = [t for t in failures.get(ip, []) if current-t < 60]
        if len(failures[ip]) >= 8:
            return response({"code": "RATE_LIMITED"}, 429)
        if not secrets.compare_digest(body.password.encode(), settings.password.encode()):
            failures[ip].append(current)
            return response({"code": "INVALID_PASSWORD"}, 401)
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        hashed = hashlib.sha256(token.encode()).hexdigest()
        with workbench.store.transaction() as conn:
            conn.execute("DELETE FROM sessions WHERE expires_at<=?", (now_utc().isoformat(),))
            conn.execute("INSERT INTO sessions VALUES(?,?,?)", (hashed, csrf, (now_utc()+timedelta(hours=8)).isoformat()))
        result = response({"csrf": csrf, "demo": settings.demo})
        result.set_cookie("vibe_session", token, httponly=True, samesite="strict", max_age=28800, path="/")
        return result

    @app.get("/api/session")
    def get_session(request: Request):
        return {"csrf": request.state.auth["csrf"], "demo": settings.demo}

    @app.post("/api/logout")
    def logout(request: Request):
        with workbench.store.transaction() as conn:
            conn.execute("DELETE FROM sessions WHERE hash=?", (request.state.auth["hash"],))
        result = response({"ok": True})
        result.delete_cookie("vibe_session")
        return result

    @app.get("/api/dashboard")
    def dashboard():
        return response(workbench.dashboard())

    @app.get("/api/runs")
    def runs():
        return workbench.store.list("run")

    @app.post("/api/runs")
    def create_run():
        return {"run_id": workbench.create_run()}

    @app.get("/api/runs/{run_id}")
    def run(run_id: str):
        return workbench.store.get("run", run_id)

    @app.post("/api/runs/{run_id}/cancel")
    def cancel_run(run_id: str):
        workbench.cancel_run(run_id)
        return {"ok": True}

    @app.post("/api/runs/{run_id}/plan")
    def replan(run_id: str):
        if workbench.store.get("run", run_id)["state"] != "COMPLETED":
            raise GateError("RESEARCH_NOT_COMPLETE", "Wait for completed research.")
        return {"plan_id": workbench.replan(run_id)}

    @app.get("/api/runs/{run_id}/evidence")
    def evidence(run_id: str):
        return {"research": workbench.store.get("research_result", run_id),
                "review_context": workbench.store.get("review_context", run_id)}

    @app.get("/api/plans/{plan_id}")
    def plan(plan_id: str):
        workbench.execution.plan(plan_id)
        return workbench.store.get("plan", plan_id)

    @app.post("/api/plans/{plan_id}/approve")
    def approve(plan_id: str, body: ApproveRequest):
        if not body.acknowledge:
            raise GateError("ACKNOWLEDGEMENT_REQUIRED", "Explicitly approve these exact orders.")
        return response(workbench.execution.approve(plan_id, body.plan_hash, "local_owner"))

    @app.post("/api/approvals/{approval_id}/execute")
    def execute(approval_id: str):
        return {"batch_id": workbench.execution.start(approval_id)}

    @app.get("/api/batches")
    def batches():
        return workbench.execution.list()

    @app.get("/api/batches/{batch_id}")
    def batch(batch_id: str):
        return workbench.execution.snapshot(batch_id)

    @app.post("/api/batches/{batch_id}/stop")
    def stop(batch_id: str):
        workbench.execution.stop(batch_id)
        return {"ok": True, "open_orders_cancelled": False}

    @app.post("/api/batches/{batch_id}/intents/{intent_id}/cancel")
    def cancel_order(batch_id: str, intent_id: str, body: AcknowledgeRequest):
        if not body.acknowledge:
            raise GateError("ACKNOWLEDGEMENT_REQUIRED", "Explicit cancellation consent is required.")
        workbench.execution.request_cancel(batch_id, intent_id)
        return {"ok": True, "status": "REQUESTED_NOT_CONFIRMED"}

    @app.post("/api/batches/{batch_id}/intents/{intent_id}/resolve")
    def resolve_unknown(batch_id: str, intent_id: str, body: ResolveUnknownRequest):
        if not body.acknowledge:
            raise GateError("ACKNOWLEDGEMENT_REQUIRED", "Explicit reconciliation consent is required.")
        return response(workbench.execution.resolve_unknown(
            batch_id, intent_id, body.broker_order_id, body.evidence_ref, "local_owner"
        ))

    def valid_scope(scope: str) -> None:
        for kind in ("run", "plan"):
            try:
                workbench.store.get(kind, scope)
                return
            except KeyError:
                pass
        workbench.execution.snapshot(scope)

    @app.get("/api/events/{scope}")
    def events(scope: str, after: int = 0):
        valid_scope(scope)
        return workbench.store.events(scope, max(0, after))

    @app.get("/api/events/{scope}/stream")
    async def stream(scope: str, request: Request, after: int = 0):
        valid_scope(scope)
        try:
            cursor = max(0, int(request.headers.get("last-event-id", str(after))))
        except ValueError:
            return response({"code": "INVALID_CURSOR"}, 422)
        async def generate():
            nonlocal cursor
            for _ in range(120):
                if await request.is_disconnected() or session(request) is None:
                    return
                batch = workbench.store.events(scope, cursor)
                for item in batch:
                    cursor = item["seq"]
                    yield f"id: {cursor}\nevent: update\ndata: {canonical(item)}\n\n"
                if not batch:
                    yield ": keepalive\n\n"
                await asyncio.sleep(0.5)
        return StreamingResponse(generate(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no"})

    web = Path(__file__).parent / "web"
    app.mount("/static", StaticFiles(directory=web), name="static")

    @app.get("/")
    def index():
        return FileResponse(web/"index.html")

    return app

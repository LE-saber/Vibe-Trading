"""Orchestration: provider reads -> visible research -> immutable reviewed plan."""
from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from .execution import ExecutionService, GateError
from .models import Policy, ResearchRequest, ResearchResult, canonical, digest, now_utc
from .news import review_context
from .planner import build_plan
from .ports import Provider
from .progress import Reporter, RunCancelled
from .store import Store


class Workbench:
    def __init__(self, store: Store, provider: Provider, enabled: bool = False,
                 policy: Policy | None = None, poll_seconds: float = 2):
        self.store, self.provider = store, provider
        self.policy = policy or Policy()
        self.execution = ExecutionService(store, provider, enabled)
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="vibe-research")
        self.stop_event = threading.Event()
        self.poll_seconds = poll_seconds
        self.worker: threading.Thread | None = None
        self.dashboard_cache: dict = {}
        self.cache_lock = threading.Lock()
        cap = provider.capabilities()
        self.store.bind_provider(cap.provider + ":" + cap.environment)

    def start_worker(self) -> None:
        def loop() -> None:
            while not self.stop_event.wait(self.poll_seconds):
                for batch_id in self.execution.active():
                    if self.stop_event.is_set():
                        return
                    self.execution.tick(batch_id)
                    with self.cache_lock:
                        self.dashboard_cache.pop("account", None)
        self.worker = threading.Thread(target=loop, name="vibe-execution", daemon=True)
        self.worker.start()

    def close(self) -> None:
        self.stop_event.set()
        for run in self.store.list("run"):
            if run["state"] in ("RUNNING", "QUEUED"):
                self.cancel_run(run["id"])
        # Local adapters must implement bounded calls and cooperative cancellation.
        self.pool.shutdown(wait=True, cancel_futures=True)
        if self.worker:
            self.worker.join()

    def dashboard(self) -> dict:
        now = now_utc()
        result = {"capabilities": self.provider.capabilities(), "observed_at": now,
                  "execution_enabled": self.execution.enabled}
        with self.cache_lock:
            for name, ttl, function in (("account", 5, lambda: self.provider.context([]).account),
                                        ("leaderboard", 30, self.provider.leaderboard),
                                        ("news", 30, self.provider.news)):
                cached = self.dashboard_cache.get(name)
                if cached and (now-cached[0]).total_seconds() < ttl:
                    result[name] = cached[1]
                    continue
                try:
                    value = {"available": True, "data": function(), "error": None}
                except Exception:
                    value = {"available": False, "data": None, "error": "PROVIDER_READ_FAILED"}
                self.dashboard_cache[name] = (now, value)
                result[name] = value
        return result

    def create_run(self, *, asynchronous: bool = True) -> str:
        run_id, now = uuid4().hex, now_utc()
        run = {"id": run_id, "state": "QUEUED", "stage": None, "completed": 0, "total": None,
               "percent": 0, "message": "Waiting for provider reads.", "created_at": now.isoformat(),
               "updated_at": now.isoformat(), "plan_id": None, "elapsed_seconds": 0}
        with self.store.transaction() as conn:
            rows = conn.execute("SELECT body FROM documents WHERE kind='run'").fetchall()
            if any(json.loads(r[0])["state"] in ("QUEUED", "RUNNING", "CANCELLING") for r in rows):
                raise GateError("RESEARCH_BUSY", "One research run is already active.")
            conn.execute("INSERT INTO documents VALUES('run',?,?)", (run_id, canonical(run)))
            self.store.event_in(conn, run_id, "queued", run)
        if asynchronous:
            self.pool.submit(self._research, run_id)
        else:
            self._research(run_id)
        return run_id

    def cancel_run(self, run_id: str) -> None:
        with self.store.transaction() as conn:
            row = conn.execute("SELECT body FROM documents WHERE kind='run' AND id=?", (run_id,)).fetchone()
            if not row:
                raise KeyError(run_id)
            run = json.loads(row[0])
            if run["state"] in ("RUNNING", "QUEUED"):
                run["state"] = "CANCELLING"
                conn.execute("UPDATE documents SET body=? WHERE kind='run' AND id=?", (canonical(run), run_id))
                self.store.event_in(conn, run_id, "cancelling", run)

    def _finish(self, run_id: str, state: str, message: str, plan_id: str | None = None) -> None:
        with self.store.transaction() as conn:
            run = json.loads(conn.execute("SELECT body FROM documents WHERE kind='run' AND id=?", (run_id,)).fetchone()[0])
            if run["state"] == "CANCELLING":
                state, message, plan_id = "CANCELLED", "Cancelled by user.", None
            run.update(state=state, message=message, plan_id=plan_id, updated_at=now_utc().isoformat())
            if state == "COMPLETED":
                run["percent"] = 100
            conn.execute("UPDATE documents SET body=? WHERE kind='run' AND id=?", (canonical(run), run_id))
            self.store.event_in(conn, run_id, "finished", run)

    def _research(self, run_id: str) -> None:
        reporter = Reporter(self.store, run_id)
        try:
            reporter("PREPARE", 0, 1, "Reading account, competition context and news.")
            account = self.provider.context([]).account
            news = self.provider.news()  # Failure is not silently interpreted as an empty result.
            try:
                ranking = self.provider.leaderboard()
            except Exception:
                ranking = None  # Optional context; never automatic risk escalation.
            request = ResearchRequest(request_id=run_id, requested_at=now_utc(), account=account,
                                      news=news, competition=ranking)
            self.store.put("research_request", run_id, request, immutable=True)
            reporter("PREPARE", 1, 1, "Input snapshot stored; research does not authorize trading.")
            result = self.provider.research(request, reporter, reporter.cancelled)
            if result.run_id != run_id:
                raise GateError("RUN_MISMATCH", "Research returned the wrong run ID.")
            if result.demo != (self.provider.capabilities().environment == "DEMO"):
                raise GateError("MODE_MISMATCH", "Demo and local account data must not mix.")
            selected = review_context(news, set(result.candidates) | {p.symbol for p in account.positions}, request.requested_at)
            if result.news_context_hash != selected["news_hash"]:
                raise GateError("NEWS_CONTEXT_MISMATCH", "Research did not bind required selected news context.")
            self.store.put("research_result", run_id, result, immutable=True)
            self.store.put("review_context", run_id, selected, immutable=True)
            reporter("S6", 0, 1, "Building current-to-target quantities against a fresh account.")
            plan_id = self.replan(run_id)
            reporter("S6", 1, 1, "Plan persisted; user review is required before any write.")
            self._finish(run_id, "COMPLETED", "Research complete. Review plan and blockers.", plan_id)
        except RunCancelled:
            self._finish(run_id, "CANCELLED", "Cancelled by user.")
        except GateError as exc:
            self._finish(run_id, "FAILED", exc.code + ": " + exc.detail)
        except Exception:
            self._finish(run_id, "FAILED", "PROVIDER_OR_CONTRACT_ERROR: inspect local adapter; no trade submitted.")

    def replan(self, run_id: str) -> str:
        result = ResearchResult.model_validate(self.store.get("research_result", run_id))
        context = self.provider.context(list(result.candidates) + [t.symbol for t in result.targets])
        plan = build_plan(result, context, now_utc(), self.policy)
        self.store.put("plan", plan.plan_id, {"plan": plan, "hash": digest(plan)}, immutable=True)
        with self.store.transaction() as conn:
            row = conn.execute("SELECT body FROM documents WHERE kind='run' AND id=?", (run_id,)).fetchone()
            run = json.loads(row[0])
            if run["state"] == "COMPLETED":
                run["plan_id"] = plan.plan_id
                conn.execute("UPDATE documents SET body=? WHERE kind='run' AND id=?", (canonical(run), run_id))
            self.store.event_in(conn, run_id, "plan_created", {"plan_id": plan.plan_id, "blocker_count": len(plan.blockers)})
        return plan.plan_id

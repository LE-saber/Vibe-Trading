"""Process-local supervision around the durable execution ledger.

The ledger, not this thread map, owns safety.  A restart converts any durable
SENDING intent to UNKNOWN and therefore cannot replay an uncertain write.
"""

from __future__ import annotations

from pathlib import Path
import threading
import time

from competition_core.execution.models import Capabilities
from competition_core.execution.service import ExecutionService
from competition_core.execution.store import ExecutionStore


ROOT = Path(__file__).resolve().parents[2]
TERMINAL_BATCH_STATES = frozenset({"STOPPED", "REPLAN_REQUIRED", "EXPIRED", "COMPLETED"})


class DisabledProvider:
    """Explicit placeholder: no real provider is silently inferred or imported."""

    def capabilities(self):
        return Capabilities(provider="unconfigured")

    def context(self, symbols):
        raise RuntimeError("execution provider is not configured")

    def submit(self, order, client_order_id):
        raise RuntimeError("execution provider is not configured")

    def get_order(self, order_id):
        raise RuntimeError("execution provider is not configured")

    def cancel(self, order_id):
        raise RuntimeError("execution provider is not configured")


class ExecutionSupervisor:
    def __init__(self, service: ExecutionService, *, interval_seconds: float = 1.0) -> None:
        self.service = service
        self.interval_seconds = interval_seconds
        self._guard = threading.Lock()
        self._workers: dict[str, threading.Thread] = {}

    def start(self, batch_id: str) -> None:
        with self._guard:
            existing = self._workers.get(batch_id)
            if existing is not None and existing.is_alive():
                return
            worker = threading.Thread(target=self._run, args=(batch_id,), daemon=True,
                                      name=f"competition-execution-{batch_id[:12]}")
            self._workers[batch_id] = worker
            worker.start()

    def _run(self, batch_id: str) -> None:
        try:
            while True:
                state = self.service.snapshot(batch_id)["state"]
                if state in TERMINAL_BATCH_STATES or state in {
                    "MANUAL_RECONCILIATION", "PAUSED", "PAUSED_PROVIDER", "CANCEL_UNKNOWN"
                }:
                    return
                self.service.tick(batch_id)
                time.sleep(self.interval_seconds)
        finally:
            with self._guard:
                self._workers.pop(batch_id, None)


_store = ExecutionStore(ROOT / "storage/state/competition-execution.sqlite")
_service = ExecutionService(
    _store,
    DisabledProvider(),
    # This migrated research package is never an execution authority.  Keep its
    # historical state-machine test surface fail-closed even if a caller sets a
    # legacy environment variable.
    writes_enabled=False,
)
_supervisor = ExecutionSupervisor(_service)


def get_execution_service() -> ExecutionService:
    return _service


def get_execution_supervisor() -> ExecutionSupervisor:
    return _supervisor


__all__ = ["DisabledProvider", "ExecutionSupervisor", "get_execution_service", "get_execution_supervisor"]

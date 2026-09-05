"""Persisted work progress. Percentage is a phase estimate, never an ETA."""
import json
import time
from datetime import datetime

from .models import canonical, now_utc
from .store import Store

STAGES = ("PREPARE", "S0", "S1", "S2", "S3", "S4", "S5", "S5.5", "S6")
# Presentation weights; not claims about the unavailable legacy runtime.
WEIGHTS = (5, 5, 10, 15, 15, 5, 30, 5, 10)


class RunCancelled(Exception):
    pass


class Reporter:
    def __init__(self, store: Store, run_id: str):
        self.store, self.run_id = store, run_id
        self.stage_index, self.completed, self.total = -1, 0, None
        self.last_emission = 0.0

    def cancelled(self) -> bool:
        return self.store.get("run", self.run_id)["state"] in ("CANCELLING", "CANCELLED")

    def __call__(self, stage: str, completed: int, total: int | None, message: str) -> None:
        if self.cancelled():
            raise RunCancelled()
        if stage not in STAGES:
            raise ValueError("unknown progress stage")
        index = STAGES.index(stage)
        if type(completed) is not int or completed < 0:
            raise ValueError("invalid completed count")
        if total is not None and (type(total) is not int or total <= 0 or completed > total):
            raise ValueError("invalid total count")
        if index < self.stage_index or (index == self.stage_index and completed < self.completed):
            raise ValueError("progress regression")
        if index == self.stage_index and self.total is not None and total != self.total:
            raise ValueError("stage denominator changed")
        if index > self.stage_index:
            if index != self.stage_index + 1:
                raise ValueError("stages must be sequential")
            if self.stage_index >= 0 and (self.total is None or self.completed != self.total):
                raise ValueError("previous stage not complete")
        important = index != self.stage_index or (total is not None and completed == total)
        self.stage_index, self.completed, self.total = index, completed, total
        # Avoid one fsync for every stock in a several-thousand-symbol universe.
        if not important and time.monotonic() - self.last_emission < 0.2:
            return
        self.last_emission = time.monotonic()
        percent = min(99, sum(WEIGHTS[:index]) + (WEIGHTS[index] * completed // total if total else 0))
        now = now_utc()
        with self.store.transaction() as conn:
            row = conn.execute("SELECT body FROM documents WHERE kind='run' AND id=?", (self.run_id,)).fetchone()
            run = json.loads(row[0])
            if run["state"] in ("CANCELLING", "CANCELLED"):
                raise RunCancelled()
            run.update(state="RUNNING", stage=stage, completed=completed, total=total,
                       percent=percent, message=message[:240], updated_at=now.isoformat(),
                       elapsed_seconds=max(0, int((now-datetime.fromisoformat(run["created_at"])).total_seconds())))
            conn.execute("UPDATE documents SET body=? WHERE kind='run' AND id=?", (canonical(run), self.run_id))
            self.store.event_in(conn, self.run_id, "progress", run)

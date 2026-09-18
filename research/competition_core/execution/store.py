"""SQLite durability boundary for approvals, intents and reconciliation facts."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator

from competition_core.execution.models import canonical


SCHEMA_VERSION = "competition.execution.sqlite.v1"


class ExecutionStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.transaction() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS plans(
                    id TEXT PRIMARY KEY, plan_hash TEXT NOT NULL, run_id TEXT NOT NULL,
                    seal_hash TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS approvals(
                    id TEXT PRIMARY KEY, plan_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL,
                    FOREIGN KEY(plan_id) REFERENCES plans(id)
                );
                CREATE TABLE IF NOT EXISTS batches(
                    id TEXT PRIMARY KEY, approval_id TEXT NOT NULL UNIQUE, plan_id TEXT NOT NULL,
                    account_key TEXT NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
                    stop_requested INTEGER NOT NULL DEFAULT 0, working INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    FOREIGN KEY(approval_id) REFERENCES approvals(id), FOREIGN KEY(plan_id) REFERENCES plans(id)
                );
                CREATE TABLE IF NOT EXISTS account_locks(
                    account_key TEXT PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE,
                    FOREIGN KEY(batch_id) REFERENCES batches(id)
                );
                CREATE TABLE IF NOT EXISTS intents(
                    id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL, body TEXT NOT NULL, intent_hash TEXT NOT NULL,
                    broker_order_id TEXT, receipt TEXT, cancel_state TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(batch_id) REFERENCES batches(id), UNIQUE(batch_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS resolutions(
                    id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL,
                    FOREIGN KEY(intent_id) REFERENCES intents(id)
                );
                CREATE TABLE IF NOT EXISTS events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, subject_id TEXT NOT NULL,
                    kind TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL
                );
                """
            )
            row = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
            if row is None:
                connection.execute("INSERT INTO metadata VALUES('schema_version',?)", (SCHEMA_VERSION,))
            elif row[0] != SCHEMA_VERSION:
                raise RuntimeError("unsupported execution ledger schema")
        self.recover()

    def event(self, connection: sqlite3.Connection, subject_id: str, kind: str, body: Any) -> None:
        connection.execute(
            "INSERT INTO events(subject_id,kind,body,created_at) VALUES(?,?,?,?)",
            (subject_id, kind, canonical(body), datetime.now(timezone.utc).isoformat()),
        )

    def recover(self) -> None:
        """Never replay a submission whose process died after durable SENDING."""
        with self.transaction() as connection:
            affected = connection.execute(
                "SELECT DISTINCT batch_id FROM intents WHERE state='SENDING'"
            ).fetchall()
            connection.execute("UPDATE intents SET state='UNKNOWN' WHERE state='SENDING'")
            connection.execute("UPDATE batches SET working=0")
            for row in affected:
                connection.execute(
                    "UPDATE batches SET state='MANUAL_RECONCILIATION',reason=? WHERE id=?",
                    ("SUBMISSION_OUTCOME_UNKNOWN: restart after durable SENDING", row[0]),
                )
                self.event(connection, row[0], "restart_unknown", {"retry": False})

    def plan_document(self, plan_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT body FROM plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise KeyError(plan_id)
        return json.loads(row[0])

    def snapshot(self, batch_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            batch = connection.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            intents = connection.execute(
                "SELECT * FROM intents WHERE batch_id=? ORDER BY ordinal", (batch_id,)
            ).fetchall()
        if batch is None:
            raise KeyError(batch_id)
        return dict(batch) | {
            "stop_requested": bool(batch["stop_requested"]),
            "working": bool(batch["working"]),
            "intents": [
                dict(row)
                | {
                    "body": json.loads(row["body"]),
                    "receipt": json.loads(row["receipt"]) if row["receipt"] else None,
                }
                for row in intents
            ],
        }

    def list_batches(self, limit: int = 30) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("batch limit is invalid")
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT id FROM batches ORDER BY created_at DESC,id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self.snapshot(row[0]) for row in rows]

    def events(self, subject_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM events WHERE subject_id=? AND sequence>? ORDER BY sequence",
                (subject_id, after),
            ).fetchall()
        return [dict(row) | {"body": json.loads(row["body"])} for row in rows]


__all__ = ["ExecutionStore", "SCHEMA_VERSION"]

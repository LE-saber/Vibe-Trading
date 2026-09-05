"""Durable local state. One process, thread-safe transactions, no network DB.

SENDING is committed before I/O; a crash there becomes UNKNOWN, never a retry.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .models import canonical, now_utc

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS documents(
 kind TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(kind,id));
CREATE TABLE IF NOT EXISTS events(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL,
 kind TEXT NOT NULL, body TEXT NOT NULL, at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS events_scope ON events(scope,seq);
CREATE TABLE IF NOT EXISTS approvals(
 id TEXT PRIMARY KEY, plan_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches(
 id TEXT PRIMARY KEY, approval_id TEXT NOT NULL UNIQUE,
 account_id TEXT NOT NULL, plan_id TEXT NOT NULL, state TEXT NOT NULL,
 working INTEGER NOT NULL DEFAULT 0, stop_requested INTEGER NOT NULL DEFAULT 0,
 reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS account_locks(
 account_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS intents(
 id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'PLANNED',
 broker_order_id TEXT UNIQUE, receipt TEXT, cancel_state TEXT NOT NULL DEFAULT '',
 UNIQUE(batch_id,ordinal));
CREATE TABLE IF NOT EXISTS sessions(
 hash TEXT PRIMARY KEY, csrf TEXT NOT NULL, expires_at TEXT NOT NULL);
"""


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            if conn.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
                raise RuntimeError("Unsupported database schema; back up before migrating.")
            conn.executescript(SCHEMA)
            conn.execute("PRAGMA user_version=1")
        if os.name != "nt":
            self.path.chmod(0o600)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, factory=ClosingConnection)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bind_provider(self, name: str) -> None:
        with self.transaction() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key='provider'").fetchone()
            if row and row[0] != name:
                raise RuntimeError("Database/provider mismatch. Use a separate database.")
            conn.execute("INSERT OR IGNORE INTO metadata VALUES('provider',?)", (name,))

    def put(self, kind: str, id_: str, body: Any, *, immutable: bool = False) -> None:
        with self.transaction() as conn:
            verb = "INSERT" if immutable else "INSERT OR REPLACE"
            conn.execute(f"{verb} INTO documents VALUES(?,?,?)", (kind, id_, canonical(body)))

    def get(self, kind: str, id_: str) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT body FROM documents WHERE kind=? AND id=?", (kind, id_)).fetchone()
        if not row:
            raise KeyError(id_)
        return json.loads(row[0])

    def list(self, kind: str, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT body FROM documents WHERE kind=? ORDER BY rowid DESC LIMIT ?",
                                (kind, limit)).fetchall()
        return [json.loads(row[0]) for row in rows]

    @staticmethod
    def event_in(conn: sqlite3.Connection, scope: str, kind: str, body: Any) -> int:
        cursor = conn.execute("INSERT INTO events(scope,kind,body,at) VALUES(?,?,?,?)",
                              (scope, kind, canonical(body), now_utc().isoformat()))
        return cursor.lastrowid

    def event(self, scope: str, kind: str, body: Any) -> int:
        with self.transaction() as conn:
            return self.event_in(conn, scope, kind, body)

    def events(self, scope: str, after: int = 0, limit: int = 500) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM events WHERE scope=? AND seq>? ORDER BY seq LIMIT ?",
                                (scope, after, min(limit, 1000))).fetchall()
        return [dict(row) | {"body": json.loads(row["body"])} for row in rows]

    def recover(self) -> None:
        """Called only after obtaining the exclusive process lock."""
        with self.transaction() as conn:
            rows = conn.execute("SELECT id,body FROM documents WHERE kind='run'").fetchall()
            for row in rows:
                run = json.loads(row["body"])
                if run["state"] in ("QUEUED", "RUNNING", "CANCELLING"):
                    run.update(state="INTERRUPTED", message="Process restarted. No automatic replay.")
                    conn.execute("UPDATE documents SET body=? WHERE kind='run' AND id=?",
                                 (canonical(run), row["id"]))
                    self.event_in(conn, row["id"], "interrupted", run)
            rows = conn.execute("SELECT id,batch_id FROM intents WHERE state='SENDING'").fetchall()
            for row in rows:
                conn.execute("UPDATE intents SET state='UNKNOWN' WHERE id=?", (row["id"],))
                self.event_in(conn, row["batch_id"], "submission_unknown", {"intent_id": row["id"]})
            conn.execute("UPDATE intents SET cancel_state='UNKNOWN' WHERE cancel_state='SENDING'")
            conn.execute("UPDATE batches SET state='PAUSED_RESTART',working=0,stop_requested=1,"
                         "reason='Restarted; reconcile existing orders, never replay writes.' "
                         "WHERE id IN (SELECT batch_id FROM account_locks)")


class ProcessLock:
    """Cross-platform advisory lock: one worker per local database."""
    def __init__(self, path: Path):
        self.path, self.file = path, None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self.file.seek(0)
                self.file.write(b"0")
                self.file.flush()
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            self.file.close()
            self.file = None
            raise RuntimeError("Another process owns this database; use one worker.") from exc

    def release(self) -> None:
        if self.file:
            self.file.close()
            self.file = None

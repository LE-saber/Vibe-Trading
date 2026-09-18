"""SQLite state for workflow runs, portfolio snapshots, targets, and intents."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from contracts.order import OrderIntent
from contracts.portfolio import PortfolioState, TargetWeights

DEFAULT_DB = Path(__file__).resolve().parents[2] / "storage" / "state" / "competition.sqlite"


class WorkflowRepository:
    def __init__(self, database: str | Path = DEFAULT_DB) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY, phase TEXT NOT NULL, status TEXT NOT NULL,
                    fallback_used INTEGER NOT NULL, artifact_dir TEXT NOT NULL,
                    payload TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS portfolio_states (
                    as_of TEXT PRIMARY KEY, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS portfolio_targets (
                    target_id TEXT PRIMARY KEY, as_of TEXT NOT NULL, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS order_intents (
                    intent_id TEXT PRIMARY KEY, as_of TEXT NOT NULL, target_id TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                """
            )

    def put_run(self, run_id: str, phase: str, status: str, fallback_used: bool, artifact_dir: str, payload: dict) -> None:
        updated = str(payload.get("finished_at") or payload.get("started_at") or "")
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO workflow_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_id, phase, status, int(fallback_used), artifact_dir, json.dumps(payload), updated),
            )

    def put_portfolio(self, state: PortfolioState) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO portfolio_states VALUES (?, ?)",
                (state.as_of.isoformat(), json.dumps(state.to_dict())),
            )

    def latest_portfolio(self) -> PortfolioState:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM portfolio_states ORDER BY as_of DESC LIMIT 1"
            ).fetchone()
        if row is None:
            raise KeyError("portfolio state is not available")
        return PortfolioState.from_dict(json.loads(row[0]))

    def put_target(self, target: TargetWeights) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO portfolio_targets VALUES (?, ?, ?)",
                (target.target_id, target.as_of.isoformat(), json.dumps(target.to_dict())),
            )

    def latest_target(self) -> TargetWeights:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM portfolio_targets ORDER BY as_of DESC LIMIT 1"
            ).fetchone()
        if row is None:
            raise KeyError("portfolio target is not available")
        return TargetWeights.from_dict(json.loads(row[0]))

    def put_intents(self, intents: tuple[OrderIntent, ...]) -> None:
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO order_intents VALUES (?, ?, ?, ?)",
                [
                    (item.intent_id, item.as_of.isoformat(), item.target_id, json.dumps(item.to_dict()))
                    for item in intents
                ],
            )

    def latest_intents(self) -> list[dict]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT target_id FROM portfolio_targets ORDER BY as_of DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return []
            records = connection.execute(
                "SELECT payload FROM order_intents WHERE target_id = ? ORDER BY payload", (row[0],)
            ).fetchall()
        return [json.loads(item[0]) for item in records]

"""SQLite metadata plus JSON body repository for EvidenceBundle records."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable

from contracts.evidence import EvidenceBundle
from contracts.symbol import CanonicalSymbol

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class RepositoryError(RuntimeError):
    """Raised when persisted evidence is missing, corrupt, or inconsistent."""


class EvidenceRepository:
    def __init__(
        self,
        database_path: str | Path = PROJECT_ROOT / "storage" / "state" / "competition.sqlite",
        body_directory: str | Path = PROJECT_ROOT / "storage" / "artifacts" / "evidence",
    ) -> None:
        self.database_path = Path(database_path)
        self.body_directory = Path(body_directory)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.body_directory.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    evidence_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL REFERENCES symbols(symbol),
                    as_of TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    body_path TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_evidence_symbol_as_of
                    ON evidence(symbol, as_of DESC, evidence_id DESC);
                """
            )

    def register_symbol(self, symbol: CanonicalSymbol) -> None:
        payload = json.dumps(symbol.to_dict(), sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT payload_json FROM symbols WHERE symbol = ?", (symbol.symbol,)
            ).fetchone()
            if existing is not None and existing["payload_json"] != payload:
                raise RepositoryError(f"canonical symbol conflict: {symbol.symbol}")
            connection.execute(
                "INSERT OR IGNORE INTO symbols(symbol, payload_json) VALUES (?, ?)",
                (symbol.symbol, payload),
            )

    def get_symbol(self, symbol: str) -> CanonicalSymbol:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM symbols WHERE symbol = ?", (symbol,)
            ).fetchone()
        if row is None:
            raise KeyError(symbol)
        return CanonicalSymbol.from_dict(json.loads(row["payload_json"]))

    def put(self, bundle: EvidenceBundle) -> str:
        self.register_symbol(bundle.symbol)
        evidence_id = bundle.evidence_id
        target = self.body_directory / f"{evidence_id}.json"
        payload = bundle.to_json() + "\n"
        if target.exists():
            if target.read_text(encoding="utf-8") != payload:
                raise RepositoryError(f"evidence body conflict: {evidence_id}")
        else:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=self.body_directory, delete=False
            ) as handle:
                handle.write(payload)
                temporary = Path(handle.name)
            temporary.replace(target)

        relative_body = target.relative_to(self.database_path.parent.parent).as_posix()
        metadata = (
            bundle.symbol.symbol,
            bundle.as_of.isoformat(),
            bundle.snapshot_id,
            bundle.schema_version,
            relative_body,
        )
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT symbol, as_of, snapshot_id, schema_version, body_path "
                "FROM evidence WHERE evidence_id = ?",
                (evidence_id,),
            ).fetchone()
            if existing is not None and tuple(existing) != metadata:
                raise RepositoryError(f"evidence metadata conflict: {evidence_id}")
            connection.execute(
                "INSERT OR IGNORE INTO evidence "
                "(evidence_id, symbol, as_of, snapshot_id, schema_version, body_path) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (evidence_id, *metadata),
            )
        return evidence_id

    def get(self, evidence_id: str) -> EvidenceBundle:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT body_path FROM evidence WHERE evidence_id = ?", (evidence_id,)
            ).fetchone()
        if row is None:
            raise KeyError(evidence_id)
        path = self.database_path.parent.parent / row["body_path"]
        try:
            bundle = EvidenceBundle.from_dict(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RepositoryError(f"cannot read evidence body: {evidence_id}") from exc
        if bundle.evidence_id != evidence_id:
            raise RepositoryError(f"evidence body hash mismatch: {evidence_id}")
        return bundle

    def latest(self, symbol: str) -> EvidenceBundle:
        canonical = CanonicalSymbol.from_core(symbol)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT evidence_id FROM evidence WHERE symbol = ? "
                "ORDER BY as_of DESC, evidence_id DESC LIMIT 1",
                (canonical.symbol,),
            ).fetchone()
        if row is None:
            raise KeyError(symbol)
        return self.get(row["evidence_id"])

    def latest_many(self, symbols: Iterable[str]) -> dict[str, EvidenceBundle]:
        result: dict[str, EvidenceBundle] = {}
        for symbol in symbols:
            canonical = CanonicalSymbol.from_core(symbol).symbol
            try:
                result[canonical] = self.latest(canonical)
            except KeyError:
                continue
        return result

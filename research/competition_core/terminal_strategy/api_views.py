"""Bounded read-only projections for persisted terminal evaluations."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
import re
from typing import Any

from competition_core.pipeline.artifacts import confined_path, content_hash
from competition_core.pipeline.errors import PipelineError

from .persistence import TerminalEvaluationPersistence, TerminalPersistenceError
from .service import TerminalEvaluationResult


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
_EVALUATION_ID = re.compile(r"^[0-9a-f]{64}$")


def decision_projection(result: TerminalEvaluationResult) -> dict[str, Any]:
    """Return the bounded response used after a runtime evaluation."""

    return _decision_payload(
        result.run_id,
        result.evaluation_id,
        result.status,
        result.snapshot.to_dict(),
        result.forecast.to_dict(),
        result.mode_decision.to_dict(),
        result.funnel.to_dict(),
        None if result.allocation_policy is None else result.allocation_policy.to_dict(),
        result.blockers,
        result.reasons,
        result.source_hashes.account_snapshot_hash,
        result.source_hashes.leaderboard_snapshot_hash,
    )


def persisted_decision_projection(loaded: Mapping[str, Any]) -> dict[str, Any]:
    """Project a fully hash-verified persistence load without rehydrating it."""

    artifacts = loaded["artifacts"]
    manifest = loaded["manifest"]
    source = loaded["source_hashes"]
    return _decision_payload(
        manifest["run_id"],
        manifest["evaluation_id"],
        manifest["status"],
        artifacts["terminal-snapshot.json"],
        artifacts["award-line-forecast.json"],
        artifacts["mode-decision.json"],
        {
            "watchlist": artifacts["candidates-watch.json"].get("candidates", []),
            "focus": artifacts["candidates-focus.json"].get("candidates", []),
            "terminal": artifacts["candidates-terminal.json"].get("candidates", []),
            "status": artifacts["candidates-terminal.json"].get("status", "PERSISTED"),
            "blockers": manifest.get("blockers", []),
        },
        artifacts["allocation-policy.json"],
        manifest.get("blockers", []),
        manifest.get("reasons", []),
        source["account_snapshot_hash"],
        source["leaderboard_snapshot_hash"],
    )


def persisted_candidates_projection(loaded: Mapping[str, Any], tier: str) -> dict[str, Any]:
    if tier not in {"watch", "focus", "terminal"}:
        raise PipelineError("INVALID_ARGUMENTS", "terminal candidate tier is invalid")
    payload = loaded["artifacts"][f"candidates-{tier}.json"]
    return {
        "schema_version": "competition.terminal-candidates-view.v1",
        "run_id": payload.get("run_id"),
        "evaluation_id": payload.get("evaluation_id"),
        "tier": tier,
        "mode": payload.get("mode"),
        "as_of": payload.get("as_of"),
        "trade_date": payload.get("trade_date"),
        "candidates": payload.get("candidates", []),
        "funnel_hash": payload.get("funnel_hash"),
        "input_hash": payload.get("input_hash"),
        "execution_ready": False,
        "orders_submitted": 0,
    }


def persisted_history(root: str | Path, run_id: str, limit: int) -> list[dict[str, Any]]:
    loaded = _load_all(root, run_id)
    rows: list[dict[str, Any]] = []
    for item in loaded:
        manifest = item["manifest"]
        snapshot = item["artifacts"]["terminal-snapshot.json"]
        mode = item["artifacts"]["mode-decision.json"]
        rows.append({
            "evaluation_id": manifest["evaluation_id"],
            "status": manifest["status"],
            "as_of": snapshot.get("as_of"),
            "trade_date": snapshot.get("trade_date"),
            "mode": mode.get("mode"),
            "blockers": list(manifest.get("blockers", [])),
            "execution_ready": False,
            "orders_submitted": 0,
        })
    rows.sort(key=lambda row: (_as_of_key(row.get("as_of")), str(row["evaluation_id"])), reverse=True)
    return rows[:limit]


def latest_persisted(root: str | Path, run_id: str) -> Mapping[str, Any]:
    loaded = _load_all(root, run_id)
    if not loaded:
        raise PipelineError("NOT_FOUND", "terminal evaluation does not exist")
    loaded.sort(
        key=lambda item: (
            _as_of_key(item["artifacts"]["terminal-snapshot.json"].get("as_of")),
            str(item["manifest"]["evaluation_id"]),
        ),
        reverse=True,
    )
    return loaded[0]


def load_persisted(root: str | Path, run_id: str, evaluation_id: str | None = None) -> Mapping[str, Any]:
    if evaluation_id is None:
        return latest_persisted(root, run_id)
    if not isinstance(evaluation_id, str) or not _EVALUATION_ID.fullmatch(evaluation_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid terminal evaluation identifier")
    store = TerminalEvaluationPersistence(root)
    try:
        return store.load(run_id, evaluation_id)
    except TerminalPersistenceError as exc:
        message = str(exc)
        if "missing" in message or "unavailable" in message:
            raise PipelineError("NOT_FOUND", "terminal evaluation does not exist") from exc
        raise PipelineError("STALE_DATA", "terminal evaluation integrity check failed") from exc


def status_projection(root: str | Path, run_ids: Iterable[str] | None = None) -> dict[str, Any]:
    """Return recent status rows, skipping absent/unsafe run directories."""

    base = _runs_root(root)
    names = sorted(set(run_ids)) if run_ids is not None else _run_names(base)
    rows: list[dict[str, Any]] = []
    for run_id in names:
        if not isinstance(run_id, str) or not _IDENTIFIER.fullmatch(run_id):
            continue
        try:
            loaded = latest_persisted(base, run_id)
        except (PipelineError, OSError, ValueError):
            continue
        manifest = loaded["manifest"]
        snapshot = loaded["artifacts"]["terminal-snapshot.json"]
        mode = loaded["artifacts"]["mode-decision.json"]
        rows.append({
            "run_id": run_id,
            "evaluation_id": manifest["evaluation_id"],
            "status": manifest["status"],
            "as_of": snapshot.get("as_of"),
            "trade_date": snapshot.get("trade_date"),
            "mode": mode.get("mode"),
            "execution_ready": False,
            "orders_submitted": 0,
        })
    rows.sort(key=lambda row: (_as_of_key(row.get("as_of")), str(row["evaluation_id"])), reverse=True)
    return {
        "schema_version": "competition.terminal-status-view.v1",
        "runs": rows,
        "execution_ready": False,
        "orders_submitted": 0,
    }


def _load_all(root: str | Path, run_id: str) -> list[Mapping[str, Any]]:
    if not isinstance(run_id, str) or not _IDENTIFIER.fullmatch(run_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid research task identifier")
    run_directory = _runs_root(root) / run_id
    if run_directory.is_symlink() or not run_directory.is_dir():
        raise PipelineError("NOT_FOUND", "research run does not exist")
    terminal_directory = confined_path(run_directory, "terminal")
    if not terminal_directory.is_dir() or terminal_directory.is_symlink():
        return []
    store = TerminalEvaluationPersistence(root)
    loaded: list[Mapping[str, Any]] = []
    for directory in terminal_directory.iterdir():
        if directory.is_symlink():
            raise PipelineError("STALE_DATA", "terminal evaluation path is a symlink")
        if not directory.is_dir() or not _EVALUATION_ID.fullmatch(directory.name):
            raise PipelineError("STALE_DATA", "terminal evaluation directory is invalid")
        try:
            loaded.append(store.load(run_id, directory.name))
        except TerminalPersistenceError as exc:
            raise PipelineError("STALE_DATA", "terminal evaluation integrity check failed") from exc
    return loaded


def _run_names(root: Path) -> list[str]:
    if not root.is_dir() or root.is_symlink():
        return []
    return [item.name for item in root.iterdir() if item.is_dir() and not item.is_symlink()]


def _runs_root(root: str | Path) -> Path:
    supplied = Path(root).resolve()
    return supplied if supplied.name == "runs" else supplied / "runs"


def _as_of_key(value: Any) -> float:
    """Use instant ordering for ISO timestamps while failing closed on bad data."""

    if not isinstance(value, str):
        return float("-inf")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            return float("-inf")
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return float("-inf")


def _decision_payload(
    run_id: str,
    evaluation_id: str,
    status: str,
    snapshot: Mapping[str, Any],
    forecast: Mapping[str, Any],
    mode: Mapping[str, Any],
    funnel: Mapping[str, Any],
    allocation: Mapping[str, Any] | None,
    blockers: Iterable[str],
    reasons: Iterable[str],
    account_hash: str | None,
    leaderboard_hash: str | None,
) -> dict[str, Any]:
    candidates = {
        "watch": len(funnel.get("watchlist", funnel.get("watch", [])) or []),
        "focus": len(funnel.get("focus", []) or []),
        "terminal": len(funnel.get("terminal", []) or []),
    }
    allocation_view = None
    if allocation is not None and allocation.get("available", True) is not False:
        policy = allocation.get("policy", allocation)
        allocation_view = {
            "mode": policy.get("mode"),
            "core_count": len(policy.get("core_symbols", []) or []),
            "aggressive_count": len(policy.get("aggressive_symbols", []) or []),
            "total_position_count": policy.get("total_position_count"),
            "core_budget": policy.get("core_budget"),
            "aggressive_budget": policy.get("aggressive_budget"),
            "cash_target": policy.get("cash_target"),
            "execution_ready": False,
        }
    return {
        "schema_version": "competition.terminal-decision-view.v1",
        "run_id": run_id,
        "evaluation_id": evaluation_id,
        "status": status,
        "mode": mode.get("mode"),
        "mode_reasons": list(mode.get("reasons", [])),
        "blockers": list(blockers),
        "reasons": list(reasons),
        "forecast": {
            "available": forecast.get("available"),
            "target_asset": forecast.get("target_asset"),
            "required_return": forecast.get("required_return"),
            "confidence": forecast.get("confidence"),
            "blockers": list(forecast.get("blockers", [])),
        },
        "funnel": {"status": funnel.get("status"), "counts": candidates},
        "allocation": allocation_view,
        "snapshot": {
            "as_of": snapshot.get("as_of"),
            "trade_date": snapshot.get("trade_date"),
            "snapshot_hash": content_hash(dict(snapshot)),
            "account_snapshot_hash": account_hash or snapshot.get("account_snapshot_hash"),
            "leaderboard_snapshot_hash": leaderboard_hash or snapshot.get("leaderboard_snapshot_hash"),
        },
        "execution_ready": False,
        "orders_submitted": 0,
        "production_accepted": False,
    }


__all__ = [
    "decision_projection",
    "latest_persisted",
    "load_persisted",
    "persisted_candidates_projection",
    "persisted_decision_projection",
    "persisted_history",
    "status_projection",
]

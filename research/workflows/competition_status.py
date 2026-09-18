"""Read-only account/leaderboard checkpoint with cross-source reconciliation."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from adapters.ninef.leaderboard import NinefLeaderboard
from competition_core.daily_workflow import DailyWorkflowConfig, load_daily_workflow_config
from competition_core.pipeline.account import AccountSnapshot
from competition_core.pipeline.time_context import SHANGHAI
from workflows.artifacts import content_hash, write_json


def build_competition_status(
    *,
    checkpoint: str,
    account: AccountSnapshot,
    account_receipt: dict[str, Any],
    leaderboard: NinefLeaderboard,
    observed_at: datetime,
    config: DailyWorkflowConfig | None = None,
) -> dict[str, Any]:
    policy = config or load_daily_workflow_config()
    if checkpoint not in policy.leaderboard_checkpoints:
        raise ValueError("unsupported leaderboard checkpoint")
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise ValueError("status observation time must be timezone-aware")
    now = observed_at.astimezone(SHANGHAI)
    if account.as_of > now or leaderboard.as_of > now:
        raise ValueError("account or leaderboard timestamp is in the future")
    max_age = timedelta(seconds=policy.max_quote_age_seconds * 15)
    account_age = now - account.as_of
    leaderboard_age = now - leaderboard.as_of
    orders = account_receipt.get("orders")
    if not isinstance(orders, dict) or orders.get("complete") is not True:
        raise ValueError("authoritative account order history is incomplete")
    if type(orders.get("open_count")) is not int or orders["open_count"] < 0:
        raise ValueError("authoritative open-order count is invalid")
    if account.pending_order_count != orders["open_count"]:
        raise ValueError("account and order stream disagree on open orders")

    own = leaderboard.own
    asset_difference: Decimal | None = None
    if own is not None and own.total_assets is not None:
        asset_difference = account.current_total_asset - own.total_assets
    crosscheck = "UNAVAILABLE"
    if asset_difference is not None:
        tolerance = max(Decimal("1.00"), account.current_total_asset * Decimal("0.001"))
        crosscheck = "MATCH" if abs(asset_difference) <= tolerance else "MISMATCH"

    account_payload = account.to_dict()
    leaderboard_payload = leaderboard.to_dict()
    return {
        "schema_version": "competition.daily-status.v1",
        "checkpoint": checkpoint,
        "observed_at": now.isoformat(),
        "read_only": True,
        "orders_submitted": 0,
        "account": account_payload,
        "leaderboard": leaderboard_payload,
        "summary": {
            "own_rank": None if own is None else own.rank,
            "total_participants": leaderboard.total_participants,
            "account_total_assets": str(account.current_total_asset),
            "leaderboard_total_assets": None if own is None or own.total_assets is None else str(own.total_assets),
            "asset_difference": None if asset_difference is None else str(asset_difference),
            "asset_crosscheck": crosscheck,
            "available_cash": str(account.available_cash),
            "position_count": len(account.positions),
            "open_order_count": orders["open_count"],
            "account_stale": account_age > max_age,
            "leaderboard_stale": leaderboard_age > max_age,
        },
        "source_hashes": {
            "account_snapshot": account_receipt.get("account_snapshot_hash") or content_hash(account_payload),
            "leaderboard_snapshot": content_hash(leaderboard_payload),
        },
    }


def write_competition_status(
    *,
    run_id: str,
    checkpoint: str,
    account: AccountSnapshot,
    account_receipt: dict[str, Any],
    leaderboard: NinefLeaderboard,
    artifact_root: str | Path,
    observed_at: datetime | None = None,
    config: DailyWorkflowConfig | None = None,
) -> dict[str, Any]:
    if not isinstance(run_id, str) or not run_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in run_id):
        raise ValueError("invalid daily status run id")
    root = Path(artifact_root).resolve()
    run_dir = root / run_id
    if run_dir.resolve().parent != root or run_dir.exists():
        raise FileExistsError(f"workflow artifact already exists or is invalid: {run_dir}")
    run_dir.mkdir(parents=True)
    payload = build_competition_status(
        checkpoint=checkpoint,
        account=account,
        account_receipt=account_receipt,
        leaderboard=leaderboard,
        observed_at=observed_at or datetime.now(SHANGHAI),
        config=config,
    )
    write_json(run_dir, "account.json", payload["account"])
    write_json(run_dir, "leaderboard.json", payload["leaderboard"])
    write_json(run_dir, "competition-status.json", payload)
    write_json(run_dir, "run_manifest.json", {
        "run_id": run_id,
        "phase": checkpoint,
        "status": "complete",
        "read_only": True,
        "orders_submitted": 0,
        "observed_at": payload["observed_at"],
        "content_hash": content_hash(payload),
    })
    return payload


__all__ = ["build_competition_status", "write_competition_status"]

#!/usr/bin/env python3
"""Capture one read-only Ninef account and competition-ranking checkpoint."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from adapters.ninef.account import fetch_ninef_account, resolve_ninef_token  # noqa: E402
from adapters.ninef.leaderboard import fetch_ninef_leaderboard  # noqa: E402
from competition_core.daily_workflow import load_daily_workflow_config  # noqa: E402
from competition_core.pipeline.time_context import SHANGHAI  # noqa: E402
from workflows.competition_status import write_competition_status  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", choices=("preopen", "midday", "tail", "postclose"), required=True)
    parser.add_argument("--artifact-root", type=Path, default=ROOT / "runs/daily-status")
    parser.add_argument("--run-id")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--competition-id")
    args = parser.parse_args()

    now = datetime.now(SHANGHAI)
    run_id = args.run_id or f"{now:%Y%m%d}-{args.checkpoint}-{now:%H%M%S}"
    token = resolve_ninef_token(token_file=args.token_file)
    try:
        account = fetch_ninef_account(token)
        leaderboard = fetch_ninef_leaderboard(token, competition_id=args.competition_id)
    finally:
        token = ""
    payload = write_competition_status(
        run_id=run_id,
        checkpoint=args.checkpoint,
        account=account.snapshot,
        account_receipt=account.receipt,
        leaderboard=leaderboard,
        artifact_root=args.artifact_root,
        observed_at=now,
        config=load_daily_workflow_config(),
    )
    print(json.dumps({
        "run_id": run_id,
        "checkpoint": args.checkpoint,
        "observed_at": payload["observed_at"],
        "summary": payload["summary"],
        "orders_submitted": 0,
        "read_only": True,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

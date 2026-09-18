"""Build and persist one deterministic Competition Universe JSON snapshot."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.tushare.client import create_tushare_client
from competition_core.config import load_config
from competition_core.universe import CompetitionUniverse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="Competition trading date in YYYY-MM-DD format")
    parser.add_argument("--phase", choices=("preopen", "postclose"), default=None)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "storage" / "artifacts" / "universe",
    )
    args = parser.parse_args()

    config = load_config()
    snapshot = CompetitionUniverse(create_tushare_client(), config).build(
        args.date, args.phase
    )
    target = snapshot.write_json(args.output)
    print(
        f"{target} snapshot_id={snapshot.snapshot_id} "
        f"eligible={len(snapshot.eligible_members)}"
    )


if __name__ == "__main__":
    main()

"""Detached worker for the local Web task controller; no shell or trading tools."""

import argparse
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from competition_core.research_worker import execute_task


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{32}", args.attempt_id):
        raise SystemExit(2)
    try:
        execute_task(runs_root=args.runs_root, run_id=args.run_id, attempt_id=args.attempt_id)
    except Exception:
        # Worker writes only sanitized failures; never emit provider/model bodies.
        raise SystemExit(1)


if __name__ == "__main__":
    main()

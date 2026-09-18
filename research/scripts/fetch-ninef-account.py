"""Fetch one read-only Ninef account snapshot for explicit S6 input."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from adapters.ninef.account import fetch_ninef_account, resolve_ninef_token


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--receipt-out", type=Path)
    parser.add_argument("--fee-out", type=Path)
    parser.add_argument("--token-file", type=Path)
    args = parser.parse_args()
    try:
        token = resolve_ninef_token(token_file=args.token_file)
        result = fetch_ninef_account(token)
        _atomic_json(args.out, result.snapshot.to_dict())
        if args.fee_out:
            if result.fee_schedule is None:
                raise RuntimeError("current-day Ninef fee evidence is unavailable")
            _atomic_json(args.fee_out, result.fee_schedule.to_dict())
        if args.receipt_out:
            _atomic_json(args.receipt_out, result.receipt)
        print(json.dumps({
            "ok": True,
            "account_file": str(args.out),
            "receipt_file": str(args.receipt_out) if args.receipt_out else None,
            "fee_file": str(args.fee_out) if args.fee_out else None,
            "as_of": result.snapshot.as_of.isoformat(),
            "position_count": len(result.snapshot.positions),
            "pending_order_count": result.snapshot.pending_order_count,
            "account_snapshot_hash": result.snapshot.snapshot_hash,
        }, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

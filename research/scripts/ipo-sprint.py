"""Collect or evaluate the independent, read-only IPO sprint route."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from competition_core.ipo_sprint import (  # noqa: E402
    IpoSprintCollectionError,
    IpoSprintError,
    collect_ipo_sprint_snapshot,
    evaluate_ipo_sprint_snapshot,
    load_ipo_sprint_config,
)
from adapters.tushare.client import (  # noqa: E402
    TushareConfigurationError,
    configured_tushare_client,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    evaluate = commands.add_parser("evaluate", help="evaluate a collected standalone snapshot")
    evaluate.add_argument("--input", required=True, type=Path)
    evaluate.add_argument("--output", required=True, type=Path)
    evaluate.add_argument("--config", type=Path, default=ROOT / "config" / "ipo-sprint.yaml")
    collect = commands.add_parser("collect", help="collect a standalone Tushare + Sina snapshot")
    collect.add_argument("--snapshot-output", required=True, type=Path)
    collect.add_argument("--token-file", type=Path)
    collect.add_argument("--base-url")
    collect.add_argument("--start-date", help="YYYY-MM-DD; defaults to 30 calendar days before observation")
    collect.add_argument("--observed-at", help="aware ISO-8601 time; defaults to current local time")
    return parser


def _write_json(path: Path, value: object) -> None:
    target = path.expanduser().resolve(strict=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def _evaluate(args: argparse.Namespace) -> None:
    input_path = args.input.expanduser().resolve(strict=True)
    output_path = args.output.expanduser().resolve(strict=False)
    config_path = args.config.expanduser().resolve(strict=False)
    if output_path in {input_path, config_path}:
        raise IpoSprintError("output path must differ from input and config")
    snapshot = json.loads(input_path.read_text(encoding="utf-8"))
    result = evaluate_ipo_sprint_snapshot(snapshot, config=load_ipo_sprint_config(config_path))
    _write_json(output_path, result)


def _collect(args: argparse.Namespace) -> None:
    output_path = args.snapshot_output.expanduser().resolve(strict=False)
    if args.token_file is not None:
        token_path = args.token_file.expanduser().resolve(strict=False)
        if output_path == token_path:
            raise IpoSprintCollectionError("snapshot output must differ from token input")
    observed = (
        datetime.fromisoformat(args.observed_at.replace("Z", "+00:00"))
        if args.observed_at else datetime.now(ZoneInfo("Asia/Shanghai"))
    )
    start = date.fromisoformat(args.start_date) if args.start_date else None
    client = configured_tushare_client(token_file=args.token_file, base_url=args.base_url)
    snapshot = collect_ipo_sprint_snapshot(
        client, observed_at=observed, start_date=start,
    )
    _write_json(output_path, snapshot)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    # Preserve the first evaluator's explicit --input/--output command shape.
    if raw and raw[0] not in {"collect", "evaluate", "-h", "--help"}:
        raw.insert(0, "evaluate")
    args = build_parser().parse_args(raw)
    try:
        _collect(args) if args.command == "collect" else _evaluate(args)
    except (
        IpoSprintCollectionError,
        IpoSprintError,
        TushareConfigurationError,
        OSError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ):
        sys.stderr.write("ipo-sprint operation failed; source evidence was not accepted\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Immutable last-valid Core responses for explicit stale degradation."""

from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

DEFAULT_ROOT = Path(__file__).resolve().parents[2] / "storage" / "artifacts" / "core-cache"


class SnapshotCache:
    def __init__(self, root: str | Path = DEFAULT_ROOT) -> None:
        self.root = Path(root)

    def put(self, kind: str, snapshot_date: date, phase: str, payload: dict[str, Any], variant: str = "default") -> Path:
        directory = self.root / kind / phase / variant
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{snapshot_date.isoformat()}.json"
        body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False) as handle:
            handle.write(body)
            temporary = Path(handle.name)
        temporary.replace(target)
        return target

    def latest(self, kind: str, requested_date: date, phase: str, variant: str = "default") -> dict[str, Any]:
        directory = self.root / kind / phase / variant
        if not directory.is_dir():
            raise KeyError("no cached response")
        eligible: list[tuple[date, Path]] = []
        for path in directory.glob("????-??-??.json"):
            try:
                item_date = date.fromisoformat(path.stem)
            except ValueError:
                continue
            if item_date <= requested_date:
                eligible.append((item_date, path))
        if not eligible:
            raise KeyError("no cached response on or before requested date")
        cached_date, path = max(eligible, key=lambda item: item[0])
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("cached response is invalid")
        payload["cached_date"] = cached_date.isoformat()
        return payload

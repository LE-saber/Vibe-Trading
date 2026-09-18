"""Small reproducible artifact writer shared by deterministic workflows."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any


def write_json(directory: Path, name: str, payload: Any) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / name
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False) as handle:
        handle.write(body)
        temporary = Path(handle.name)
    temporary.replace(target)
    return target


def content_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

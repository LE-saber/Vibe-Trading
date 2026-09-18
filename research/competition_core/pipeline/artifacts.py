"""Atomic, hash-checked research artifacts; no mutable content under a frozen ID."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

from competition_core.pipeline.errors import PipelineError


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(json_bytes(value)).hexdigest()


def file_hash(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replace_mutable(temporary: Path, target: Path) -> None:
    """Tolerate brief Windows sharing conflicts from concurrent readers."""
    for attempt in range(6):
        try:
            os.replace(temporary, target)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.01 * (2**attempt))


def atomic_bytes(path: str | Path, payload: bytes, *, immutable: bool = False) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".partial-", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if immutable:
            try:
                # Same-volume link is atomic and cannot overwrite an existing
                # content-addressed artifact, even with concurrent workers.
                os.link(temporary, target)
            except FileExistsError:
                if target.read_bytes() != payload:
                    raise PipelineError("STALE_DATA", "immutable artifact content conflict", path=str(target))
        else:
            _replace_mutable(temporary, target)
        return target
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def atomic_json(path: str | Path, value: Any, *, immutable: bool = False) -> Path:
    return atomic_bytes(path, json_bytes(value), immutable=immutable)


def read_json(path: str | Path, *, expected_hash: str | None = None) -> Any:
    path = Path(path)
    body = path.read_bytes()
    if expected_hash is not None and hashlib.sha256(body).hexdigest() != expected_hash:
        raise PipelineError("STALE_DATA", "artifact hash mismatch", path=str(path))
    return json.loads(body)


def confined_path(root: str | Path, relative: str) -> Path:
    root = Path(root).resolve()
    target = (root / relative).resolve()
    if target == root or not target.is_relative_to(root):
        raise PipelineError("INVALID_ARGUMENTS", "artifact path escapes the run directory")
    return target

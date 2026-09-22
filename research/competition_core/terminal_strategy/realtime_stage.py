"""Hash-bound terminal projection of an uncommitted realtime observation."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.artifacts import confined_path, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.realtime import _inputs


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")


def load_terminal_realtime_stage(repository: Any, check_id: str) -> dict[str, Any]:
    """Project one verified realtime observation without committing S5.5.

    The returned envelope is terminal evidence only.  Its artifact hash binds
    the immutable request, response and observation files, but it is never
    written as a pipeline checkpoint.
    """

    if not isinstance(check_id, str) or _IDENTIFIER.fullmatch(check_id) is None:
        raise PipelineError("INVALID_ARGUMENTS", "invalid terminal realtime observation identifier")

    root = Path(repository.directory).resolve()
    relative = f"realtime/{check_id}"
    raw_realtime = Path(repository.directory) / "realtime"
    raw_directory = raw_realtime / check_id
    raw_files = tuple(raw_directory / name for name in ("request.json", "response.json", "observation.json"))
    if raw_realtime.is_symlink() or raw_directory.is_symlink() or any(path.is_symlink() for path in raw_files):
        raise PipelineError("STALE_DATA", "terminal realtime observation cannot use symlinks")
    directory = confined_path(root, relative)
    if not directory.is_dir() or any(not path.is_file() for path in raw_files):
        raise PipelineError("NOT_READY", "terminal realtime observation is unavailable")

    inputs, config, context, stages = _inputs(repository)
    observation_path = directory / "observation.json"
    observation = read_json(observation_path)
    integrity_hash = observation.pop("integrity_hash", None) if isinstance(observation, dict) else None
    if not isinstance(observation, dict) or integrity_hash != content_hash(observation):
        raise PipelineError("STALE_DATA", "terminal realtime observation integrity mismatch")

    request_hash = observation.get("request_hash")
    response_hash = observation.get("response_hash")
    if not all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
               for value in (request_hash, response_hash)):
        raise PipelineError("STALE_DATA", "terminal realtime observation file hashes are invalid")
    request = read_json(directory / "request.json", expected_hash=request_hash)
    read_json(directory / "response.json", expected_hash=response_hash)

    stage_hashes = {name: stage["artifact_hash"] for name, stage in stages.items()}
    expected_request = {
        "context": context.to_dict(),
        "snapshot_id": inputs["snapshot_id"],
        "config_hash": config.config_hash,
        "stage_hashes": stage_hashes,
    }
    if (
        observation.get("run_id") != repository.run_id
        or observation.get("check_id") != check_id
        or not isinstance(request, dict)
        or request.get("check_id") != check_id
        or any(request.get(key) != value for key, value in expected_request.items())
    ):
        raise PipelineError("STALE_DATA", "terminal realtime observation differs from frozen research")

    shortlist = list(stages["S4"]["output_symbols"])
    requested = request.get("symbols")
    if (
        not isinstance(requested, list)
        or requested[:len(shortlist)] != shortlist
        or len(requested) != len(set(requested))
    ):
        raise PipelineError("IDENTITY_CONFLICT", "terminal realtime observation identities are invalid")
    try:
        for symbol in requested:
            CanonicalSymbol.from_core(symbol)
    except (SymbolContractError, TypeError, ValueError) as exc:
        raise PipelineError("IDENTITY_CONFLICT", "terminal realtime observation symbol is invalid") from exc

    result = observation.get("result")
    if (
        not isinstance(result, dict)
        or result.get("stage") != "S5_5"
        or result.get("status") not in {"SUCCESS", "PARTIAL"}
    ):
        raise PipelineError("STALE_DATA", "terminal realtime result is invalid")
    rows = result.get("rows")
    outputs = result.get("output_symbols")
    if not isinstance(rows, list) or not isinstance(outputs, list) or result.get("input_symbols") != requested:
        raise PipelineError("STALE_DATA", "terminal realtime result identities are invalid")
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") not in requested or row["symbol"] in by_symbol:
            raise PipelineError("IDENTITY_CONFLICT", "terminal realtime result rows are invalid")
        by_symbol[row["symbol"]] = row
    if (
        set(by_symbol) != set(requested)
        or len(outputs) != len(set(outputs))
        or not set(outputs) <= set(requested)
    ):
        raise PipelineError("IDENTITY_CONFLICT", "terminal realtime result coverage is invalid")

    binding = {
        "schema_version": "competition.terminal-realtime-binding.v1",
        "check_id": check_id,
        "request_hash": request_hash,
        "response_hash": response_hash,
        "observation_hash": file_hash(observation_path),
        "result_hash": content_hash(result),
    }
    shortlist_set = set(shortlist)
    body = {
        "stage": "S5_5",
        "status": result["status"],
        "input_symbols": shortlist,
        "output_symbols": [symbol for symbol in outputs if symbol in shortlist_set],
        "rows": [by_symbol[symbol] for symbol in shortlist],
        "snapshot_id": inputs["snapshot_id"],
        "config_hash": config.config_hash,
        **context.to_dict(),
        "terminal_observation_binding": binding,
    }
    return body | {"artifact_hash": content_hash(body)}


__all__ = ["load_terminal_realtime_stage"]

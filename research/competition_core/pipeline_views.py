"""Read-only, bounded projections of frozen pipeline runs.

This module deliberately does not construct ``PipelineRepository`` through its
public constructor: that constructor creates a run directory.  A web GET must
never create a run, so an already-existing, confined directory is attached to a
repository instance only after it has been validated.
"""

from __future__ import annotations

from datetime import datetime
import math
from pathlib import Path
import re
from typing import Any, Mapping

from competition_core.pipeline.artifacts import content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository, STAGES
from contracts.evidence import EvidenceBundle, EvidenceContractError
from contracts.symbol import CanonicalSymbol, SymbolContractError


_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}\Z")
_SHA256 = re.compile(r"[a-f0-9]{64}\Z")
_SOURCE_LABEL = re.compile(r"[A-Za-z0-9_.:-]{1,160}\Z")

_CONTEXT_FIELDS = (
    "research_date",
    "phase",
    "market_data_reference_date",
    "as_of",
    "timezone",
)
_PHASES = {"preopen", "intraday", "postclose"}
_RUN_STATUSES = {"PENDING", "RUNNING", "PARTIAL", "SUCCESS", "FAILED"}

# These are the only non-stage files that may appear in the public artifact
# index.  The index returns names and hashes only; it is not a file-read API.
_PUBLIC_REPORTS = (
    "ai-review-audit.json",
    "ai-review-audit.md",
    "ai-semantic-review.md",
    "diversification-audit.json",
    "evidence-quality-audit.ipynb",
    "evidence-quality-audit.json",
    "s3-resume-audit.json",
    "acceptance-report.json",
    "acceptance-report.md",
)

_PRIVATE_KEYS = {
    "account",
    "account_json",
    "api_key",
    "authorization",
    "config",
    "configuration",
    "credential",
    "credentials",
    "directory",
    "error",
    "exception",
    "exception_type",
    "failure",
    "file",
    "file_path",
    "manifest",
    "manifest_path",
    "password",
    "path",
    "provenance",
    "raw_request",
    "raw_response",
    "request",
    "request_id",
    "response",
    "secret",
    "source_provenance",
    "token",
    "traceback",
    "transport",
    "vibe_root",
}

_IDENTITY_FIELDS = (
    "ts_code",
    "name",
    "exchange",
    "board",
    "market",
    "currency",
    "lot_size",
    "is_eligible",
    "is_st",
    "is_suspended",
    "is_tradeable",
    "is_delisting_risk",
    "list_date",
    "list_status",
    "delist_date",
    "liquidity_date",
    "new_listing_special_sessions",
    "trading_rule_key",
    "exclusion_reasons",
)
_SECTOR_FIELDS = ("status", "value", "classification", "as_of")
_BOARD_FIELDS = ("board", "exchange")
_STYLE_FIELDS = ("overheat_score", "overheat_threshold")

_SCORE_FIELDS = (
    "score",
    "quant_score",
    "s1_quant_score",
    "selection_score",
    "overheat_score",
    "trend_score",
    "momentum_score",
    "relative_strength_score",
    "liquidity_score",
    "volatility_quality_score",
    "data_quality_score",
    "rank",
    "s1_rank",
    "s2_rank",
    "s3_rank",
)
_SCORE_MAPPING_FIELDS = (
    "scores",
    "component_scores",
    "feature_percentiles",
    "effective_weights",
    "overheat_percentiles",
    "s3_components",
    "s3_effective_weights",
)
_REASON_FIELDS = (
    "keep_reasons",
    "reject_reasons",
    "reasons",
    "risk_flags",
    "missing_fields",
    "evidence_missing_fields",
    "ai_risks",
)
_ROW_FIELDS = (
    "symbol",
    "name",
    "selected",
    *_SCORE_FIELDS,
    *_SCORE_MAPPING_FIELDS,
    *_REASON_FIELDS,
    "identity",
    "evidence_id",
    "evidence_confidence",
    "evidence_coverage",
    "main_flow_ratio_5d",
    "correlation_cluster",
    "correlation_to_selected",
    "sector_metadata",
    "style_metadata",
    "board_metadata",
    "overheated",
    "ai_review",
    "ai_status",
    "ai_verdict",
    "holding_disposition",
    "ai_eligible",
    "review_origin",
    "packet_hash",
    "role",
    "current_quantity",
    "sellable_quantity",
    "target_quantity",
    "delta_quantity",
    "target_weight",
    "execution_eligible",
    "realtime_gate",
    "metrics",
    "quote",
    "meta",
)
_REVIEW_FIELDS = (
    "symbol",
    "verdict",
    "strengths",
    "risks",
    "evidence_conflicts",
    "missing_critical_info",
    "summary",
)
_QUOTE_FIELDS = (
    "symbol",
    "price",
    "prev_close",
    "open",
    "high",
    "low",
    "bid",
    "ask",
    "volume",
    "volume_unit",
    "amount",
    "amount_unit",
    "server_time",
    "trade_date",
    "received_at",
    "timestamp_quality",
    "timestamp_source",
    "source",
    "stale",
    "fallback_used",
)
_META_FIELDS = (
    "research_date",
    "phase",
    "market_data_reference_date",
    "as_of",
    "source",
    "fallback_used",
    "stale",
    "snapshot_id",
    "observed_at",
    "server_time",
    "trade_date",
    "received_at",
    "timestamp_quality",
    "timestamp_source",
    "execution_window",
)

_MAX_NESTING = 8
_MAX_COLLECTION_ITEMS = 200
_MAX_TEXT = 4000


def get_run_view(runs_root, run_id, observed_at):
    """Return a read-only summary of one existing run.

    Shape::
        {
            "run_id": str,
            "status": "PENDING"|"RUNNING"|"PARTIAL"|"SUCCESS"|"FAILED",
            "context": {"research_date": str, "phase": str,
                        "market_data_reference_date": str, "as_of": str,
                        "timezone": "Asia/Shanghai"},
            "phase": str,
            "as_of": str,
            "reference_date": str,
            "observed_at": str,
            "config_version": str,
            "config_hash": str,
            "hash": str,  # frozen specification_hash
            "snapshot_id": str,
            "stages": [
                {"stage": str, "input_count": int, "output_count": int,
                 "elapsed": float|None, "elapsed_seconds": float|None,
                 "checkpoint": bool, "status": "PENDING"|"PARTIAL"|"SUCCESS"}
            ],  # exactly S0, S1, S2, S3, S4, S5, S5_5, S6
            "execution_ready": False,
            "current_execution_ready": False,
            "production_accepted": False,
            "execution_ready_at_generation": bool|None,
            "historical_draft_only": bool,
        }

    ``stages`` counts and statuses come from hash-validated stage artifacts,
    never from mutable ``run.json["stages"]`` counters.  The generation flag
    is an informational value from a committed S6 draft; it is never promoted
    to current readiness and no account is read or returned.
    """
    observed = _observed_at(observed_at)
    access = _open_run(runs_root, run_id)
    stages = _load_all_stages(access)
    _validate_stage_context(access, stages)
    generation = _generation_readiness(stages["S6"])
    return {
        "run_id": access["run_id"],
        "status": access["state"]["status"],
        "context": dict(access["context"]),
        "phase": access["context"]["phase"],
        "as_of": access["context"]["as_of"],
        "reference_date": access["context"]["market_data_reference_date"],
        "observed_at": observed,
        "config_version": access["state"]["config_version"],
        "config_hash": access["state"]["config_hash"],
        "hash": access["state"]["specification_hash"],
        "snapshot_id": access["state"]["snapshot_id"],
        "stages": [_stage_summary(stage, stages[stage]) for stage in STAGES],
        "execution_ready": False,
        "current_execution_ready": False,
        "production_accepted": False,
        "execution_ready_at_generation": generation,
        "historical_draft_only": stages["S6"] is not None,
    }


def get_stage_rows(runs_root, run_id, stage, offset=0, limit=100, selected_only=False):
    """Return one bounded, read-only page of a committed stage's public rows.

    Shape::
        {
            "run_id": str, "stage": str,
            "status": "PENDING"|"PARTIAL"|"SUCCESS",
            "input_count": int, "output_count": int,
            "elapsed": float|None, "elapsed_seconds": float|None,
            "checkpoint": bool,
            "offset": int, "limit": int, "selected_only": bool,
            "total": int, "returned": int, "has_more": bool,
            "next_offset": int|None,
            "rows": [public row objects],
            "execution_ready": False,
            "current_execution_ready": False,
            "production_accepted": False,
            "execution_ready_at_generation": bool|None,
            "historical_draft_only": bool,
        }

    Only the explicit public row allow-list is copied.  In particular, this
    function never returns stage-level account data, packets, review request
    or response files, source-provenance paths, configuration, or exceptions.
    ``limit`` is allowed to be zero for an empty page but never exceeds 200.
    """
    _validate_stage(stage)
    _validate_page(offset, limit, selected_only)
    access = _open_run(runs_root, run_id)
    result = _load_stage(access, stage)
    if result is None:
        page_rows: list[dict[str, Any]] = []
        total = 0
        summary = _stage_summary(stage, None)
        generation = None
        historical = False
    else:
        _validate_stage_context(access, {stage: result})
        source_rows = result["rows"]
        _validate_stage_symbols(source_rows, stage)
        filtered = [row for row in source_rows if not selected_only or row.get("selected") is True]
        total = len(filtered)
        page = filtered[offset:offset + limit] if limit else []
        page_rows = [_public_row(row, stage) for row in page]
        summary = _stage_summary(stage, result)
        generation = _generation_readiness(result) if stage == "S6" else None
        historical = stage == "S6"
    returned = len(page_rows)
    has_more = offset + returned < total
    return {
        "run_id": access["run_id"],
        "stage": stage,
        "status": summary["status"],
        "input_count": summary["input_count"],
        "output_count": summary["output_count"],
        "elapsed": summary["elapsed"],
        "elapsed_seconds": summary["elapsed_seconds"],
        "checkpoint": summary["checkpoint"],
        "offset": offset,
        "limit": limit,
        "selected_only": selected_only,
        "total": total,
        "returned": returned,
        "has_more": has_more,
        "next_offset": offset + returned if has_more else None,
        "rows": page_rows,
        "execution_ready": False,
        "current_execution_ready": False,
        "production_accepted": False,
        "execution_ready_at_generation": generation,
        "historical_draft_only": historical,
    }


def get_stock_view(runs_root, run_id, symbol):
    """Return one run-owned canonical stock's bounded cross-stage view.

    Shape::
        {
            "run_id": str, "symbol": str,
            "context": {"research_date": str, "phase": str,
                        "market_data_reference_date": str, "as_of": str,
                        "timezone": "Asia/Shanghai"},
            "phase": str, "as_of": str, "reference_date": str,
            "config_version": str, "hash": str, "snapshot_id": str,
            "identity": public S0 identity mapping|None,
            "stages": [
                {"stage": str, "status": str, "present": bool,
                 "selected": bool|None, "scores": mapping,
                 "reasons": mapping, "ai": public AI mapping|None,
                 "evidence_id": str|None}
            ],  # exactly the eight pipeline stages; absent rows stay absent
            "evidence": public run-local Evidence mapping|None,
            "execution_ready": False,
            "current_execution_ready": False,
            "production_accepted": False,
            "execution_ready_at_generation": bool|None,
            "historical_draft_only": bool,
        }

    The symbol must occur in at least one hash-validated row of this run.
    Evidence is read only when its S3 body hash is explicitly bound by this
    run's S3 provenance (and, when present, its checkpoint is checked too).
    No global EvidenceRepository or ``latest`` lookup is used.
    """
    canonical = _canonical_symbol(symbol)
    access = _open_run(runs_root, run_id)
    stages = _load_all_stages(access)
    _validate_stage_context(access, stages)
    _validate_all_stage_symbols(stages)

    rows_by_stage: dict[str, dict[str, Any] | None] = {}
    owned = False
    for stage in STAGES:
        result = stages[stage]
        row = None
        if result is not None:
            matches = [candidate for candidate in result["rows"] if candidate.get("symbol") == canonical]
            if len(matches) > 1:
                raise PipelineError("STALE_DATA", "run stage contains duplicate stock identity", stage=stage)
            row = matches[0] if matches else None
        rows_by_stage[stage] = row
        owned = owned or row is not None
    if not owned:
        raise PipelineError("NOT_FOUND", "stock is not present in this pipeline run")

    s0_identity = _public_identity(rows_by_stage["S0"].get("identity")) if rows_by_stage["S0"] else None
    evidence = _load_bound_evidence(access, stages, rows_by_stage, canonical)
    return {
        "run_id": access["run_id"],
        "symbol": canonical,
        "context": dict(access["context"]),
        "phase": access["context"]["phase"],
        "as_of": access["context"]["as_of"],
        "reference_date": access["context"]["market_data_reference_date"],
        "config_version": access["state"]["config_version"],
        "hash": access["state"]["specification_hash"],
        "snapshot_id": access["state"]["snapshot_id"],
        "identity": s0_identity,
        "stages": [_stock_stage_view(stage, stages[stage], rows_by_stage[stage]) for stage in STAGES],
        "evidence": evidence,
        "execution_ready": False,
        "current_execution_ready": False,
        "production_accepted": False,
        "execution_ready_at_generation": _generation_readiness(stages["S6"]),
        "historical_draft_only": stages["S6"] is not None,
    }


def list_public_artifacts(runs_root, run_id):
    """List only allow-listed public artifact names and their file SHA256.

    Shape::
        {"run_id": str, "artifacts": [
            {"path": "S0.json"|"S0.parquet"|..., "kind": "stage"|"report",
             "format": "json"|"parquet"|"md"|"ipynb", "sha256": str}
        ]}

    ``path`` is a fixed, relative public identifier, never an arbitrary path
    supplied by a caller.  Stage pairs are included only after
    ``PipelineRepository.load_stage`` validates the committed JSON, semantic
    hash, source references, and Parquet hash.  No configuration, input,
    account, packet, review raw-response, or arbitrary file is listed.
    """
    access = _open_run(runs_root, run_id)
    stages = _load_all_stages(access)
    _validate_stage_context(access, stages)
    artifacts: list[dict[str, Any]] = []
    for stage in STAGES:
        if stages[stage] is None:
            continue
        for suffix, format_name in (("json", "json"), ("parquet", "parquet")):
            relative = f"{stage}.{suffix}"
            path = _safe_existing_path(access["root"], access["directory"], relative)
            if path is None:
                raise PipelineError("STALE_DATA", "committed stage companion is missing", stage=stage)
            artifacts.append({
                "path": relative,
                "kind": "stage",
                "format": format_name,
                "sha256": _file_hash(path, stage=stage),
            })
    for name in _PUBLIC_REPORTS:
        path = _safe_existing_path(access["root"], access["directory"], name)
        if path is None:
            continue
        artifacts.append({
            "path": name,
            "kind": "report",
            "format": Path(name).suffix.removeprefix("."),
            "sha256": _file_hash(path),
        })
    return {"run_id": access["run_id"], "artifacts": artifacts}


def _open_run(runs_root, run_id) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    root = _resolve_root(runs_root)
    raw_directory = root / run_id
    try:
        if not raw_directory.exists() or not raw_directory.is_dir():
            raise PipelineError("NOT_FOUND", "pipeline run is not available")
        directory = raw_directory.resolve(strict=True)
    except PipelineError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise PipelineError("NOT_FOUND", "pipeline run is not available") from exc
    _ensure_confined(root, directory)

    run_path = _safe_existing_path(root, directory, "run.json")
    if run_path is None:
        raise PipelineError("NOT_FOUND", "pipeline run is not available")
    state = _read_json(run_path, "run state")
    if not isinstance(state, Mapping) or state.get("run_id") != run_id:
        raise PipelineError("STALE_DATA", "run state identity is invalid")
    context = _run_context(state)
    _validate_run_state_fields(state)
    repository = object.__new__(PipelineRepository)
    repository.run_id = run_id
    repository.directory = directory
    return {
        "root": root,
        "directory": directory,
        "run_id": run_id,
        "state": dict(state),
        "context": context,
        "repository": repository,
    }


def _resolve_root(value: Any) -> Path:
    try:
        candidate = Path(value)
        if not candidate.exists() or not candidate.is_dir():
            raise PipelineError("NOT_FOUND", "pipeline runs root is not available")
        root = candidate.resolve(strict=True)
    except PipelineError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise PipelineError("NOT_FOUND", "pipeline runs root is not available") from exc
    if not root.is_dir():
        raise PipelineError("NOT_FOUND", "pipeline runs root is not available")
    return root


def _validate_run_id(value: Any) -> str:
    if not isinstance(value, str) or _RUN_ID.fullmatch(value) is None:
        raise PipelineError("INVALID_ARGUMENTS", "invalid pipeline run_id")
    return value


def _validate_stage(value: Any) -> str:
    if value not in STAGES:
        raise PipelineError("INVALID_ARGUMENTS", "unknown pipeline stage")
    return value


def _validate_page(offset: Any, limit: Any, selected_only: Any) -> None:
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise PipelineError("INVALID_ARGUMENTS", "offset must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 200:
        raise PipelineError("LIMIT_EXCEEDED", "limit must be between zero and 200")
    if not isinstance(selected_only, bool):
        raise PipelineError("INVALID_ARGUMENTS", "selected_only must be boolean")


def _observed_at(value: Any) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PipelineError("INVALID_ARGUMENTS", "observed_at must be timezone-aware")
    return value.isoformat()


def _run_context(state: Mapping[str, Any]) -> dict[str, str]:
    try:
        context = {key: state[key] for key in _CONTEXT_FIELDS}
    except KeyError as exc:
        raise PipelineError("STALE_DATA", "run context is incomplete") from exc
    if (
        any(not isinstance(value, str) or not value for value in context.values())
        or context["phase"] not in _PHASES
        or context["timezone"] != "Asia/Shanghai"
    ):
        raise PipelineError("STALE_DATA", "run context is invalid")
    try:
        context_date = datetime.fromisoformat(context["as_of"])
        datetime.fromisoformat(context["research_date"] + "T00:00:00")
        datetime.fromisoformat(context["market_data_reference_date"] + "T00:00:00")
    except ValueError as exc:
        raise PipelineError("STALE_DATA", "run context is invalid") from exc
    if context_date.tzinfo is None or context_date.utcoffset() is None:
        raise PipelineError("STALE_DATA", "run context is invalid")
    return context


def _validate_run_state_fields(state: Mapping[str, Any]) -> None:
    required = ("config_version", "config_hash", "specification_hash", "snapshot_id", "status")
    if any(key not in state for key in required):
        raise PipelineError("STALE_DATA", "run state is incomplete")
    if (
        not isinstance(state["config_version"], str)
        or not state["config_version"]
        or not isinstance(state["snapshot_id"], str)
        or not state["snapshot_id"]
        or not _SHA256.fullmatch(str(state["config_hash"]))
        or not _SHA256.fullmatch(str(state["specification_hash"]))
        or state["status"] not in _RUN_STATUSES
    ):
        raise PipelineError("STALE_DATA", "run state is invalid")


def _ensure_confined(root: Path, target: Path) -> None:
    try:
        confined = target.is_relative_to(root) and target != root
    except (OSError, RuntimeError, ValueError):
        confined = False
    if not confined:
        raise PipelineError("INVALID_ARGUMENTS", "pipeline artifact is outside the permitted root")


def _safe_existing_path(root: Path, directory: Path, relative: str) -> Path | None:
    # All callers pass fixed, generated identifiers.  Keep this check local so
    # a future caller cannot turn this helper into an arbitrary file reader.
    if (
        not isinstance(relative, str)
        or not relative
        or Path(relative).is_absolute()
        or ".." in Path(relative).parts
        or "\\" in relative
    ):
        raise PipelineError("INVALID_ARGUMENTS", "invalid public artifact identifier")
    candidate = directory / relative
    try:
        if not candidate.exists():
            return None
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "pipeline artifact cannot be inspected") from exc
    _ensure_confined(root, resolved)
    try:
        if not resolved.is_file():
            raise PipelineError("STALE_DATA", "pipeline artifact is not a file")
    except OSError as exc:
        raise PipelineError("STALE_DATA", "pipeline artifact cannot be inspected") from exc
    return resolved


def _read_json(path: Path, label: str, expected_hash: str | None = None) -> Any:
    try:
        value = read_json(path, expected_hash=expected_hash)
    except PipelineError as exc:
        if "hash" in str(exc).lower():
            raise PipelineError("STALE_DATA", f"{label} hash mismatch") from exc
        raise PipelineError(exc.code, f"{label} is invalid") from exc
    except (OSError, TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", f"{label} is invalid") from exc
    return value


def _file_hash(path: Path, stage: str | None = None) -> str:
    try:
        return file_hash(path)
    except (OSError, TypeError, ValueError) as exc:
        if stage:
            raise PipelineError("STALE_DATA", "stage artifact hash cannot be read", stage=stage) from exc
        raise PipelineError("STALE_DATA", "public artifact hash cannot be read") from exc


def _load_stage(access: Mapping[str, Any], stage: str) -> dict[str, Any] | None:
    _validate_stage(stage)
    json_path = _safe_existing_path(access["root"], access["directory"], f"{stage}.json")
    if json_path is None:
        # Inspect an orphan companion only for confinement; do not promote it
        # to a checkpoint or infer a stage from it.
        _safe_existing_path(access["root"], access["directory"], f"{stage}.parquet")
        return None
    _safe_existing_path(access["root"], access["directory"], f"{stage}.parquet")
    try:
        result = access["repository"].load_stage(stage)
    except PipelineError as exc:
        message = str(exc).lower()
        if "hash mismatch" in message:
            safe_message = "stage artifact hash mismatch"
        elif "identity mismatch" in message:
            safe_message = "stage artifact identity mismatch"
        elif "semantic" in message:
            safe_message = "stage semantic hash mismatch"
        elif "parquet" in message:
            safe_message = "stage Parquet hash mismatch"
        elif "escape" in message or "outside" in message:
            safe_message = "stage artifact path is outside the permitted root"
        else:
            safe_message = "committed stage artifact is invalid"
        raise PipelineError(exc.code, safe_message, stage=stage) from exc
    except (OSError, TypeError, ValueError, KeyError, IndexError) as exc:
        raise PipelineError("STALE_DATA", "committed stage artifact is invalid", stage=stage) from exc
    if not isinstance(result, dict):
        raise PipelineError("STALE_DATA", "committed stage artifact is invalid", stage=stage)
    return result


def _load_all_stages(access: Mapping[str, Any]) -> dict[str, dict[str, Any] | None]:
    return {stage: _load_stage(access, stage) for stage in STAGES}


def _validate_stage_context(access: Mapping[str, Any], stages: Mapping[str, Any]) -> None:
    state = access["state"]
    context = access["context"]
    for stage, result in stages.items():
        if result is None:
            continue
        for key in _CONTEXT_FIELDS:
            if key in result and result[key] != context[key]:
                raise PipelineError("STALE_DATA", "stage context differs from run", stage=stage)
        if "snapshot_id" in result and result["snapshot_id"] != state["snapshot_id"]:
            raise PipelineError("STALE_DATA", "stage snapshot differs from run", stage=stage)
        if "config_hash" in result and result["config_hash"] != state["config_hash"]:
            raise PipelineError("STALE_DATA", "stage configuration differs from run", stage=stage)


def _validate_stage_symbols(rows: Any, stage: str) -> None:
    if not isinstance(rows, list):
        raise PipelineError("STALE_DATA", "stage rows are invalid", stage=stage)
    for row in rows:
        if not isinstance(row, Mapping):
            raise PipelineError("STALE_DATA", "stage row is invalid", stage=stage)
        try:
            CanonicalSymbol.from_core(row.get("symbol"))
        except (SymbolContractError, TypeError, ValueError) as exc:
            raise PipelineError("STALE_DATA", "stage row identity is not canonical", stage=stage) from exc


def _validate_all_stage_symbols(stages: Mapping[str, Any]) -> None:
    for stage, result in stages.items():
        if result is not None:
            _validate_stage_symbols(result["rows"], stage)


def _stage_summary(stage: str, result: Mapping[str, Any] | None) -> dict[str, Any]:
    if result is None:
        return {
            "stage": stage,
            "input_count": 0,
            "output_count": 0,
            "elapsed": None,
            "elapsed_seconds": None,
            "checkpoint": False,
            "status": "PENDING",
        }
    input_symbols = result.get("input_symbols")
    output_symbols = result.get("output_symbols")
    if not isinstance(input_symbols, list) or not isinstance(output_symbols, list):
        raise PipelineError("STALE_DATA", "stage counts are invalid", stage=stage)
    elapsed = result.get("elapsed_seconds")
    if elapsed is not None and (
        isinstance(elapsed, bool)
        or not isinstance(elapsed, (int, float))
        or not math.isfinite(float(elapsed))
        or elapsed < 0
    ):
        raise PipelineError("STALE_DATA", "stage elapsed time is invalid", stage=stage)
    status = result.get("status")
    if status not in {"SUCCESS", "PARTIAL"}:
        raise PipelineError("STALE_DATA", "stage status is invalid", stage=stage)
    elapsed_value = float(elapsed) if elapsed is not None else None
    return {
        "stage": stage,
        "input_count": len(input_symbols),
        "output_count": len(output_symbols),
        "elapsed": elapsed_value,
        "elapsed_seconds": elapsed_value,
        "checkpoint": True,
        "status": status,
    }


def _generation_readiness(result: Mapping[str, Any] | None) -> bool | None:
    if result is None:
        return None
    value = result.get("execution_ready")
    if value is None:
        return None
    if type(value) is not bool:
        raise PipelineError("STALE_DATA", "generation readiness is invalid", stage="S6")
    return value


def _canonical_symbol(value: Any) -> str:
    try:
        return CanonicalSymbol.from_core(value).symbol
    except (SymbolContractError, TypeError, ValueError) as exc:
        raise PipelineError("INVALID_ARGUMENTS", "invalid canonical stock symbol") from exc


def _public_row(row: Mapping[str, Any], stage: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in _ROW_FIELDS:
        if key not in row:
            continue
        value = row[key]
        if key == "symbol":
            result[key] = _canonical_symbol(value)
        elif key == "identity":
            result[key] = _public_identity(value)
        elif key == "sector_metadata":
            result[key] = _public_mapping(value, _SECTOR_FIELDS, "sector metadata")
        elif key == "board_metadata":
            result[key] = _public_mapping(value, _BOARD_FIELDS, "board metadata")
        elif key == "style_metadata":
            result[key] = _public_mapping(value, _STYLE_FIELDS, "style metadata")
        elif key == "ai_review":
            result[key] = _public_review(value)
        elif key == "quote":
            result[key] = _public_mapping(value, _QUOTE_FIELDS, "quote")
        elif key == "meta":
            result[key] = _public_mapping(value, _META_FIELDS, "runtime metadata")
        else:
            result[key] = _safe_value(value, f"stage.{stage}.{key}")
    # A row cannot be useful to the UI without its identity.  load_stage does
    # not enforce canonical symbol syntax itself, so keep this guard here.
    if "symbol" not in result:
        raise PipelineError("STALE_DATA", "stage row identity is missing", stage=stage)
    return result


def _public_identity(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _public_mapping(value, _IDENTITY_FIELDS, "stock identity")


def _public_mapping(value: Any, fields: tuple[str, ...], label: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise PipelineError("STALE_DATA", f"{label} is invalid")
    return {
        key: _safe_value(value[key], f"{label}.{key}")
        for key in fields
        if key in value
    }


def _public_review(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise PipelineError("STALE_DATA", "AI review is invalid")
    return {
        key: _safe_value(value[key], f"AI review.{key}")
        for key in _REVIEW_FIELDS
        if key in value
    }


def _safe_source(value: Any) -> str | None:
    if isinstance(value, str) and _SOURCE_LABEL.fullmatch(value):
        return value
    return None


def _safe_value(value: Any, label: str, depth: int = 0) -> Any:
    if depth > _MAX_NESTING:
        raise PipelineError("STALE_DATA", "public artifact nesting is too deep")
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PipelineError("STALE_DATA", "public artifact contains a non-finite value")
        return value
    if isinstance(value, str):
        if len(value) > _MAX_TEXT:
            raise PipelineError("STALE_DATA", "public artifact text is too large")
        return value
    if isinstance(value, Mapping):
        if len(value) > _MAX_COLLECTION_ITEMS:
            raise PipelineError("STALE_DATA", "public artifact mapping is too large")
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PipelineError("STALE_DATA", "public artifact key is invalid")
            lowered = key.lower()
            if lowered in _PRIVATE_KEYS:
                continue
            if lowered == "source":
                source = _safe_source(item)
                if source is not None:
                    result[key] = source
                continue
            result[key] = _safe_value(item, f"{label}.{key}", depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_COLLECTION_ITEMS:
            raise PipelineError("STALE_DATA", "public artifact list is too large")
        return [_safe_value(item, f"{label}[{index}]", depth + 1) for index, item in enumerate(value)]
    raise PipelineError("STALE_DATA", "public artifact value is not JSON-safe")


def _public_scores(row: Mapping[str, Any]) -> dict[str, Any]:
    scores: dict[str, Any] = {}
    for key in (*_SCORE_FIELDS, *_SCORE_MAPPING_FIELDS):
        if key in row:
            scores[key] = _safe_value(row[key], f"scores.{key}")
    return scores


def _public_reasons(row: Mapping[str, Any]) -> dict[str, Any]:
    reasons: dict[str, Any] = {}
    for key in _REASON_FIELDS:
        if key in row:
            reasons[key] = _safe_value(row[key], f"reasons.{key}")
    return reasons


def _public_ai(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if not any(key in row for key in ("ai_review", "ai_status", "ai_verdict", "ai_eligible", "review_origin")):
        return None
    result: dict[str, Any] = {
        "status": _safe_value(row.get("ai_status"), "AI status") if "ai_status" in row else None,
        "verdict": _safe_value(row.get("ai_verdict"), "AI verdict") if "ai_verdict" in row else None,
        "eligible": _safe_value(row.get("ai_eligible"), "AI eligibility") if "ai_eligible" in row else None,
        "origin": _safe_value(row.get("review_origin"), "AI origin") if "review_origin" in row else None,
        "report": _public_review(row.get("ai_review")) if "ai_review" in row else None,
    }
    return result


def _stock_stage_view(stage: str, result: Mapping[str, Any] | None, row: Mapping[str, Any] | None) -> dict[str, Any]:
    if result is None:
        return {
            "stage": stage,
            "status": "PENDING",
            "present": False,
            "selected": None,
            "scores": {},
            "reasons": {},
            "ai": None,
            "evidence_id": None,
        }
    if row is None:
        return {
            "stage": stage,
            "status": result["status"],
            "present": False,
            "selected": None,
            "scores": {},
            "reasons": {},
            "ai": None,
            "evidence_id": None,
        }
    evidence_id = row.get("evidence_id")
    if evidence_id is not None and (not isinstance(evidence_id, str) or _SHA256.fullmatch(evidence_id) is None):
        raise PipelineError("STALE_DATA", "stage Evidence identity is invalid", stage=stage)
    return {
        "stage": stage,
        "status": result["status"],
        "present": True,
        "selected": row.get("selected") if type(row.get("selected")) is bool else None,
        "scores": _public_scores(row),
        "reasons": _public_reasons(row),
        "ai": _public_ai(row),
        "evidence_id": evidence_id,
    }


def _load_bound_evidence(
    access: Mapping[str, Any],
    stages: Mapping[str, Any],
    rows_by_stage: Mapping[str, Mapping[str, Any] | None],
    symbol: str,
) -> dict[str, Any] | None:
    evidence_ids = {
        row.get("evidence_id")
        for row in rows_by_stage.values()
        if row is not None and row.get("evidence_id") is not None
    }
    if not evidence_ids:
        return None
    if len(evidence_ids) != 1:
        raise PipelineError("STALE_DATA", "stock has conflicting Evidence identities")
    evidence_id = next(iter(evidence_ids))
    if not isinstance(evidence_id, str) or _SHA256.fullmatch(evidence_id) is None:
        raise PipelineError("STALE_DATA", "run Evidence identity is invalid")
    s3 = stages["S3"]
    if s3 is None:
        raise PipelineError("STALE_DATA", "run Evidence is not bound to a committed S3 stage")
    references = s3.get("source_provenance")
    if not isinstance(references, list):
        raise PipelineError("STALE_DATA", "run Evidence binding is invalid")
    body_relative = f"evidence/{evidence_id}.json"
    body_refs = [
        reference
        for reference in references
        if isinstance(reference, Mapping)
        and reference.get("path") == body_relative
        and reference.get("evidence_id", evidence_id) == evidence_id
    ]
    if len(body_refs) != 1:
        raise PipelineError("STALE_DATA", "run Evidence body is not hash-bound")
    body_hash = body_refs[0].get("sha256")
    if not isinstance(body_hash, str) or _SHA256.fullmatch(body_hash) is None:
        raise PipelineError("STALE_DATA", "run Evidence body binding is invalid")
    body_path = _safe_existing_path(access["root"], access["directory"], body_relative)
    if body_path is None:
        raise PipelineError("STALE_DATA", "run Evidence body is missing")
    body = _read_json(body_path, "run Evidence body", expected_hash=body_hash)
    try:
        bundle = EvidenceBundle.from_dict(body)
    except (EvidenceContractError, TypeError, ValueError) as exc:
        raise PipelineError("STALE_DATA", "run Evidence body is invalid") from exc
    if (
        bundle.evidence_id != evidence_id
        or bundle.symbol.symbol != symbol
        or bundle.snapshot_id != access["state"]["snapshot_id"]
        or bundle.as_of.isoformat() != access["context"]["as_of"]
    ):
        raise PipelineError("STALE_DATA", "run Evidence identity differs from run")

    checkpoint_relative = f"evidence/{symbol}.checkpoint.json"
    checkpoint_refs = [
        reference
        for reference in references
        if isinstance(reference, Mapping) and reference.get("path") == checkpoint_relative
    ]
    if len(checkpoint_refs) > 1:
        raise PipelineError("STALE_DATA", "run Evidence checkpoint binding is ambiguous")
    if checkpoint_refs:
        checkpoint_hash = checkpoint_refs[0].get("sha256")
        if not isinstance(checkpoint_hash, str) or _SHA256.fullmatch(checkpoint_hash) is None:
            raise PipelineError("STALE_DATA", "run Evidence checkpoint binding is invalid")
        checkpoint_path = _safe_existing_path(access["root"], access["directory"], checkpoint_relative)
        if checkpoint_path is None:
            raise PipelineError("STALE_DATA", "run Evidence checkpoint is missing")
        checkpoint = _read_json(checkpoint_path, "run Evidence checkpoint", expected_hash=checkpoint_hash)
        if not isinstance(checkpoint, Mapping):
            raise PipelineError("STALE_DATA", "run Evidence checkpoint is invalid")
        record = dict(checkpoint)
        integrity = record.pop("integrity_hash", None)
        if not isinstance(integrity, str) or content_hash(record) != integrity:
            raise PipelineError("STALE_DATA", "run Evidence checkpoint hash mismatch")
        if (
            record.get("symbol") != symbol
            or record.get("evidence_id") != evidence_id
            or record.get("body_path") != body_relative
            or record.get("body_hash") != body_hash
        ):
            raise PipelineError("STALE_DATA", "run Evidence checkpoint identity mismatch")

    return _public_evidence(bundle)


def _public_evidence(bundle: EvidenceBundle) -> dict[str, Any]:
    sections = {
        name: _safe_value(dict(bundle.sections[name]), f"Evidence.{name}")
        for name in (
            "market",
            "technical",
            "flow",
            "margin",
            "dragon_tiger",
            "fundamental",
            "events",
            "risk",
        )
    }
    provenance = []
    for item in bundle.provenance:
        source = _safe_source(item.source)
        record: dict[str, Any] = {"field": item.field, "as_of": item.as_of.isoformat()}
        if source is not None:
            record["source"] = source
        provenance.append(record)
    return {
        "evidence_id": bundle.evidence_id,
        "symbol": bundle.symbol.symbol,
        "as_of": bundle.as_of.isoformat(),
        "snapshot_id": bundle.snapshot_id,
        "missing": list(bundle.missing),
        "sections": sections,
        "provenance": provenance,
    }


__all__ = [
    "get_run_view",
    "get_stage_rows",
    "get_stock_view",
    "list_public_artifacts",
]

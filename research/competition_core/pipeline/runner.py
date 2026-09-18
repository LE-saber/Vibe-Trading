"""Program-owned research stages. No Agent Tool wrappers and no implicit data date."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Callable

import pandas as pd

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.config import PipelineConfig
from competition_core.pipeline.dataset import CanonicalDataset
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.features import base_features, supplemental_features
from competition_core.pipeline.persistence import PipelineRepository
from competition_core.pipeline.scoring import score_stage
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext, SHANGHAI
from contracts.symbol import CanonicalSymbol

ROOT = Path(__file__).resolve().parents[2]
_SNAPSHOT_FIELDS = ("schema_version", "competition_id", "as_of", "snapshot_phase",
                    "market_data_reference_date", "members", "sources", "warnings")


def validate_snapshot(snapshot: dict[str, Any], context: ResearchContext) -> list[str]:
    context.validate_snapshot(snapshot)
    if not set(_SNAPSHOT_FIELDS) <= set(snapshot) or not isinstance(snapshot["members"], list):
        raise PipelineError("STALE_DATA", "Universe snapshot schema is invalid")
    original = json.dumps({key: snapshot[key] for key in _SNAPSHOT_FIELDS}, sort_keys=True,
                          ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if hashlib.sha256(original).hexdigest() != snapshot["snapshot_id"]:
        raise PipelineError("STALE_DATA", "Universe snapshot original content hash mismatch")
    all_symbols, eligible = [], []
    for row in snapshot["members"]:
        identity = CanonicalSymbol.from_core(row["ts_code"])
        if row.get("exchange") != identity.exchange or type(row.get("is_eligible")) is not bool:
            raise PipelineError("IDENTITY_CONFLICT", "Universe member identity/eligibility is invalid")
        all_symbols.append(identity.symbol)
        if row["is_eligible"]:
            eligible.append(identity.symbol)
    if len(set(all_symbols)) != len(all_symbols) or not eligible:
        raise PipelineError("IDENTITY_CONFLICT", "Universe has duplicates or no eligible stocks")
    return sorted(eligible)


def code_fingerprint(vibe_root: Path) -> dict[str, str]:
    paths = [*(ROOT / "competition_core/pipeline").glob("*.py"),
             ROOT / "competition_core/technical_indicators.py",
             ROOT / "adapters/vibe/technical.py", ROOT / "scripts/vibe-technical-worker.py"]
    hashes = {str(path.relative_to(ROOT)).replace("\\", "/"): file_hash(path) for path in sorted(paths)}
    hashes["runtime/pandas"] = pd.__version__
    return hashes


def _feature_checkpoint(repository: PipelineRepository, *, panel: pd.DataFrame, context: ResearchContext,
                        sessions: list[str], symbols: list[str], specification: dict[str, Any],
                        vibe_root: Path, technical_provider: Callable[..., dict[str, Any]], timeout: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = repository.directory / "features.json"
    identity = {"specification_hash": content_hash(specification), "symbols_hash": content_hash(symbols)}
    if path.exists():
        frozen = read_json(path)
        digest = frozen.pop("integrity_hash", None)
        if content_hash(frozen) != digest or any(frozen.get(key) != value for key, value in identity.items()):
            raise PipelineError("STALE_DATA", "feature checkpoint content/input mismatch")
        frozen["integrity_hash"] = digest
    else:
        technical = technical_provider(panel, context=context, sessions=sessions, symbols=symbols,
                                       work_dir=repository.directory / "work/technical", vibe_root=vibe_root, timeout_seconds=timeout)
        frame = base_features(panel, context=context, sessions=sessions, symbols=symbols, technical_rows=technical["rows"])
        frozen = {**identity, "context": context.to_dict(), "rows": frame.astype(object).where(frame.notna(), None).to_dict("records"),
                  "technical_provenance": technical["provenance"]}
        frozen["integrity_hash"] = content_hash(frozen)
        atomic_json(path, frozen, immutable=True)
    frame = pd.DataFrame(frozen["rows"]).set_index("symbol", drop=False)
    if sorted(frame.index) != symbols:
        raise PipelineError("IDENTITY_CONFLICT", "feature checkpoint differs from full eligible Universe")
    return frame, {"path": path.name, "sha256": file_hash(path), "technical": frozen["technical_provenance"]}


def _s0(snapshot: dict[str, Any], symbols: list[str]) -> dict[str, Any]:
    reasons = Counter(reason for row in snapshot["members"] if not row["is_eligible"] for reason in row["exclusion_reasons"])
    rows = [{"symbol": member["ts_code"], "identity": member, "selected": member["is_eligible"],
             "reject_reasons": member["exclusion_reasons"],
             "keep_reasons": ["competition_universe_eligible"] if member["is_eligible"] else []}
            for member in sorted(snapshot["members"], key=lambda row: row["ts_code"])]
    return {"stage": "S0", "status": "SUCCESS", "input_symbols": [row["symbol"] for row in rows],
            "output_symbols": symbols, "rows": rows,
            "summary": {"candidate_count": len(rows), "eligible_count": len(symbols), "excluded_count": len(rows) - len(symbols),
                        "excluded_reason_counts": dict(sorted(reasons.items())), "snapshot_id": snapshot["snapshot_id"],
                        "market_data_reference_date": snapshot["market_data_reference_date"], "llm_calls": 0, "agent_tool_calls": 0}}


def run_screening(*, dataset_root: Path, manifest_path: Path, snapshot: dict[str, Any], context: ResearchContext,
                  config: PipelineConfig, repository: PipelineRepository, source_client: ResearchDataClient,
                  vibe_root: Path, technical_provider: Callable[..., dict[str, Any]] | None = None,
                  through_stage: str = "S2", progress: Callable[[dict[str, Any]], None] | None = None,
                  evidence_store=None) -> dict[str, Any]:
    """Run/resume S0–S4. Completed research stages are not full-pipeline acceptance."""
    stages = ("S0", "S1", "S2", "S3", "S4")
    if through_stage not in stages:
        raise PipelineError("INVALID_ARGUMENTS", "research currently supports through S0/S1/S2/S3/S4")
    if through_stage in {"S3", "S4"}:
        from competition_core.pipeline.evidence_builder import evidence_policy
        evidence_policy(config)
    if source_client.context != context:
        raise PipelineError("STALE_DATA", "research source context differs from run")
    if context.as_of > datetime.now(SHANGHAI):
        raise PipelineError("NOT_READY", "research as_of is in the future")
    from adapters.vibe.technical import compute_technical
    from competition_core.pipeline.dated_cache import DatedSourceCache
    technical_provider = technical_provider or compute_technical
    with repository.lock():
        symbols = validate_snapshot(snapshot, context)
        manifest_path = Path(manifest_path).resolve()
        manifest = read_json(manifest_path)
        dataset = CanonicalDataset(dataset_root, manifest["source"])
        requested_stages = stages[:stages.index(through_stage) + 1]
        all_checkpoints_present = all(
            (repository.directory / f"{stage}.json").is_file() for stage in requested_stages
        )
        if all_checkpoints_present:
            # A complete resume only needs frozen-input identity checks here;
            # recover_checkpoint below still validates every checkpoint hash.
            dataset._validate_manifest_header(manifest, manifest_path)
            panel_path = dataset._resolve_path(manifest["panel_path"], "panel")
            if file_hash(panel_path) != manifest["panel_hash"]:
                raise PipelineError("STALE_DATA", "panel hash mismatch", path=str(panel_path))
            panel = None
        else:
            panel = dataset.load(manifest_path)
        if (manifest["symbols"] != symbols or manifest["snapshot_id"] != snapshot["snapshot_id"]
                or any(manifest[key] != context.to_dict()[key] for key in ("research_date", "phase", "market_data_reference_date", "timezone"))):
            raise PipelineError("STALE_DATA", "canonical dataset differs from frozen Universe/context")
        quality_path = dataset._resolve_path(manifest["quality_path"], "quality")
        quality = read_json(quality_path, expected_hash=manifest["quality_hash"])
        settings = config.section("dataset")
        if manifest["sessions"] < settings["warmup_sessions"] or quality["full_qfq_history_ratio"] < settings["minimum_full_history_ratio"]:
            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "canonical warmup/coverage gate is not satisfied")
        fingerprint = code_fingerprint(vibe_root)
        specification = {"context": context.to_dict(), "snapshot_id": snapshot["snapshot_id"], "config": config.to_dict(),
                         "config_hash": config.config_hash, "dataset_root": str(dataset.root), "manifest_path": str(manifest_path),
                         "manifest_hash": file_hash(manifest_path), "dataset_id": manifest["dataset_id"],
                         "code_hashes": fingerprint, "vibe_root": str(vibe_root.resolve())}
        repository.initialize(specification)
        atomic_json(repository.directory / "snapshot.json", snapshot, immutable=True)
        metadata = {**context.to_dict(), "snapshot_id": snapshot["snapshot_id"], "config_hash": config.config_hash,
                    "dataset_id": manifest["dataset_id"], "source": manifest["source"], "fallback_used": False, "stale": False,
                    "source_provenance": [{"dataset_manifest_hash": file_hash(manifest_path), "panel_hash": manifest["panel_hash"]}]}
        sessions = manifest["session_dates"]
        features = None
        s1 = None
        try:
            for stage in requested_stages:
                saved = repository.recover_checkpoint(stage)
                if saved is not None:
                    if stage == "S1":
                        s1 = saved
                    if progress:
                        progress({"stage": stage, "checkpoint_reused": True, "count": len(saved["output_symbols"])})
                    continue
                repository.start_stage(stage)
                started = time.monotonic()
                if progress:
                    progress({"stage": stage, "status": "RUNNING"})
                stage_meta = dict(metadata)
                if stage == "S0":
                    result = _s0(snapshot, symbols)
                elif stage == "S4":
                    from competition_core.pipeline.diversification import build_diversified_stage
                    result = build_diversified_stage(s3=repository.load_stage("S3"), snapshot=snapshot,
                        panel=panel, sessions=sessions, context=context, config=config)
                    stage_meta["source_provenance"] = [*metadata["source_provenance"],
                        {"path": "S3.json", "sha256": file_hash(repository.directory / "S3.json")}]
                elif stage == "S3":
                    from competition_core.pipeline.evidence_builder import build_evidence_stage
                    from competition_core.storage.evidence import EvidenceRepository
                    evidence_store = evidence_store or EvidenceRepository()
                    cache = DatedSourceCache(repository.directory / "sources", source_client, source_name=manifest["source"])
                    result, references = build_evidence_stage(s2=repository.load_stage("S2"), snapshot=snapshot,
                        panel=panel, sessions=sessions, context=context, config=config, repository=repository,
                        store=evidence_store, cache=cache, source=manifest["source"], progress=progress)
                    stage_meta["source_provenance"] = [*metadata["source_provenance"], *references]
                else:
                    if features is None:
                        features, feature_ref = _feature_checkpoint(repository, panel=panel, context=context, sessions=sessions,
                            symbols=symbols, specification=specification, vibe_root=vibe_root, technical_provider=technical_provider,
                            timeout=config.section("screening")["technical_timeout_seconds"])
                    stage_meta["source_provenance"] = [*metadata["source_provenance"], feature_ref]
                    if stage == "S1":
                        result = score_stage(features, config, stage=stage)
                    else:
                        s1 = s1 or repository.load_stage("S1")
                        if not s1["output_symbols"]:
                            raise PipelineError("DATA_COVERAGE_INSUFFICIENT", "S1 has no scorable candidates")
                        cache = DatedSourceCache(repository.directory / "sources", source_client, source_name=manifest["source"])
                        screening = config.section("screening")
                        benchmark = cache.get("index_daily", {"ts_code": screening["benchmark_symbol"],
                            "start_date": sessions[0].replace("-", ""), "end_date": sessions[-1].replace("-", ""),
                            "fields": "ts_code,trade_date,close"}, key_fields=("ts_code", "trade_date"))
                        limits = [cache.get("stk_limit", {"trade_date": day.replace("-", ""), "fields": "ts_code,trade_date,pre_close,up_limit,down_limit"},
                                            key_fields=("ts_code", "trade_date")) for day in sessions[-screening["limit_lookback_sessions"]:]]
                        expanded = supplemental_features(features.loc[s1["output_symbols"]], panel, context=context, sessions=sessions,
                            benchmark_symbol=screening["benchmark_symbol"], benchmark=benchmark, limit_batches=limits,
                            limit_sessions=screening["limit_lookback_sessions"])
                        result = score_stage(expanded, config, stage=stage)
                        previous = {row["symbol"]: row for row in s1["rows"]}
                        for row in result["rows"]:
                            row["s1_quant_score"], row["s1_rank"] = previous[row["symbol"]]["quant_score"], previous[row["symbol"]]["rank"]
                        result["summary"]["source_status"] = {"benchmark": benchmark["status"], "price_limits": [batch["status"] for batch in limits]}
                        stage_meta["source_provenance"] += [{key: batch[key] for key in ("endpoint", "request_id", "source", "observed_at", "integrity_hash", "status")}
                                                               for batch in [benchmark, *limits]]
                if fingerprint != code_fingerprint(vibe_root):
                    raise PipelineError("STALE_DATA", "pipeline code changed during stage calculation")
                committed = repository.commit_stage(stage, result, metadata=stage_meta, elapsed=time.monotonic() - started)
                if stage == "S1":
                    s1 = committed
                if progress:
                    progress({"stage": stage, "status": result["status"], "count": len(result["output_symbols"]), "elapsed_seconds": committed["elapsed_seconds"]})
            return repository.update(status="PARTIAL", completed_through=through_stage,
                                     pending_stage={"S0": "S1", "S1": "S2", "S2": "S3", "S3": "S4", "S4": "S5"}[through_stage], error=None)
        except Exception as exc:
            error = exc.to_dict() if isinstance(exc, PipelineError) else {"code": "INTERNAL_ERROR", "message": "research stage failed", "details": {"exception_type": type(exc).__name__}}
            repository.update(status="FAILED", error=error)
            raise

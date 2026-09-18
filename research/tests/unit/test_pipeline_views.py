"""Synthetic, offline tests for the public read-only pipeline projections."""

from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path

import pytest

from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, content_hash, file_hash, read_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.persistence import PipelineRepository, STAGES
from competition_core.pipeline_views import (
    get_run_view,
    get_stage_rows,
    get_stock_view,
    list_public_artifacts,
)
from contracts.evidence import EvidenceBundle, SECTION_NAMES
from contracts.symbol import CanonicalSymbol
from tests.pipeline_fixtures import context, small_config


OBSERVED_AT = datetime.fromisoformat("2026-09-03T09:00:00+08:00")
SYMBOLS = ["600001.SH", "600002.SH", "300001.SZ", "000001.SZ"]


def _repository(tmp_path: Path, run_id: str = "view-run") -> tuple[Path, PipelineRepository, object, object]:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    repository = PipelineRepository(runs_root, run_id)
    research_context = context()
    config = small_config()
    repository.initialize({
        "context": research_context.to_dict(),
        "snapshot_id": "snapshot-view-fixture",
        "config": config.to_dict(),
        "config_hash": config.config_hash,
    })
    return runs_root, repository, research_context, config


def _row(
    symbol: str,
    *,
    selected: bool,
    evidence_id: str | None = None,
    include_ai: bool = False,
    score: float = 81.5,
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "selected": selected,
        "score": score,
        "quant_score": score - 1,
        "selection_score": score + 1,
        "component_scores": {"trend": score, "liquidity": 72.0},
        "keep_reasons": ["fixture momentum survived screening"],
        "reject_reasons": [],
        "risk_flags": [],
        "features": {"private_feature": "not public"},
        "account": {"account_json": "TOP-SECRET-ACCOUNT"},
        "raw_response": {"secret": "TOP-SECRET-MODEL"},
    }
    if evidence_id is not None:
        row["evidence_id"] = evidence_id
        row["evidence_confidence"] = 0.94
    if include_ai:
        row.update({
            "ai_status": "SUCCESS",
            "ai_verdict": "KEEP",
            "ai_eligible": True,
            "review_origin": "model",
            "packet_hash": "a" * 64,
            "ai_review": {
                "symbol": symbol,
                "verdict": "KEEP",
                "strengths": ["evidence-backed trend"],
                "risks": ["fixture risk"],
                "evidence_conflicts": [],
                "missing_critical_info": [],
                "summary": "Synthetic review only.",
                "raw_response": "TOP-SECRET-MODEL",
            },
        })
    return row


def _commit_stage(
    repository: PipelineRepository,
    research_context: object,
    config: object,
    stage: str,
    rows: list[dict[str, object]],
    *,
    output_symbols: list[str] | None = None,
    status: str = "SUCCESS",
    extra_metadata: dict[str, object] | None = None,
    extra_result: dict[str, object] | None = None,
) -> dict[str, object]:
    outputs = output_symbols if output_symbols is not None else [
        row["symbol"] for row in rows if row.get("selected") is True
    ]
    result: dict[str, object] = {
        "stage": stage,
        "status": status,
        "input_symbols": [row["symbol"] for row in rows],
        "output_symbols": outputs,
        "rows": rows,
    }
    if extra_result:
        result.update(extra_result)
    metadata: dict[str, object] = {
        **research_context.to_dict(),
        "snapshot_id": "snapshot-view-fixture",
        "config_hash": config.config_hash,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    repository.start_stage(stage)
    return repository.commit_stage(stage, result, metadata=metadata, elapsed=1.25)


def _write_bound_evidence(repository: PipelineRepository, research_context: object) -> tuple[str, dict[str, object]]:
    symbol = CanonicalSymbol.from_core("600001.SH")
    bundle = EvidenceBundle(
        symbol=symbol,
        as_of=research_context.as_of,
        snapshot_id="snapshot-view-fixture",
        sections={name: {} for name in SECTION_NAMES},
        missing=(),
        provenance=(),
    )
    evidence_id = bundle.evidence_id
    body_relative = f"evidence/{evidence_id}.json"
    body_path = repository.directory / body_relative
    atomic_json(body_path, bundle.to_dict(), immutable=True)
    body_hash = file_hash(body_path)

    checkpoint_relative = "evidence/600001.SH.checkpoint.json"
    checkpoint_path = repository.directory / checkpoint_relative
    checkpoint_record: dict[str, object] = {
        "symbol": "600001.SH",
        "inputs_hash": "b" * 64,
        "candidate_hash": "c" * 64,
        "member_hash": "d" * 64,
        "evidence_id": evidence_id,
        "body_path": body_relative,
        "body_hash": body_hash,
    }
    atomic_json(checkpoint_path, {
        **checkpoint_record,
        "integrity_hash": content_hash(checkpoint_record),
    }, immutable=True)
    return evidence_id, {
        "path": body_relative,
        "sha256": body_hash,
        "evidence_id": evidence_id,
        "checkpoint": {
            "path": checkpoint_relative,
            "sha256": file_hash(checkpoint_path),
        },
    }


def _commit_chain(repository: PipelineRepository, research_context: object, config: object) -> str:
    evidence_id, evidence_ref = _write_bound_evidence(repository, research_context)
    for stage in STAGES:
        rows = [
            _row(
                symbol,
                selected=symbol in {"600001.SH", "600002.SH"},
                evidence_id=evidence_id if symbol == "600001.SH" and stage in {"S3", "S4", "S5", "S6"} else None,
                include_ai=stage == "S5" and symbol == "600001.SH",
                score=80 + index,
            )
            for index, symbol in enumerate(SYMBOLS)
        ]
        metadata = {"source_provenance": [
            {"path": evidence_ref["path"], "sha256": evidence_ref["sha256"], "evidence_id": evidence_id},
            {"path": evidence_ref["checkpoint"]["path"], "sha256": evidence_ref["checkpoint"]["sha256"]},
        ]} if stage == "S3" else None
        extra_result = None
        if stage == "S6":
            extra_result = {
                "execution_ready": True,
                "production_accepted": True,
                "account_assessment": {"account_json": "TOP-SECRET-ACCOUNT"},
                "raw_response": {"secret": "TOP-SECRET-MODEL"},
            }
        _commit_stage(
            repository,
            research_context,
            config,
            stage,
            rows,
            status="PARTIAL" if stage == "S3" else "SUCCESS",
            extra_metadata=metadata,
            extra_result=extra_result,
        )
    repository.update(status="SUCCESS", current_stage="S6", execution_ready=True, production_accepted=True)
    return evidence_id


def _walk_paths(root: Path) -> set[str]:
    return {str(path.relative_to(root)) for path in root.rglob("*")}


def test_missing_run_get_does_not_create_directory(tmp_path):
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    before = _walk_paths(runs_root)

    with pytest.raises(PipelineError) as raised:
        get_run_view(runs_root, "missing-run", OBSERVED_AT)

    assert raised.value.code == "NOT_FOUND"
    assert _walk_paths(runs_root) == before


def test_run_view_uses_hash_validated_stage_counts_and_separates_readiness(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_chain(repository, research_context, config)
    state = read_json(repository.directory / "run.json")
    state["stages"]["S0"]["output_count"] = 999999
    atomic_json(repository.directory / "run.json", state)

    view = get_run_view(runs_root, repository.run_id, OBSERVED_AT)

    assert [stage["stage"] for stage in view["stages"]] == list(STAGES)
    assert view["stages"][0]["input_count"] == len(SYMBOLS)
    assert view["stages"][0]["output_count"] == 2
    assert view["stages"][3]["status"] == "PARTIAL"
    assert view["stages"][6]["status"] == "SUCCESS"
    assert view["execution_ready"] is False
    assert view["current_execution_ready"] is False
    assert view["production_accepted"] is False
    assert view["execution_ready_at_generation"] is True
    assert view["historical_draft_only"] is True
    assert view["context"]["as_of"] == research_context.as_of.isoformat()
    assert view["config_version"] == config.version
    assert view["hash"] == state["specification_hash"]


def test_stage_rows_are_bounded_and_total_is_filtered_total(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    rows = [
        _row(symbol, selected=index % 2 == 0, score=70 + index)
        for index, symbol in enumerate(SYMBOLS)
    ]
    _commit_stage(repository, research_context, config, "S0", rows)

    page = get_stage_rows(runs_root, repository.run_id, "S0", offset=1, limit=1, selected_only=True)

    assert page["total"] == 2
    assert page["returned"] == 1
    assert page["has_more"] is False
    assert page["next_offset"] is None
    assert page["rows"][0]["symbol"] == "300001.SZ"
    assert "features" not in page["rows"][0]
    assert "account" not in page["rows"][0]
    assert "raw_response" not in page["rows"][0]
    assert page["execution_ready"] is False

    with pytest.raises(PipelineError) as too_large:
        get_stage_rows(runs_root, repository.run_id, "S0", limit=201)
    assert too_large.value.code == "LIMIT_EXCEEDED"
    with pytest.raises(PipelineError):
        get_stage_rows(runs_root, repository.run_id, "S0", offset=-1)


def test_stage_and_run_symlinks_cannot_escape_root(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_stage(repository, research_context, config, "S0", [_row("600001.SH", selected=True)])
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "S0.json").write_text("{}", encoding="utf-8")
    escaped_run = tmp_path / "escaped-run"
    escaped_run.mkdir()

    try:
        (repository.directory / "S0.json").unlink()
        (repository.directory / "S0.json").symlink_to(outside / "S0.json")
        escaped_link = runs_root / "escaped"
        escaped_link.symlink_to(escaped_run, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(PipelineError):
        get_stage_rows(runs_root, repository.run_id, "S0")
    with pytest.raises(PipelineError):
        get_run_view(runs_root, "escaped", OBSERVED_AT)


@pytest.mark.parametrize("artifact", ["S0.json", "S0.parquet"])
def test_tampered_stage_json_or_parquet_is_rejected(tmp_path, artifact):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_stage(repository, research_context, config, "S0", [_row("600001.SH", selected=True)])
    path = repository.directory / artifact
    if artifact.endswith("json"):
        value = read_json(path)
        value["rows"][0]["score"] = 0
        path.write_text(json.dumps(value), encoding="utf-8")
    else:
        path.write_bytes(b"tampered parquet")

    with pytest.raises(PipelineError) as raised:
        get_stage_rows(runs_root, repository.run_id, "S0")
    assert raised.value.code == "STALE_DATA"
    assert "tampered" not in str(raised.value).lower()
    assert "D:\\" not in str(raised.value)


def test_stock_view_joins_only_run_owned_rows_and_bound_evidence(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    evidence_id = _commit_chain(repository, research_context, config)

    view = get_stock_view(runs_root, repository.run_id, "600001.SH")

    assert view["symbol"] == "600001.SH"
    assert [stage["stage"] for stage in view["stages"]] == list(STAGES)
    assert view["stages"][0]["present"] is True
    assert view["stages"][0]["scores"]["score"] == 80
    assert view["stages"][3]["evidence_id"] == evidence_id
    assert view["stages"][5]["ai"]["report"]["verdict"] == "KEEP"
    assert view["evidence"]["evidence_id"] == evidence_id
    assert view["evidence"]["snapshot_id"] == "snapshot-view-fixture"
    assert "account_assessment" not in view
    encoded = json.dumps(view, ensure_ascii=False)
    assert "TOP-SECRET" not in encoded
    assert "raw_response" not in encoded
    assert "C:\\" not in encoded

    with pytest.raises(PipelineError) as missing:
        get_stock_view(runs_root, repository.run_id, "600003.SH")
    assert missing.value.code == "NOT_FOUND"
    with pytest.raises(PipelineError) as traversal:
        get_stock_view(runs_root, repository.run_id, "../../600001.SH")
    assert traversal.value.code == "INVALID_ARGUMENTS"


def test_stock_view_does_not_use_unbound_or_latest_evidence(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_stage(repository, research_context, config, "S0", [_row("600001.SH", selected=True)])
    # A separate, unreferenced body must not become a substitute Evidence source.
    unbound = repository.directory / "evidence" / ("e" * 64 + ".json")
    atomic_json(unbound, {"symbol": "600001.SH", "snapshot_id": "newer-latest"})

    view = get_stock_view(runs_root, repository.run_id, "600001.SH")

    assert view["evidence"] is None
    assert view["stages"][0]["evidence_id"] is None


def test_public_artifact_index_is_allowlisted_and_hashes_files(tmp_path):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_stage(repository, research_context, config, "S0", [_row("600001.SH", selected=True)])
    atomic_bytes(repository.directory / "ai-review-audit.md", b"public report\n")
    atomic_bytes(repository.directory / "random.txt", b"do not list")
    atomic_bytes(repository.directory / "portfolio.json", b"account-like file")

    listing = list_public_artifacts(runs_root, repository.run_id)

    paths = {artifact["path"] for artifact in listing["artifacts"]}
    assert paths == {"S0.json", "S0.parquet", "ai-review-audit.md"}
    for artifact in listing["artifacts"]:
        assert not Path(artifact["path"]).is_absolute()
        assert artifact["sha256"] == file_hash(repository.directory / artifact["path"])
    assert "random.txt" not in paths
    assert "portfolio.json" not in paths


def test_public_views_do_not_call_recovery_or_expose_stage_level_secrets(tmp_path, monkeypatch):
    runs_root, repository, research_context, config = _repository(tmp_path)
    _commit_chain(repository, research_context, config)

    def fail_recovery(*args, **kwargs):
        raise AssertionError("GET must not recover checkpoints")

    monkeypatch.setattr(PipelineRepository, "recover_checkpoint", fail_recovery)
    run_view = get_run_view(runs_root, repository.run_id, OBSERVED_AT)
    rows_view = get_stage_rows(runs_root, repository.run_id, "S6")

    encoded = json.dumps({"run": run_view, "rows": rows_view}, ensure_ascii=False)
    assert "TOP-SECRET" not in encoded
    assert "account_assessment" not in encoded
    assert "execution_ready" in encoded
    assert run_view["current_execution_ready"] is False
    assert rows_view["current_execution_ready"] is False

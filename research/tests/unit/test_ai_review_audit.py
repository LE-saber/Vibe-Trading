"""Synthetic, read-only checks for the P10F AI review audit."""
import copy
import json
from pathlib import Path
import runpy

import pytest

from competition_core.pipeline.artifacts import atomic_json, content_hash, file_hash
from competition_core.pipeline.persistence import PipelineRepository

FIXTURES = runpy.run_path(str(Path(__file__).resolve().parents[1] / "pipeline_fixtures.py"))
context, small_config = FIXTURES["context"], FIXTURES["small_config"]


MODULE = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/audit-ai-reviews.py"))
audit_run = MODULE["audit_run"]
extract_conclusions = MODULE["extract_conclusions"]


def _seal(path, value):
    atomic_json(path, value | {"integrity_hash": content_hash(value)}, immutable=True)


def _make_run(tmp_path, failed=(), changed_score=False, count=12):
    root = tmp_path / "runs"
    repo = PipelineRepository(root, "completedS5")
    cfg, ctx = small_config(), context()
    manifest = tmp_path / "manifest.json"
    atomic_json(manifest, {"fixture": True})
    repo.initialize({"context": ctx.to_dict(), "snapshot_id": "fixture-snapshot", "config": cfg.to_dict(),
                     "config_hash": cfg.config_hash, "manifest_path": str(manifest), "manifest_hash": file_hash(manifest)})
    symbols = [f"600{index:03d}.SH" for index in range(1, count + 1)]
    rows = [{"symbol": symbol, "selected": True, "quant_score": 80 - index,
             "selection_score": 70 - index, "s1_quant_score": 75 - index,
             "component_scores": {"trend": 60 + index}, "keep_reasons": [], "reject_reasons": []}
            for index, symbol in enumerate(symbols)]
    for stage in ("S0", "S1", "S2", "S3", "S4"):
        repo.start_stage(stage)
        repo.commit_stage(stage, {"stage": stage, "status": "SUCCESS", "input_symbols": symbols,
                                  "output_symbols": symbols, "rows": copy.deepcopy(rows), "summary": {"llm_calls": 0, "agent_tool_calls": 0}},
                         metadata=ctx.to_dict() | {"snapshot_id": "fixture-snapshot", "config_hash": cfg.config_hash}, elapsed=0)
    runtime = {"provider": "fixture", "model": "synthetic-model"}
    review_inputs = {"context": ctx.to_dict(), "config_hash": cfg.config_hash, "runtime": runtime,
                     "stage_hashes": {stage: repo.load_stage(stage)["artifact_hash"] for stage in ("S0", "S1", "S2", "S3", "S4")},
                     "code_hashes": {"scripts/audit-ai-reviews.py": file_hash(Path(__file__).resolve().parents[2] / "scripts/audit-ai-reviews.py")}}
    atomic_json(repo.directory / "review-inputs.json", review_inputs, immutable=True)
    s5_rows = []
    for index, symbol in enumerate(symbols):
        fields = {"technical.momentum": 0.5}
        packet = {"schema_version": "p10.research_packet.v1", "symbol": symbol, "fields": fields,
                  "metadata": {"original_scores": {"quant_score": 80 - index, "selection_score": 70 - index,
                                                     "s1_quant_score": 75 - index, "component_scores": {"trend": 60 + index}}},
                  "system_prompt": "s" * 2500, "user_prompt": "u", "packet_chars": 2501}
        packet["packet_hash"] = content_hash(packet)
        atomic_json(repo.directory / "packets" / f"{symbol}.json", packet, immutable=True)
        review = {"symbol": symbol, "verdict": "KEEP", "strengths": ["动量支持观察[technical.momentum]"],
                  "risks": [], "evidence_conflicts": [], "missing_critical_info": [],
                  "summary": "保留研究资格[technical.momentum]"}
        attempt = {"attempt": 1, "packet_hash": packet["packet_hash"], "status": "FAILED" if symbol in failed else "SUCCESS",
                   "error": {"code": "TIMEOUT", "message": "fixture"} if symbol in failed else None,
                   "response_hash": None, "llm_calls_confirmed": 0 if symbol in failed else 1,
                   "llm_call_count_unknown": symbol in failed}
        if symbol not in failed:
            payload = {"ok": True, "runtime": runtime, "packet_hash": packet["packet_hash"], "tool_calls": [],
                       "invalid_tool_calls": [], "response_model": "synthetic-model", "usage": {"input_tokens": 8, "output_tokens": 7},
                       "llm_calls": 1, "content": json.dumps(review, ensure_ascii=False)}
            response = repo.directory / "reviews" / symbol / "attempt-1" / "response.json"
            atomic_json(response, payload, immutable=True)
            attempt["response_hash"] = file_hash(response)
        result_path = repo.directory / "reviews" / symbol / "attempt-1" / "result.json"
        _seal(result_path, attempt)
        record = {"input_hash": content_hash(review_inputs), "packet_hash": packet["packet_hash"], "symbol": symbol,
                  "status": attempt["status"], "review": None if symbol in failed else review,
                  "successful_attempt": None if symbol in failed else 1, "attempts": [attempt]}
        _seal(repo.directory / "reviews" / symbol / "checkpoint.json", record)
        row = copy.deepcopy(rows[index])
        if changed_score and index == 0:
            row["quant_score"] = 999
        row.update(ai_review=None if symbol in failed else review, ai_status=attempt["status"],
                   ai_verdict="REVIEW" if symbol in failed else "KEEP", ai_eligible=False if symbol in failed else True,
                   review_origin="system_failure" if symbol in failed else "model", packet_hash=packet["packet_hash"])
        s5_rows.append(row)
    repo.start_stage("S5")
    repo.commit_stage("S5", {"stage": "S5", "status": "PARTIAL" if failed else "SUCCESS", "input_symbols": symbols,
                              "output_symbols": symbols, "rows": s5_rows,
                              "summary": {"llm_calls_confirmed": len(symbols) - len(failed), "llm_calls_unknown_attempts": len(failed),
                                          "attempts": len(symbols), "agent_tool_calls": 0}},
                     metadata=ctx.to_dict() | {"snapshot_id": "fixture-snapshot", "config_hash": cfg.config_hash}, elapsed=0)
    return repo, symbols


def test_failed_candidate_keeps_full_success_receipts(tmp_path):
    repo, symbols = _make_run(tmp_path / "failed", failed={"600003.SH"})
    report = audit_run(repo.directory)
    assert report["structural_status"] == "NOT_ACCEPTED"
    assert report["candidate_count"] == 12 and len(report["candidates"]) == 12
    failed = next(item for item in report["candidates"] if item["symbol"] == "600003.SH")
    good = next(item for item in report["candidates"] if item["symbol"] == "600001.SH")
    assert failed["status"] == "FAILED" and failed["verdict"] == "REVIEW" and not failed["ai_eligible"]
    assert good["checks"]["successful_raw_response_strictly_valid"] and good["conclusions"]


def test_tampered_raw_response_is_reported_without_hiding_other_reviews(tmp_path):
    repo, symbols = _make_run(tmp_path)
    response = repo.directory / "reviews" / symbols[0] / "attempt-1" / "response.json"
    atomic_json(response, {"tampered": True})
    report = audit_run(repo.directory)
    assert report["structural_status"] == "NOT_ACCEPTED"
    assert any(issue["code"] == "RAW_RESPONSE_HASH_MISMATCH" for issue in report["issues"])
    assert next(item for item in report["candidates"] if item["symbol"] == symbols[1])["conclusions"]


def test_field_prefix_and_comma_citations_return_values():
    receipt = extract_conclusions({"summary": "综合复核[fields.a, fields.b]"}, {"a": 1, "b": None})
    assert [item["value"] for item in receipt[0]["citations"]] == [1, None]
    assert all(item["found"] for item in receipt[0]["citations"])


def test_score_change_is_not_accepted(tmp_path):
    repo, _ = _make_run(tmp_path, changed_score=True)
    report = audit_run(repo.directory)
    assert report["structural_status"] == "NOT_ACCEPTED"
    assert not report["checks"]["scores_preserved"]
    assert any(issue["code"] == "SCORES_CHANGED" for issue in report["issues"])


def test_candidate_count_lower_bound_ten_is_allowed(tmp_path):
    repo, _ = _make_run(tmp_path, count=10)
    report = audit_run(repo.directory)
    assert report["checks"]["candidate_count_10_15"]

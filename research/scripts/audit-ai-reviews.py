"""Read-only structural audit for a committed S5 AI-review batch."""
from __future__ import annotations
import argparse
import copy
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from competition_core.pipeline.artifacts import atomic_bytes, atomic_json, content_hash, file_hash, read_json  # noqa: E402
from competition_core.pipeline.errors import PipelineError  # noqa: E402
from competition_core.pipeline.persistence import PipelineRepository  # noqa: E402
from competition_core.pipeline.reviews import _read_sealed, _validate_payload  # noqa: E402

STAGES = ("S0", "S1", "S2", "S3", "S4", "S5")
SCREENING_STAGES = STAGES[:-1]
REVIEW_FIELDS = ("strengths", "risks", "evidence_conflicts", "missing_critical_info", "summary")
CITATION = re.compile(r"\[([^\]]*)\]")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}\Z")

def _issue(issues, code, message, **details):
    item = {"code": code, "message": message}
    if details:
        item["details"] = details
    issues.append(item)

def _error(exc):
    return ({"code": exc.code, "message": str(exc), "details": exc.details}
            if isinstance(exc, PipelineError) else {"code": type(exc).__name__, "message": str(exc)})

def _repository(root: Path, run_id: str) -> PipelineRepository:
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise PipelineError("INVALID_ARGUMENTS", "invalid pipeline run_id")
    root, directory = root.resolve(), (root / run_id).resolve()
    if directory.parent != root or not directory.is_dir() or not (directory / "run.json").is_file():
        raise PipelineError("NOT_READY", "pipeline run directory/state is missing", run_id=run_id)
    return PipelineRepository(root, run_id)

def semantic_content(stage: dict[str, Any]) -> dict[str, Any]:
    """Same semantic projection as persistence.load_stage, kept local for this read-only audit."""
    return {key: copy.deepcopy(stage[key]) for key in stage["semantic_keys"]}

def _load(repo, names, issues):
    stages, verified = {}, {}
    for name in names:
        try:
            stage = repo.load_stage(name)
            if stage is None:
                raise PipelineError("NOT_READY", "stage artifact is missing", stage=name)
            stages[name] = stage | {"_artifact_file_hash": file_hash(repo.directory / f"{name}.json")}
            verified[name] = True
        except Exception as exc:
            verified[name] = False
            _issue(issues, "STAGE_INVALID", f"{name} failed load_stage verification", stage=name, error=_error(exc))
            try:
                raw = read_json(repo.directory / f"{name}.json")
                if isinstance(raw, dict):
                    stages[name] = raw | {"_unverified": True}
            except Exception:
                pass
    return stages, verified

def extract_conclusions(review: Any, fields: Any) -> list[dict[str, Any]]:
    """Return conclusion text plus values for [field] and [fields.field] citations."""
    if not isinstance(review, dict) or not isinstance(fields, dict):
        return []
    result = []
    for section in REVIEW_FIELDS:
        value = review.get(section)
        texts = [value] if section == "summary" and isinstance(value, str) else (
            value if section != "summary" and isinstance(value, list) else [])
        for text in texts:
            if not isinstance(text, str):
                continue
            citations = []
            for group in CITATION.findall(text):
                for reference in (part.strip() for part in group.split(",")):
                    if not reference:
                        continue
                    field = reference[7:] if reference.startswith("fields.") else reference
                    citations.append({"reference": reference, "field": field, "found": field in fields,
                                      "value": copy.deepcopy(fields[field]) if field in fields else None})
            result.append({"section": section, "text": text, "citations": citations})
    return result

def _scores(row):
    if not isinstance(row, dict):
        return {}
    containers = {"component_scores", "effective_weights", "s3_components", "s3_effective_weights"}
    names = {"quant_score", "s1_quant_score", "selection_score", "overheat_score", "trend_score",
             "momentum_score", "relative_strength_score", "liquidity_score", "volatility_quality_score",
             "data_quality_score"}
    return {key: copy.deepcopy(value) for key, value in row.items()
            if key in names or key in containers or "score" in key.lower()}

def _model_ok(model, runtime):
    configured, provider = runtime.get("model"), runtime.get("provider")
    if not isinstance(model, str) or not isinstance(configured, str):
        return False
    allowed = {configured, configured.split("/")[-1]}
    if isinstance(provider, str):
        allowed.add(f"{provider}/{configured}")
    return model in allowed

def _usage_totals(attempts):
    total = {}
    for attempt in attempts:
        usage = attempt.get("usage", {})
        for key in ("input_tokens", "output_tokens", "total_tokens"):
            if isinstance(usage.get(key), int) and not isinstance(usage[key], bool):
                total[key] = total.get(key, 0) + usage[key]
    return total

def _candidate(directory, symbol, s4_row, s5_row, review_inputs, issues):
    runtime = review_inputs.get("runtime", {})
    packet_path = directory / "packets" / f"{symbol}.json"
    packet = None
    packet_ok = chars_ok = False
    try:
        packet = read_json(packet_path)
        if not isinstance(packet, dict):
            raise PipelineError("STALE_DATA", "packet is not an object", symbol=symbol)
        packet_ok = isinstance(packet.get("packet_hash"), str) and content_hash(
            {key: value for key, value in packet.items() if key != "packet_hash"}) == packet["packet_hash"]
        chars_ok = (isinstance(packet.get("system_prompt"), str) and isinstance(packet.get("user_prompt"), str)
                    and packet.get("packet_chars") == len(packet["system_prompt"]) + len(packet["user_prompt"]))
        if not packet_ok:
            _issue(issues, "PACKET_HASH_MISMATCH", "packet hash mismatch", symbol=symbol)
        if not chars_ok:
            _issue(issues, "PACKET_CHAR_COUNT_MISMATCH", "packet character count mismatch", symbol=symbol)
    except Exception as exc:
        _issue(issues, "PACKET_INVALID", "packet could not be read", symbol=symbol, error=_error(exc))
    packet_hash = packet.get("packet_hash") if isinstance(packet, dict) else None

    checkpoint = None
    checkpoint_ok = False
    checkpoint_path = directory / "reviews" / symbol / "checkpoint.json"
    try:
        checkpoint = _read_sealed(checkpoint_path)
        checkpoint_ok = (checkpoint.get("symbol") == symbol and checkpoint.get("packet_hash") == packet_hash
                         and checkpoint.get("status") in {"SUCCESS", "FAILED"})
        if not checkpoint_ok:
            _issue(issues, "CHECKPOINT_IDENTITY_INVALID", "checkpoint identity/status is invalid", symbol=symbol)
    except Exception as exc:
        _issue(issues, "CHECKPOINT_INVALID", "checkpoint could not be verified", symbol=symbol, error=_error(exc))

    stored_attempts = checkpoint.get("attempts", []) if checkpoint else []
    if not isinstance(stored_attempts, list):
        stored_attempts = []
        _issue(issues, "ATTEMPTS_INVALID", "checkpoint attempts are not a list", symbol=symbol)
    successful_number = checkpoint.get("successful_attempt") if checkpoint else None
    receipts, failed, models, payloads = [], [], [], {}
    strict_ok = matches_s5 = True
    success_path = success_hash = None
    for stored in stored_attempts:
        number = stored.get("attempt") if isinstance(stored, dict) else None
        attempt_dir = directory / "reviews" / symbol / f"attempt-{number}"
        result_path, response_path = attempt_dir / "result.json", attempt_dir / "response.json"
        result, result_ok = None, False
        try:
            result = _read_sealed(result_path)
            result_ok = result == stored
            if not result_ok:
                _issue(issues, "ATTEMPT_RECORD_MISMATCH", "sealed attempt differs from checkpoint", symbol=symbol, attempt=number)
        except Exception as exc:
            _issue(issues, "ATTEMPT_RESULT_INVALID", "attempt result could not be verified", symbol=symbol,
                   attempt=number, error=_error(exc))
        exists = response_path.is_file()
        actual_hash = file_hash(response_path) if exists else None
        recorded_hash = stored.get("response_hash") if isinstance(stored, dict) else None
        hash_ok = (recorded_hash is None and not exists) or (isinstance(recorded_hash, str) and actual_hash == recorded_hash)
        if not hash_ok:
            _issue(issues, "RAW_RESPONSE_HASH_MISMATCH", "raw response hash differs from attempt", symbol=symbol, attempt=number)
        payload = None
        if exists:
            try:
                payload = read_json(response_path)
                if not isinstance(payload, dict):
                    raise PipelineError("STALE_DATA", "raw response is not an object")
                payloads[number] = payload
                model = payload.get("response_model")
                if isinstance(model, str):
                    models.append(model)
                if payload.get("tool_calls") or payload.get("invalid_tool_calls"):
                    _issue(issues, "TOOL_CALLS_OBSERVED", "raw response contains tool calls", symbol=symbol, attempt=number)
                if payload.get("ok") is True and payload.get("runtime") != runtime:
                    _issue(issues, "RUNTIME_MISMATCH", "raw response runtime differs from review-inputs", symbol=symbol, attempt=number)
                if model is not None and not _model_ok(model, runtime):
                    _issue(issues, "MODEL_MISMATCH", "raw response model differs from configured model", symbol=symbol, attempt=number)
            except Exception as exc:
                _issue(issues, "RAW_RESPONSE_INVALID", "raw response could not be read", symbol=symbol,
                       attempt=number, error=_error(exc))
        status = (result or stored).get("status") if isinstance(result or stored, dict) else None
        if status == "FAILED":
            failed.append({"attempt": number, "error": copy.deepcopy((result or stored).get("error")),
                           "response_path": response_path.relative_to(directory).as_posix() if exists else None,
                           "response_hash": actual_hash})
        if number == successful_number:
            success_path = response_path.relative_to(directory).as_posix() if exists else None
            success_hash = actual_hash
            if payload is None or not isinstance(packet, dict):
                strict_ok = matches_s5 = False
                _issue(issues, "SUCCESS_RESPONSE_MISSING", "successful checkpoint has no durable raw response", symbol=symbol)
            else:
                try:
                    validated = _validate_payload(payload, packet, runtime)
                    strict_ok = checkpoint is not None and checkpoint.get("status") == "SUCCESS" and validated == checkpoint.get("review")
                    matches_s5 = s5_row is not None and validated == s5_row.get("ai_review")
                    if not strict_ok:
                        _issue(issues, "CHECKPOINT_REVIEW_INVALID", "successful response differs from checkpoint review", symbol=symbol)
                    if not matches_s5:
                        _issue(issues, "REVIEW_S5_MISMATCH", "strict raw review differs from S5 row", symbol=symbol)
                except Exception as exc:
                    strict_ok = matches_s5 = False
                    _issue(issues, "REVIEW_SCHEMA_INVALID", "successful raw response failed validate_review", symbol=symbol,
                           error=_error(exc))
        receipts.append({"attempt": number, "status": status, "result_path": result_path.relative_to(directory).as_posix(),
                         "result_verified": result_ok, "response_path": response_path.relative_to(directory).as_posix() if exists else None,
                         "response_hash": actual_hash, "recorded_response_hash": recorded_hash, "response_hash_match": hash_ok,
                         "model": payload.get("response_model") if isinstance(payload, dict) else None,
                         "usage": copy.deepcopy(payload.get("usage", {})) if isinstance(payload, dict) and isinstance(payload.get("usage", {}), dict) else {},
                         "tool_calls_empty": not (payload.get("tool_calls") or payload.get("invalid_tool_calls")) if isinstance(payload, dict) else None})

    status = checkpoint.get("status") if checkpoint else None
    row_status = s5_row.get("ai_status") if isinstance(s5_row, dict) else None
    row_review = s5_row.get("ai_review") if isinstance(s5_row, dict) else None
    scores_ok = s4_row is not None and s5_row is not None and _scores(s4_row) == _scores(s5_row)
    if not scores_ok:
        _issue(issues, "SCORES_CHANGED", "S5 score values differ from S4", symbol=symbol)
    original = packet.get("metadata", {}).get("original_scores", {}) if isinstance(packet, dict) else {}
    packet_scores_ok = isinstance(original, dict) and all(s4_row.get(key) == value for key, value in original.items()
                                                           if isinstance(s4_row, dict) and key in s4_row)
    if not packet_scores_ok:
        _issue(issues, "PACKET_SCORES_CHANGED", "packet original scores differ from S4", symbol=symbol)
    row_packet_ok = isinstance(s5_row, dict) and s5_row.get("packet_hash") == packet_hash
    if not row_packet_ok:
        _issue(issues, "S5_PACKET_HASH_MISMATCH", "S5 row packet hash differs from packet", symbol=symbol)
    failed_not_promoted = status != "FAILED" or (isinstance(s5_row, dict) and row_status == "FAILED" and row_review is None
                                                  and s5_row.get("ai_verdict") == "REVIEW" and s5_row.get("ai_eligible") is False)
    if not failed_not_promoted:
        _issue(issues, "FAILED_REVIEW_PROMOTED", "failed candidate was promoted", symbol=symbol)
    conclusions = extract_conclusions(row_review, packet.get("fields", {}) if isinstance(packet, dict) else {})
    receipt_ok = (all(item["citations"] and all(citation["found"] for citation in item["citations"])
                      for item in conclusions) if row_review is not None else status == "FAILED")
    if not receipt_ok:
        _issue(issues, "CITATION_RECEIPT_INCOMPLETE", "review citation receipt is incomplete", symbol=symbol)
    return {"symbol": symbol, "status": status, "ai_status": row_status,
            "verdict": s5_row.get("ai_verdict") if isinstance(s5_row, dict) else None,
            "ai_eligible": s5_row.get("ai_eligible") if isinstance(s5_row, dict) else None,
            "packet_path": packet_path.relative_to(directory).as_posix(), "packet_hash": packet_hash,
            "packet_chars": packet.get("packet_chars") if isinstance(packet, dict) else None,
            "raw_response_path": success_path, "raw_response_hash": success_hash,
            "raw_response_paths": [item["response_path"] for item in receipts if item["response_path"]],
            "raw_response_hashes": [item["response_hash"] for item in receipts if item["response_hash"]],
            "model": models[0] if models else runtime.get("model"), "configured_model": runtime.get("model"),
            "observed_models": sorted(set(models)), "failed_attempt_count": len(failed), "failed_attempts": failed,
            "attempts": receipts,
            "usage": copy.deepcopy(payloads.get(successful_number, {}).get("usage", {})) if isinstance(payloads.get(successful_number), dict) else {},
            "usage_totals": _usage_totals(receipts), "conclusions": conclusions,
            "checks": {"checkpoint_verified": checkpoint_ok, "packet_verified": packet_ok and chars_ok,
                        "s5_packet_hash_match": row_packet_ok, "scores_preserved": scores_ok,
                        "packet_original_scores_match": packet_scores_ok,
                        "successful_raw_response_strictly_valid": strict_ok if status == "SUCCESS" else None,
                        "successful_review_matches_s5": matches_s5 if status == "SUCCESS" else None,
                        "successful_model_observed": bool(payloads.get(successful_number, {}).get("response_model")) if status == "SUCCESS" else True,
                        "failed_candidate_not_promoted": failed_not_promoted, "citation_receipt_complete": receipt_ok,
                        "models_match": all(_model_ok(model, runtime) for model in models),
                        "tool_calls_empty": all(item["tool_calls_empty"] is True for item in receipts if item["tool_calls_empty"] is not None)}}

def _compare(repo, stages, compare_id, issues):
    if compare_id is None:
        return {"requested": False, "semantic_match": None, "stages": []}
    path = Path(compare_id)
    if not path.is_dir():
        path = repo.directory.parent / str(compare_id)
    try:
        other = _repository(path.parent, path.name)
        other_stages, _ = _load(other, SCREENING_STAGES, issues)
        left, right = read_json(repo.directory / "inputs.json"), read_json(other.directory / "inputs.json")
    except Exception as exc:
        _issue(issues, "COMPARE_RUN_INVALID", "comparison run could not be read", run_id=str(compare_id), error=_error(exc))
        return {"requested": True, "semantic_match": False, "run_id": str(compare_id), "stages": []}
    same_inputs = all(left.get(key) == right.get(key) for key in ("snapshot_id", "config_hash", "context", "manifest_hash"))
    if not same_inputs:
        _issue(issues, "COMPARE_INPUTS_DIFFER", "comparison screening inputs differ")
    rows, matched = [], same_inputs
    for name in SCREENING_STAGES:
        a, b = stages.get(name), other_stages.get(name)
        equal = False
        if a is not None and b is not None:
            try:
                equal = semantic_content(a) == semantic_content(b)
            except Exception as exc:
                _issue(issues, "SEMANTIC_CONTENT_INVALID", "semantic projection failed", stage=name, error=_error(exc))
        matched &= equal
        if not equal:
            _issue(issues, "SCREENING_SEMANTIC_DIFFER", "complete semantic content differs", stage=name)
        rows.append({"stage": name, "semantic_content_equal": equal,
                     "run_semantic_hash": a.get("semantic_hash") if a else None,
                     "compare_semantic_hash": b.get("semantic_hash") if b else None})
    return {"requested": True, "run_id": other.run_id, "same_inputs": same_inputs,
            "semantic_match": matched, "stages": rows}

def _code_hashes(review_inputs, issues):
    expected = review_inputs.get("code_hashes", {})
    if not isinstance(expected, dict) or not expected:
        _issue(issues, "CODE_HASHES_MISSING", "review-inputs has no frozen code hashes")
        return {"expected": {}, "observed": {}, "match": False}
    observed = {}
    for name, wanted in expected.items():
        path = None
        if isinstance(name, str):
            base = (ROOT.parent / "Vibe-Trading").resolve() if name.startswith("vibe/") else ROOT.resolve()
            relative = Path(name[5:] if name.startswith("vibe/") else name)
            target = (base / relative).resolve()
            if not relative.is_absolute() and target.is_relative_to(base):
                path = target
        actual = file_hash(path) if path is not None and path.is_file() else None
        observed[name] = actual
        if actual != wanted:
            _issue(issues, "CODE_HASH_MISMATCH" if actual is not None else "CODE_HASH_UNAVAILABLE",
                   "frozen review code hash differs from current readable source", path=name, expected=wanted, observed=actual)
    return {"expected": copy.deepcopy(expected), "observed": observed,
            "match": all(observed[name] == wanted for name, wanted in expected.items())}

def audit_run(directory: Path, compare_screening_run: str | Path | None = None) -> dict[str, Any]:
    directory = Path(directory).resolve()
    repo = _repository(directory.parent, directory.name)
    issues = []
    stages, stage_verified = _load(repo, STAGES, issues)
    try:
        review_inputs = read_json(directory / "review-inputs.json")
        if not isinstance(review_inputs, dict) or not isinstance(review_inputs.get("runtime"), dict):
            raise PipelineError("STALE_DATA", "review-inputs runtime is invalid")
    except Exception as exc:
        review_inputs = {}
        _issue(issues, "REVIEW_INPUTS_INVALID", "review-inputs could not be read", error=_error(exc))
    code_hashes = _code_hashes(review_inputs, issues)
    s4, s5 = stages.get("S4"), stages.get("S5")
    s4_symbols = s4.get("output_symbols", []) if isinstance(s4, dict) else []
    s5_inputs = s5.get("input_symbols", []) if isinstance(s5, dict) else []
    s5_outputs = s5.get("output_symbols", []) if isinstance(s5, dict) else []
    s4_rows = {row.get("symbol"): row for row in s4.get("rows", [])} if isinstance(s4, dict) else {}
    s5_rows = {row.get("symbol"): row for row in s5.get("rows", [])} if isinstance(s5, dict) else {}
    if not 10 <= len(s4_symbols) <= 15:
        _issue(issues, "CANDIDATE_COUNT_INVALID", "S4 candidate count is outside the ten-to-fifteen range", count=len(s4_symbols))
    input_match, output_match = isinstance(s4, dict) and s5_inputs == s4_symbols, isinstance(s4, dict) and s5_outputs == s4_symbols
    if not input_match:
        _issue(issues, "S5_INPUTS_DIFFER", "S5 input_symbols differ from S4 output_symbols")
    if not output_match:
        _issue(issues, "S5_OUTPUTS_DIFFER", "S5 output_symbols differ from S4 output_symbols")
    candidates = [_candidate(directory, symbol, s4_rows.get(symbol), s5_rows.get(symbol), review_inputs, issues) for symbol in s4_symbols]
    per = [item["checks"] for item in candidates]
    summary = s5.get("summary", {}) if isinstance(s5, dict) else {}
    comparison = _compare(repo, stages, compare_screening_run, issues)
    checks = {
        "stage_artifacts_hash_verified": all(stage_verified.get(name) for name in STAGES),
        "code_hashes_match": code_hashes["match"], "review_input_runtime_present": bool(review_inputs.get("runtime")),
        "review_input_stage_hashes_match": bool(review_inputs.get("stage_hashes")) and all(
            stages.get(name, {}).get("artifact_hash") == review_inputs.get("stage_hashes", {}).get(name) for name in SCREENING_STAGES),
        "s5_status_success": isinstance(s5, dict) and s5.get("status") == "SUCCESS", "candidate_count_10_15": 10 <= len(s4_symbols) <= 15,
        "s5_input_matches_s4": input_match, "s5_output_matches_s4": output_match,
        "packets_verified": bool(per) and all(item["packet_verified"] for item in per),
        "scores_preserved": bool(per) and all(item["scores_preserved"] for item in per),
        "packet_original_scores_match": bool(per) and all(item["packet_original_scores_match"] for item in per),
        "no_tool_calls": isinstance(s5, dict) and summary.get("agent_tool_calls") == 0 and all(item["tool_calls_empty"] for item in per),
        "no_different_models": bool(review_inputs.get("runtime")) and all(item["models_match"] and item["successful_model_observed"] for item in per),
        "all_checkpoints_verified": bool(per) and all(item["checkpoint_verified"] for item in per),
        "successful_raw_responses_strictly_valid": bool(candidates) and all(item["status"] != "SUCCESS" or item["checks"]["successful_raw_response_strictly_valid"] for item in candidates),
        "successful_review_content_matches_s5": bool(candidates) and all(item["status"] != "SUCCESS" or item["checks"]["successful_review_matches_s5"] for item in candidates),
        "failed_candidates_not_promoted": bool(per) and all(item["failed_candidate_not_promoted"] for item in per),
        "citation_field_receipts_complete": bool(per) and all(item["citation_receipt_complete"] for item in per),
        "screening_semantic_content_match": comparison["semantic_match"] is not False,
    }
    status = "PASS" if all(checks.values()) and not issues else "NOT_ACCEPTED"
    report = {"schema_version": "p10f.ai_review.audit.v1", "run_id": repo.run_id, "structural_status": status,
              "production_pipeline_status": "NOT_ACCEPTED", "semantic_review_status": "REQUIRES_HUMAN_REVIEW",
              "checks": checks, "issues": issues, "code_hashes": code_hashes,
              "stages": [{"stage": name, "load_stage_verified": bool(stage_verified.get(name)), "path": f"{name}.json",
                          "artifact_file_hash": stages.get(name, {}).get("_artifact_file_hash"),
                          "artifact_hash": stages.get(name, {}).get("artifact_hash"), "semantic_hash": stages.get(name, {}).get("semantic_hash"),
                          "status": stages.get(name, {}).get("status")} for name in STAGES],
              "review_inputs": {"path": "review-inputs.json", "runtime": copy.deepcopy(review_inputs.get("runtime")),
                                "config_hash": review_inputs.get("config_hash"), "stage_hashes": copy.deepcopy(review_inputs.get("stage_hashes", {}))},
              "s5": {"status": s5.get("status") if isinstance(s5, dict) else None, "input_symbols": s5_inputs,
                     "output_symbols": s5_outputs, "summary": copy.deepcopy(summary)},
              "candidate_count": len(candidates), "candidates": candidates, "screening_comparison": comparison,
              "caveats": ["Structural checks do not prove natural-language conclusions are true.",
                          "Citation receipts expose packet field values for human one-by-one review.",
                          "No API/model call or existing run artifact rewrite is performed.",
                          "Failed candidates are not converted into passing reviews."]}
    report["report_id"] = content_hash(report)
    return report

def _markdown(report):
    lines = [f"# P10F AI review audit: {report['structural_status']}", "",
             "Production pipeline: NOT_ACCEPTED; semantic review: REQUIRES_HUMAN_REVIEW.", "",
             f"Run: `{report['run_id']}`", f"Checks: `{sum(report['checks'].values())}/{len(report['checks'])}` passed.", "",
             "| Symbol | Status | Verdict | Packet chars | Raw response | Model | Failed | Usage |", "|---|---|---|---:|---|---|---:|---|"]
    for item in report["candidates"]:
        usage = json.dumps(item.get("usage", {}), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        lines.append(f"| {item['symbol']} | {item.get('status')} | {item.get('verdict')} | {item.get('packet_chars')} | `{item.get('raw_response_path')}` | {item.get('model')} | {item.get('failed_attempt_count', 0)} | `{usage}` |")
    lines += ["", "S0–S5 were read through load_stage; detailed hashes/issues/receipts are in the sibling JSON.",
              "S0–S4 complete semantic content is compared when a screening run is supplied.",
              "Natural-language truth remains a human review responsibility."]
    return "\n".join(lines) + "\n"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--compare-screening-run")
    parser.add_argument("--runs-root", type=Path, default=ROOT / "runs")
    args = parser.parse_args()
    try:
        directory = (args.runs_root / args.run_id).resolve()
        report = audit_run(directory, args.compare_screening_run)
        atomic_json(directory / "ai-review-audit.json", report, immutable=True)
        atomic_bytes(directory / "ai-review-audit.md", _markdown(report).encode("utf-8"), immutable=True)
        print({key: report[key] for key in ("run_id", "structural_status", "production_pipeline_status", "semantic_review_status")}, flush=True)
        raise SystemExit(0 if report["structural_status"] == "PASS" else 1)
    except SystemExit:
        raise
    except Exception as exc:
        print({"status": "NOT_ACCEPTED", "error": _error(exc)}, flush=True)
        raise SystemExit(1)

if __name__ == "__main__":
    main()

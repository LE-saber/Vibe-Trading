"""Synthetic review retries, durable raw responses and checkpoint integrity."""

import copy
import json

import pytest

from competition_core.pipeline.artifacts import atomic_json
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.reviews import _review_one, _retry_feedback

RUNTIME = {"provider": "fixture", "model": "synthetic-model"}
PACKET = {"symbol": "300750.SZ", "packet_hash": "fixture-packet", "fields": {"technical.momentum": .1, "margin.net": None}}
POLICY = {"max_attempts": 2, "timeout_seconds": 30, "packet_max_chars": 4000, "reasoning_effort": "configured", "max_output_tokens": 4096}


def answer(packet=PACKET):
    review = {"symbol": packet["symbol"], "verdict": "KEEP", "strengths": ["动量为正[technical.momentum]"], "risks": [],
              "evidence_conflicts": [], "missing_critical_info": [], "summary": "保留研究资格[technical.momentum]"}
    return {"ok": True, "runtime": RUNTIME, "packet_hash": packet["packet_hash"], "tool_calls": [],
            "response_model": "synthetic-model", "llm_calls": 1, "content": json.dumps(review)}


def run(root, invoker, packet=PACKET):
    return _review_one(packet=packet, runtime=RUNTIME, root=root, identity_hash="fixture-input", policy=POLICY,
                       vibe_root=root, invoker=invoker)


def test_success_reuses_checkpoint_without_model_call(tmp_path):
    record, reused = run(tmp_path, lambda **kw: answer())
    assert not reused and record["status"] == "SUCCESS" and len(record["attempts"]) == 1
    again, reused = run(tmp_path, lambda **kw: pytest.fail("must not call model again"))
    assert reused and again == record


def test_invalid_first_response_retries_with_same_prompt_budget(tmp_path):
    seen = []
    feedback = []
    def invoke(**kw):
        seen.append(copy.deepcopy(kw["packet"]))
        feedback.append(kw["retry_feedback"])
        payload = answer()
        if len(seen) == 1:
            payload["content"] = "not JSON"
        return payload
    record, _ = run(tmp_path, invoke)
    assert record["status"] == "SUCCESS" and record["successful_attempt"] == 2
    assert seen == [PACKET, PACKET]
    assert sum(attempt["llm_calls_confirmed"] for attempt in record["attempts"]) == 2
    assert feedback[0] == "" and 0 < len(feedback[1]) <= 128


def test_retry_feedback_uses_only_validator_field_not_provider_or_model_text():
    note = _retry_feedback([{"error": {"details": {"field": "summary"}, "message": "private-ignored"}}])
    assert "summary" in note and "private-ignored" not in note and len(note) <= 128
    note = _retry_feedback([{"error": {"details": {"field": "untrusted-private-field"}}}])
    assert "untrusted-private-field" not in note and len(note) <= 128


def test_retry_feedback_makes_citation_and_numeric_constraints_explicit():
    note = _retry_feedback([{"error": {"details": {"field": "summary"},
                                         "message": "review claim must cite supplied fields"}}])
    assert "每条陈述都必须有引用" in note
    assert "fields已有键" in note and "不得无引用" in note
    assert len(note) <= 128

    note = _retry_feedback([{"error": {"details": {"field": "risks"},
                                         "message": "review prose must be qualitative, without forecasts or quantities"}}])
    assert "阿拉伯/中文数字" in note
    assert "为/高于/低于/超过/不足/达到/百分之+数词" in note
    assert "引用键保持原样" in note and len(note) <= 128
    assert len(_retry_feedback([{"error": {"details": {"field": "missing_critical_info"},
                                               "message": "review prose must be qualitative, without forecasts or quantities"}}])) <= 128


def test_exhausted_failure_not_reported_as_model_approval_or_retried_forever(tmp_path):
    calls = []
    def fail(**kw):
        calls.append(1)
        raise RuntimeError("synthetic sensitive provider exception must not be persisted")
    record, _ = run(tmp_path, fail)
    assert record["status"] == "FAILED" and record["review"] is None and len(calls) == 2
    assert record["attempts"][0]["error"] == {"code": "AI_PROVIDER_ERROR", "exception_type": "RuntimeError"}
    run(tmp_path, lambda **kw: pytest.fail("budget exhausted"))
    assert all(attempt["llm_call_count_unknown"] for attempt in record["attempts"])


def test_durable_response_after_interruption_is_recovered_without_repeating_call(tmp_path):
    attempt = tmp_path / "reviews/300750.SZ/attempt-1"
    atomic_json(attempt / "response.json", answer())
    record, _ = run(tmp_path, lambda **kw: pytest.fail("response already durable"))
    assert record["status"] == "SUCCESS" and record["successful_attempt"] == 1


def test_ambiguous_interrupted_attempt_consumes_budget_slot(tmp_path):
    atomic_json(tmp_path / "reviews/300750.SZ/attempt-1/started.json", {"synthetic": True})
    calls = []
    def invoke(**kw):
        calls.append(kw["directory"].name)
        return answer()
    record, _ = run(tmp_path, invoke)
    assert calls == ["attempt-2"] and record["successful_attempt"] == 2
    assert record["attempts"][0]["error"]["code"] == "AI_ATTEMPT_INTERRUPTED"


@pytest.mark.parametrize("fault", ["tools", "model", "packet", "runtime"])
def test_wrong_runtime_tools_or_packet_never_pass(fault, tmp_path):
    payload = answer()
    if fault == "tools":
        payload["tool_calls"] = [{"name": "get_market_data", "args": {}}]
    elif fault == "model":
        payload["response_model"] = "different-model"
    elif fault == "packet":
        payload["packet_hash"] = "different"
    else:
        payload["runtime"] = {"model": "different"}
    record, _ = run(tmp_path, lambda **kw: payload)
    assert record["status"] == "FAILED" and record["review"] is None


def test_tampered_raw_response_fails_closed(tmp_path):
    run(tmp_path, lambda **kw: answer())
    # Deliberate corruption fixture; no production artifacts involved.
    atomic_json(tmp_path / "reviews/300750.SZ/attempt-1/response.json", {"tampered": True})
    with pytest.raises(PipelineError, match="changed"):
        run(tmp_path, lambda **kw: pytest.fail("must not call model"))

"""No external API in these worker-boundary tests."""

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from adapters.vibe import review
from competition_core.pipeline.artifacts import atomic_bytes, atomic_json
from competition_core.pipeline.errors import PipelineError


@pytest.fixture
def vibe(tmp_path):
    root = tmp_path / "vibe"
    atomic_bytes(root / ".venv/Scripts/python.exe", b"synthetic placeholder, never executed")
    (root / "agent").mkdir()
    return root


def test_probe_uses_isolated_bounded_hidden_subprocess(vibe, tmp_path, monkeypatch):
    calls = []
    def run(command, **kw):
        calls.append((command, kw))
        atomic_json(Path(command[command.index("--output") + 1]), {"ok": True, "runtime": {"model": "fixture"}})
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(review.subprocess, "run", run)
    result = review.probe_vibe(directory=tmp_path / "probe", vibe_root=vibe)
    assert result == {"model": "fixture"}
    assert calls[0][0][0] == str(Path(sys.executable).resolve())
    assert calls[0][1]["cwd"] == review.ROOT
    assert calls[0][1]["timeout"] == 30 and calls[0][1]["shell"] is False
    assert calls[0][1]["capture_output"] is True


@pytest.mark.parametrize("fault", ["timeout", "native_error", "provider_error", "missing_response"])
def test_worker_failures_never_leak_captured_provider_text(vibe, tmp_path, monkeypatch, fault):
    private = "SYNTHETIC-SECRET-SHOULD-NOT-LEAK"
    def run(command, **kw):
        if fault == "timeout":
            raise subprocess.TimeoutExpired(command, 30, stderr=private)
        if fault != "missing_response":
            atomic_json(Path(command[command.index("--output") + 1]), {"ok": fault == "native_error",
                        "error": {"code": "AI_PROVIDER_ERROR"}})
        return SimpleNamespace(returncode=int(fault == "provider_error"), stdout="",
                               stderr=("Windows fatal exception:" if fault == "native_error" else "") + private)
    monkeypatch.setattr(review.subprocess, "run", run)
    with pytest.raises(PipelineError) as error:
        review.probe_vibe(directory=tmp_path / "probe", vibe_root=vibe)
    assert private not in str(error.value) and private not in str(error.value.to_dict())


def test_worker_has_no_tool_binding_or_agent_execution_path():
    import ast
    tree = ast.parse(review.WORKER.read_text(encoding="utf-8"))
    calls = [node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)]
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert "invoke_chat" in names
    assert not set(calls) & {"bind_tools", "execute", "ainvoke", "run_agent"}


def test_deepseek_generation_policy_uses_documented_wire_token_key():
    runtime = {"provider": "deepseek", "model": "configured-model", "temperature": 0}
    result = review.effective_runtime(runtime, {"reasoning_effort": "low", "max_output_tokens": 4096})
    assert result["generation_parameters"] == {
        "extra_body": {"max_tokens": 4096}, "response_format": {"type": "json_object"}, "reasoning_effort": "low"}
    assert "generation_parameters" not in runtime


@pytest.mark.parametrize("policy", [{}, {"reasoning_effort": "low"},
    {"reasoning_effort": "low", "max_output_tokens": True},
    {"reasoning_effort": "invalid", "max_output_tokens": 4096}])
def test_legacy_or_invalid_generation_policy_cannot_start_s5(policy):
    with pytest.raises(PipelineError):
        review.effective_runtime({"provider": "deepseek"}, policy)


def test_other_provider_never_silently_inherits_deepseek_policy():
    with pytest.raises(PipelineError):
        review.effective_runtime({"provider": "other"}, {"reasoning_effort": "low", "max_output_tokens": 4096})
    result = review.effective_runtime({"provider": "other"}, {"reasoning_effort": "configured", "max_output_tokens": 4096})
    assert result["generation_parameters"] == {"max_tokens": 4096}


def test_deepseek_non_thinking_is_explicit_and_does_not_change_global_runtime():
    runtime = {"provider": "deepseek", "model": "configured-model", "temperature": 0}
    result = review.effective_runtime(runtime, {"reasoning_effort": "none", "max_output_tokens": 4096})
    assert result["generation_parameters"] == {"extra_body": {"max_tokens": 4096, "thinking": {"type": "disabled"}},
                                              "response_format": {"type": "json_object"}}
    assert "generation_parameters" not in runtime


def test_retry_message_is_separate_from_unchanged_frozen_evidence_and_bounded():
    packet = {"system_prompt": "a" * 1000, "user_prompt": "b" * 2872, "packet_chars": 3872}
    messages = review.review_messages(packet, "c" * 128, 4000)
    assert [len(item["content"]) for item in messages] == [1000, 2872, 128]
    assert len(review.review_messages(packet, "", 4000)) == 2
    for feedback, maximum in (("c" * 129, 4000), ("c" * 128, 3999), (None, 4000), ("", 5000)):
        with pytest.raises(PipelineError):
            review.review_messages(packet, feedback, maximum)

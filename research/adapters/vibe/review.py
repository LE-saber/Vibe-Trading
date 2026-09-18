"""Isolated, bounded access to Vibe's configured LLM. Never an agent/tool loop."""

from pathlib import Path
import os
import subprocess
import sys

from competition_core.pipeline.artifacts import atomic_json, file_hash, read_json
from competition_core.pipeline.errors import PipelineError

ROOT = Path(__file__).resolve().parents[2]
WORKER = ROOT / "scripts/vibe-review-worker.py"


def review_messages(packet: dict, feedback: str, maximum: int) -> list[dict]:
    chars = len(packet["system_prompt"]) + len(packet["user_prompt"])
    if (not isinstance(feedback, str) or len(feedback) > 128
            or chars != packet["packet_chars"] or not 2500 <= chars <= chars + len(feedback) <= maximum < 5000):
        raise PipelineError("INVALID_ARGUMENTS", "review messages exceed the declared prompt budget")
    messages = [{"role": "system", "content": packet["system_prompt"]},
                {"role": "user", "content": packet["user_prompt"]}]
    if feedback:
        messages.append({"role": "user", "content": feedback})
    return messages


def effective_runtime(configured: dict, policy: dict) -> dict:
    if not {"reasoning_effort", "max_output_tokens"} <= set(policy):
        raise PipelineError("NOT_CONFIGURED", "S5 requires explicit versioned reasoning and output-token budgets")
    tokens, effort = policy["max_output_tokens"], policy["reasoning_effort"]
    if (isinstance(tokens, bool) or not isinstance(tokens, int) or not 512 <= tokens <= 16384
            or not isinstance(effort, str) or effort not in {"configured", "none", "low", "high", "max"}):
        raise PipelineError("NOT_CONFIGURED", "AI generation policy is outside supported bounds")
    parameters = {"max_tokens": policy["max_output_tokens"]}
    if configured["provider"] == "deepseek":
        # LangChain renames top-level max_tokens to max_completion_tokens.
        # DeepSeek documents max_tokens; SDK extra_body puts that exact key on wire.
        parameters = {"extra_body": {"max_tokens": policy["max_output_tokens"]}}
        parameters["response_format"] = {"type": "json_object"}
        if policy["reasoning_effort"] == "none":
            parameters["extra_body"]["thinking"] = {"type": "disabled"}
        elif policy["reasoning_effort"] != "configured":
            parameters["reasoning_effort"] = policy["reasoning_effort"]
    elif policy["reasoning_effort"] != "configured":
        raise PipelineError("NOT_CONFIGURED", "explicit reasoning effort is currently supported for the configured DeepSeek path only")
    return configured | {"generation_parameters": parameters}


def _call(request: dict, *, directory: Path, vibe_root: Path, timeout: int) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    request_path, output_path = directory / "request.json", directory / "response.json"
    atomic_json(request_path, request, immutable=True)
    python = Path(sys.executable).resolve()
    worker_hash = file_hash(WORKER)
    try:
        result = subprocess.run([str(python), str(WORKER), "--request", str(request_path.resolve()),
                                 "--output", str(output_path.resolve()), "--vibe-root", str(ROOT)],
                                cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
                                timeout=timeout, shell=False, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    except subprocess.TimeoutExpired as exc:
        raise PipelineError("TIMEOUT", "Vibe review attempt exceeded its bounded timeout") from exc
    except OSError as exc:
        raise PipelineError("AI_PROVIDER_ERROR", "Vibe review worker could not start") from exc
    if worker_hash != file_hash(WORKER):
        raise PipelineError("STALE_DATA", "Vibe review worker changed during request")
    if any(marker in result.stdout + result.stderr for marker in ("Windows fatal exception:", "Fatal Python error:")):
        raise PipelineError("AI_PROVIDER_ERROR", "Vibe review worker logged a native runtime error")
    if not output_path.is_file():
        raise PipelineError("AI_PROVIDER_ERROR", "Vibe review worker returned no durable response")
    payload = read_json(output_path)
    if result.returncode != 0 or payload.get("ok") is not True:
        raise PipelineError(payload.get("error", {}).get("code", "AI_PROVIDER_ERROR"), "Vibe review attempt failed")
    return payload


def probe_vibe(*, directory: Path, vibe_root: Path) -> dict:
    return _call({"mode": "probe", "timeout_seconds": 30}, directory=directory, vibe_root=vibe_root, timeout=30)["runtime"]


def invoke_review(*, packet: dict, runtime: dict, directory: Path, vibe_root: Path,
                  timeout_seconds: int, packet_max_chars: int, reasoning_effort: str, max_output_tokens: int,
                  retry_feedback: str = "") -> dict:
    return _call({"mode": "review", "packet": packet, "expected_runtime": runtime,
                  "timeout_seconds": timeout_seconds, "packet_max_chars": packet_max_chars,
                  "retry_feedback": retry_feedback,
                  "policy": {"reasoning_effort": reasoning_effort, "max_output_tokens": max_output_tokens}},
                 directory=directory, vibe_root=vibe_root, timeout=timeout_seconds)

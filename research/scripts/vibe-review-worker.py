"""One bounded non-tool LLM call in the research Python environment."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from competition_core.pipeline.artifacts import atomic_json, content_hash, read_json
from competition_core.pipeline.errors import PipelineError


def runtime_snapshot():
    from adapters.ai.openai_compatible import Settings
    return Settings.from_env().runtime()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--vibe-root", type=Path, required=True)
    args = parser.parse_args()
    request_path, output_path = args.request.resolve(), args.output.resolve()
    if output_path.parent != request_path.parent or output_path == request_path:
        raise SystemExit(2)
    try:
        request = read_json(request_path)
        if request.get("mode") not in {"probe", "review"}:
            raise PipelineError("INVALID_ARGUMENTS", "unknown Vibe review worker mode")
        runtime = runtime_snapshot()
        if runtime["temperature"] != 0:
            raise PipelineError("NOT_CONFIGURED", "research review requires configured temperature zero")
        if request.get("mode") == "probe":
            payload = {"ok": True, "runtime": runtime, "llm_calls": 0, "tool_calls": 0}
        else:
            from adapters.vibe.review import effective_runtime, review_messages
            runtime = effective_runtime(runtime, request["policy"])
            packet = request["packet"]
            if runtime != request["expected_runtime"]:
                raise PipelineError("STALE_DATA", "Vibe model configuration changed after review preflight")
            if content_hash({key: value for key, value in packet.items() if key != "packet_hash"}) != packet["packet_hash"]:
                raise PipelineError("STALE_DATA", "research packet hash mismatch")
            feedback = request.get("retry_feedback", "")
            messages = review_messages(packet, feedback, request["packet_max_chars"])
            # No bind_tools, AgentLoop, Tool.execute or data retrieval exists here.
            from adapters.ai.openai_compatible import invoke_chat
            answer = invoke_chat(messages=messages, parameters=runtime["generation_parameters"])
            payload = {"ok": True, "runtime": runtime, "content": answer["content"],
                       "tool_calls": [], "invalid_tool_calls": [],
                       "finish_reason": answer["finish_reason"],
                       "response_model": answer["model"], "usage": answer["usage"],
                       "completed_at": datetime.now(timezone.utc).isoformat(), "packet_hash": packet["packet_hash"],
                       "sent_prompt_chars": sum(len(message["content"]) for message in messages), "sent_messages_hash": content_hash(messages),
                       "llm_calls": 1, "agent_tool_calls": 0}
    except Exception as exc:
        payload = {"ok": False, "error": {"code": exc.code if isinstance(exc, PipelineError) else "AI_PROVIDER_ERROR",
                    "exception_type": type(exc).__name__}}
    atomic_json(output_path, payload, immutable=True)
    print({"ok": payload["ok"]}, flush=True)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

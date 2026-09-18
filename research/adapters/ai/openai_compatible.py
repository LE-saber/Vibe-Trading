"""Minimal OpenAI-compatible chat-completions transport for S5 reviews.

Configuration is environment-only. Credentials are never returned, logged or
persisted. The transport performs one bounded request and never follows a
redirect, retries, calls tools or talks to a trading endpoint.
"""

from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from competition_core.pipeline.errors import PipelineError


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class Settings:
    provider: str
    model: str
    base_url: str
    api_key: str
    timeout_seconds: int
    reasoning_effort: str

    @classmethod
    def from_env(cls) -> "Settings":
        provider = os.environ.get("RESEARCH_LLM_PROVIDER", "openai_compatible").strip()
        model = os.environ.get("RESEARCH_LLM_MODEL", "").strip()
        base_url = os.environ.get("RESEARCH_LLM_BASE_URL", "").strip().rstrip("/")
        api_key = os.environ.get("RESEARCH_LLM_API_KEY", "").strip()
        reasoning = os.environ.get("RESEARCH_LLM_REASONING_EFFORT", "configured").strip()
        try:
            timeout = int(os.environ.get("RESEARCH_LLM_TIMEOUT_SECONDS", "120"))
        except ValueError as exc:
            raise PipelineError("NOT_CONFIGURED", "AI timeout must be an integer") from exc
        parsed = urllib.parse.urlsplit(base_url)
        local_http = parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}
        if (not provider or not model or not api_key or not base_url
                or (parsed.scheme != "https" and not local_http)
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or not parsed.netloc or not 10 <= timeout <= 600
                or reasoning not in {"configured", "none", "low", "high", "max"}):
            raise PipelineError("NOT_CONFIGURED", "S5 AI review environment is incomplete or unsafe")
        return cls(provider, model, base_url, api_key, timeout, reasoning)

    @property
    def endpoint(self) -> str:
        if self.base_url.endswith("/v1"):
            return self.base_url + "/chat/completions"
        return self.base_url + "/v1/chat/completions"

    def runtime(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "adapter": "openai_compatible.chat_completions.v1",
            "reasoning_effort": self.reasoning_effort,
            "temperature": 0,
        }


def invoke_chat(*, messages: list[dict[str, str]], parameters: dict[str, Any]) -> dict[str, Any]:
    settings = Settings.from_env()
    payload: dict[str, Any] = {
        "model": settings.model,
        "messages": messages,
        "temperature": 0,
    }
    for key, value in parameters.items():
        if key == "extra_body" and isinstance(value, dict):
            payload.update(value)
        else:
            payload[key] = value
    request = urllib.request.Request(
        settings.endpoint,
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers={"Authorization": f"Bearer {settings.api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirect(),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
    )
    try:
        with opener.open(request, timeout=settings.timeout_seconds) as response:
            raw = response.read(2_000_001)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PipelineError("AI_PROVIDER_ERROR", "AI review provider request failed") from exc
    if len(raw) > 2_000_000:
        raise PipelineError("AI_PROVIDER_ERROR", "AI review provider response is too large")
    try:
        body = json.loads(raw.decode("utf-8"))
        choices = body["choices"]
        choice = choices[0]
        content = choice["message"]["content"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        raise PipelineError("AI_PROVIDER_ERROR", "AI review provider response is invalid") from exc
    if not isinstance(content, str):
        raise PipelineError("AI_PROVIDER_ERROR", "AI review provider returned non-text content")
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    return {
        "content": content,
        "finish_reason": choice.get("finish_reason"),
        "model": body.get("model"),
        "usage": {
            target: usage[source]
            for target, source in (
                ("input_tokens", "prompt_tokens"),
                ("output_tokens", "completion_tokens"),
                ("total_tokens", "total_tokens"),
            )
            if isinstance(usage.get(source), int)
        },
    }

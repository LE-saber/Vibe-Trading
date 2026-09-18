"""Bounded loopback quote requests, without Agent Tools or invented quote dates."""

import json
import os
import re
from urllib.parse import urlsplit

import httpx

from contracts.symbol import CanonicalSymbol, SymbolContractError
from competition_core.pipeline.errors import PipelineError


def runtime_origin(value: str | None = None) -> str:
    raw = os.environ.get("MARKET_RUNTIME_URL", "http://127.0.0.1:8920") if value is None else value
    if not isinstance(raw, str):
        raise PipelineError("NOT_CONFIGURED", "Market Runtime requires a plain loopback HTTP origin")
    try:
        parts = urlsplit(raw)
        valid = (parts.scheme == "http" and parts.hostname in {"127.0.0.1", "::1", "localhost"}
                 and parts.username is None and parts.password is None and not parts.query and not parts.fragment
                 and parts.path in {"", "/"} and (parts.port is None or 1 <= parts.port <= 65535))
    except ValueError:
        valid = False
    if not valid or any(char.isspace() for char in raw):
        raise PipelineError("NOT_CONFIGURED", "Market Runtime requires a plain loopback HTTP origin")
    return raw.rstrip("/")


def fetch_quotes(symbols: list[str], *, endpoint: str | None = None, transport=None) -> dict:
    """Keep service receive time separate from each provider's server timestamp."""
    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 50:
        raise PipelineError("LIMIT_EXCEEDED", "quote batch must contain one to fifty unique equities")
    try:
        for symbol in symbols:
            if re.fullmatch(r"(?:000\d{3}\.SH|399\d{3}\.SZ)", symbol) is None:
                CanonicalSymbol.from_core(symbol)
        if len(symbols) != len(set(symbols)):
            raise SymbolContractError("duplicate")
    except SymbolContractError as exc:
        raise PipelineError("IDENTITY_CONFLICT", "invalid or duplicated canonical quote identity") from exc
    origin = runtime_origin(endpoint)
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    def invalid_constant(_):
        raise ValueError("non-finite JSON number")
    try:
        with httpx.Client(timeout=10., trust_env=False, follow_redirects=False, transport=transport) as client:
            with client.stream("POST", origin + "/quotes", json={"symbols": symbols}) as response:
                if response.status_code != 200:
                    raise PipelineError("SOURCE_UNAVAILABLE", "Market Runtime quote request failed",
                                        http_status=response.status_code)
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > 1024 * 1024:
                        raise PipelineError("SOURCE_UNAVAILABLE", "Market Runtime response exceeds size budget")
                    chunks.append(chunk)
                body = json.loads(b"".join(chunks), object_pairs_hook=pairs, parse_constant=invalid_constant)
    except (httpx.HTTPError, ValueError, UnicodeError) as exc:
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError("SOURCE_UNAVAILABLE", "Market Runtime quote response unavailable or malformed") from exc
    if not isinstance(body, dict) or not isinstance(body.get("quotes"), list) or len(body["quotes"]) > 50:
        raise PipelineError("SOURCE_UNAVAILABLE", "Market Runtime response lacks a bounded quote list")
    # Neither endpoint nor envelope as_of is evidence of freshness. The pure gate
    # validates every row's source, trade_date, server_time and timestamp_quality.
    return {"endpoint": origin, "transport_source": "market_runtime.http", "response": body}

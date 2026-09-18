import json

import httpx
import pytest

from adapters.market_runtime.quotes import fetch_quotes, runtime_origin
from competition_core.pipeline.errors import PipelineError


def test_batch_transport_preserves_missing_dates_and_canonical_symbols():
    def respond(request):
        assert request.url.path == "/quotes" and request.method == "POST"
        assert json.loads(request.content) == {"symbols": ["300750.SZ", "600519.SH"]}
        return httpx.Response(200, json={"as_of": "2026-09-02T14:50:00+08:00", "quotes": [
            {"symbol": "300750.SZ", "server_time": "14:50:00", "trade_date": None,
             "timestamp_quality": "time_only", "as_of": None, "source": "mootdx"}]})
    result = fetch_quotes(["300750.SZ", "600519.SH"], transport=httpx.MockTransport(respond))
    assert result["response"]["quotes"][0]["trade_date"] is None
    assert result["response"]["quotes"][0]["as_of"] is None


def test_configured_index_shape_is_allowed_only_as_explicit_quote_identity():
    def respond(request):
        assert json.loads(request.content) == {"symbols": ["600519.SH", "000300.SH"]}
        return httpx.Response(200, json={"quotes": []})
    assert fetch_quotes(["600519.SH", "000300.SH"], transport=httpx.MockTransport(respond))["response"]["quotes"] == []


@pytest.mark.parametrize("endpoint", [
    "https://127.0.0.1:8920", "http://example.org", "http://user:secret@127.0.0.1", "http://127.0.0.1/a",
    "http://127.0.0.1?token=secret", "http://127.0.0.1/#x", "http://127.0.0.1:99999", " http://localhost:8920",
])
def test_remote_credentials_paths_and_invalid_origins_rejected(endpoint):
    with pytest.raises(PipelineError) as error:
        runtime_origin(endpoint)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("symbols", [[], ["510300.SH"], ["300750.SH"], ["300750.SZ"] * 2, ["300750.SZ"] * 51])
def test_identity_and_size_fail_before_http(symbols):
    with pytest.raises(PipelineError):
        fetch_quotes(symbols, transport=httpx.MockTransport(lambda _: pytest.fail("no network allowed")))


@pytest.mark.parametrize("status", [302, 400, 503])
def test_http_failure_does_not_echo_provider_error(status):
    response = httpx.MockTransport(lambda _: httpx.Response(status, text="token=secret",
                                                           headers={"Location": "https://external.example"}))
    with pytest.raises(PipelineError) as error:
        fetch_quotes(["600519.SH"], transport=response)
    assert error.value.details == {"http_status": status} and "secret" not in str(error.value)


@pytest.mark.parametrize("body", ["not json", "[]", '{"quotes":null}', '{"quotes":[]} ' + " " * 1048576,
                                  '{"quotes":[],"quotes":[]}', '{"quotes":[{"price":NaN}]}'],
                         ids=["invalid_json", "not_mapping", "missing_quotes", "oversized", "duplicate_key", "nonfinite"])
def test_malformed_and_oversized_responses_fail_closed(body):
    with pytest.raises(PipelineError):
        fetch_quotes(["600519.SH"], transport=httpx.MockTransport(lambda _: httpx.Response(200, text=body)))

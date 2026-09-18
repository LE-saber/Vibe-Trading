from datetime import date, datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from competition_core.pipeline.dated_cache import DatedSourceCache
from competition_core.pipeline.artifacts import atomic_json, content_hash
from competition_core.pipeline.errors import PipelineError
from competition_core.pipeline.sources import ResearchDataClient
from competition_core.pipeline.time_context import ResearchContext


SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_unpublished_margin_is_frozen_missing_without_calling_provider(tmp_path):
    context = ResearchContext(date(2026, 9, 2), "preopen", date(2026, 9, 1),
                              datetime(2026, 9, 2, 8, 0, tzinfo=SHANGHAI))
    client = ResearchDataClient(SimpleNamespace(margin_detail=lambda **_: pytest.fail("unpublished source must not be fetched")),
        context, observed_clock=lambda: datetime(2026, 9, 2, 11, tzinfo=SHANGHAI))
    cache = DatedSourceCache(tmp_path, client, source_name="synthetic-fixture")
    request = {"trade_date": "20260901", "fields": "ts_code,trade_date,rzye"}
    first = cache.get("margin_detail", request, key_fields=("ts_code", "trade_date"))
    assert first["status"] == "MISSING" and first["data"] == []
    assert first["error"]["code"] == "SOURCE_NOT_PUBLISHED"
    assert cache.get("margin_detail", request, key_fields=("ts_code", "trade_date")) == first


class ExplicitDatedFake:
    """Test-only provider fixture; no source request in this file is live."""

    def __init__(self, fetch):
        self.fetch = fetch
        self.calls = []

    def moneyflow(self, **parameters):
        self.calls.append(dict(parameters))
        return self.fetch(parameters)


def _client(provider: ExplicitDatedFake) -> ResearchDataClient:
    context = ResearchContext(
        research_date=date(2026, 9, 2),
        phase="intraday",
        market_data_reference_date=date(2026, 9, 1),
        as_of=datetime(2026, 9, 2, 10, tzinfo=SHANGHAI),
    )
    observed = datetime(2026, 9, 2, 11, tzinfo=SHANGHAI)
    return ResearchDataClient(SimpleNamespace(moneyflow=provider.moneyflow), context,
                              observed_clock=lambda: observed)


def _request(**extra):
    return {"trade_date": "20260901", "fields": "ts_code,trade_date,net_mf_amount", **extra}


def test_success_short_pages_continue_until_empty_and_sort_keys(tmp_path):
    pages = [
        pd.DataFrame([
            {"ts_code": "600002.SH", "trade_date": "20260901", "net_mf_amount": 2.0},
            {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": float("nan")},
        ]),
        pd.DataFrame([{"ts_code": "600003.SH", "trade_date": "20260901", "net_mf_amount": 3.0}]),
        pd.DataFrame(),
    ]

    page_by_offset = {0: pages[0], 2: pages[1], 3: pages[2]}
    provider = ExplicitDatedFake(lambda parameters: page_by_offset.get(parameters["offset"], pd.DataFrame()))
    client = _client(provider)
    result = DatedSourceCache(tmp_path, client, source_name="fixture").get(
        "moneyflow", _request(), key_fields=("ts_code", "trade_date")
    )

    assert result["status"] == "SUCCESS"
    assert [row["ts_code"] for row in result["data"]] == ["600001.SH", "600002.SH", "600003.SH"]
    assert result["data"][0]["net_mf_amount"] is None
    assert [call["offset"] for call in provider.calls] == [0, 2, 3]
    assert all(call["limit"] == 6000 for call in provider.calls)
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_source_ignoring_offset_duplicate_page_fails_closed(tmp_path):
    rows = pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    provider = ExplicitDatedFake(lambda _parameters: rows)

    with pytest.raises(PipelineError, match="duplicate identity"):
        DatedSourceCache(tmp_path, _client(provider), source_name="fixture").get(
            "moneyflow", _request(), key_fields=("ts_code", "trade_date")
        )
    assert [call["offset"] for call in provider.calls] == [0, 1]


def test_future_response_is_rejected_by_research_data_client(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260902", "net_mf_amount": 1.0},
    ]))

    with pytest.raises(PipelineError, match="outside requested"):
        DatedSourceCache(tmp_path, _client(provider), source_name="fixture").get(
            "moneyflow", _request(), key_fields=("ts_code", "trade_date")
        )
    assert not list(tmp_path.rglob("*.json"))


def test_missing_date_unknown_endpoint_and_forbidden_paging_are_rejected(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pytest.fail("must reject before provider"))
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")

    with pytest.raises(PipelineError):
        cache.get("moneyflow", {"fields": "ts_code,trade_date"}, key_fields=("ts_code", "trade_date"))
    with pytest.raises(PipelineError):
        cache.get("unknown", _request(), key_fields=("ts_code", "trade_date"))
    with pytest.raises(PipelineError):
        cache.get("moneyflow", {**_request(), "offset": 0}, key_fields=("ts_code", "trade_date"))
    assert provider.calls == []


def test_empty_query_is_success_not_missing(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pd.DataFrame())
    result = DatedSourceCache(tmp_path, _client(provider), source_name="fixture").get(
        "moneyflow", _request(), key_fields=("ts_code", "trade_date")
    )

    assert result["status"] == "SUCCESS"
    assert result["data"] == []
    assert result["error"] is None


def test_failed_query_is_frozen_without_secret_and_not_retried(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: (_ for _ in ()).throw(RuntimeError("secret-token-123")))
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    first = cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    provider.fetch = lambda _parameters: pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    second = cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))

    assert first["status"] == second["status"] == "MISSING"
    assert first["data"] == second["data"] == []
    assert first["error"]["code"] == "SOURCE_UNAVAILABLE"
    assert len(provider.calls) == 1
    assert "secret-token-123" not in "".join(path.read_text(encoding="utf-8") for path in tmp_path.rglob("*.json"))


def test_missing_key_field_is_frozen_as_field_error(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pd.DataFrame([
        {"trade_date": "20260901", "net_mf_amount": 1.0},
    ]))
    result = DatedSourceCache(tmp_path, _client(provider), source_name="fixture").get(
        "moneyflow", _request(), key_fields=("ts_code", "trade_date")
    )

    assert result["status"] == "MISSING"
    assert result["error"] == {"code": "FIELD_ERROR", "class": "FieldError"}
    assert result["data"] == []


def test_cache_hit_does_not_request_and_hash_tamper_is_rejected(tmp_path):
    row = pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    provider = ExplicitDatedFake(lambda parameters: row if parameters["offset"] == 0 else pd.DataFrame())
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    original = cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    assert cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date")) == original
    assert len(provider.calls) == 2

    path = next(tmp_path.rglob("*.json"))
    tampered = path.read_text(encoding="utf-8").replace("\"net_mf_amount\":1.0", "\"net_mf_amount\":9.0")
    path.write_text(tampered, encoding="utf-8")
    with pytest.raises(PipelineError, match="integrity hash"):
        cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))


def test_cache_only_never_calls_provider_and_requires_a_frozen_record(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pytest.fail("cache-only must never request"))
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    with pytest.raises(PipelineError, match="frozen source response"):
        cache.get_cached("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    assert provider.calls == []

    seeded = ExplicitDatedFake(lambda parameters: pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0}
    ]) if parameters["offset"] == 0 else pd.DataFrame())
    original = DatedSourceCache(tmp_path, _client(seeded), source_name="fixture").get(
        "moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    assert cache.get_cached("moneyflow", _request(), key_fields=("ts_code", "trade_date")) == original
    assert provider.calls == []


def test_parameter_change_gets_a_distinct_request_id_and_cache_file(tmp_path):
    provider = ExplicitDatedFake(lambda _parameters: pd.DataFrame())
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    first = cache.get("moneyflow", _request(ts_code="600001.SH"), key_fields=("ts_code", "trade_date"))
    second = cache.get("moneyflow", _request(ts_code="600002.SH"), key_fields=("ts_code", "trade_date"))

    assert first["request_id"] != second["request_id"]
    assert len(provider.calls) == 2
    assert len(list(tmp_path.rglob("*.json"))) == 2


def test_cached_record_context_or_key_identity_mismatch_fails_closed(tmp_path):
    row = pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    provider = ExplicitDatedFake(lambda parameters: row if parameters["offset"] == 0 else pd.DataFrame())
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    path = next(tmp_path.rglob("*.json"))
    # A hash-valid but identity-invalid artifact must still fail on the cache boundary.
    record = json.loads(path.read_text(encoding="utf-8"))
    record["context"]["phase"] = "postclose"
    record["integrity_hash"] = content_hash(
        {key: value for key, value in record.items() if key != "integrity_hash"}
    )
    atomic_json(path, record)
    with pytest.raises(PipelineError, match="identity"):
        cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))


def test_cache_reread_rejects_row_trade_date_outside_request_range(tmp_path):
    row = pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    request = {
        "start_date": "20260831",
        "end_date": "20260901",
        "fields": "ts_code,trade_date,net_mf_amount",
    }
    provider = ExplicitDatedFake(lambda parameters: row if parameters["offset"] == 0 else pd.DataFrame())
    cache = DatedSourceCache(tmp_path, _client(provider), source_name="fixture")
    cache.get("moneyflow", request, key_fields=("ts_code", "trade_date"))
    path = next(tmp_path.glob("*.json"))
    record = json.loads(path.read_text(encoding="utf-8"))
    record["data"][0]["trade_date"] = "20260830"
    record["integrity_hash"] = content_hash(
        {key: value for key, value in record.items() if key != "integrity_hash"}
    )
    atomic_json(path, record)

    with pytest.raises(PipelineError, match="trade_date"):
        cache.get("moneyflow", request, key_fields=("ts_code", "trade_date"))


def test_cache_reread_keeps_observed_time_and_rejects_invalid_bounds(tmp_path):
    row = pd.DataFrame([
        {"ts_code": "600001.SH", "trade_date": "20260901", "net_mf_amount": 1.0},
    ])
    provider = ExplicitDatedFake(lambda parameters: row if parameters["offset"] == 0 else pd.DataFrame())
    client = _client(provider)
    cache = DatedSourceCache(tmp_path, client, source_name="fixture")
    original = cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    path = next(tmp_path.glob("*.json"))
    record = json.loads(path.read_text(encoding="utf-8"))

    record["observed_at"] = "2026-09-02T09:59:59+08:00"
    record["integrity_hash"] = content_hash(
        {key: value for key, value in record.items() if key != "integrity_hash"}
    )
    atomic_json(path, record)
    with pytest.raises(PipelineError, match="precedes context"):
        cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))

    record["observed_at"] = "2026-09-02T11:00:01+08:00"
    record["integrity_hash"] = content_hash(
        {key: value for key, value in record.items() if key != "integrity_hash"}
    )
    atomic_json(path, record)
    with pytest.raises(PipelineError, match="future relative"):
        cache.get("moneyflow", _request(), key_fields=("ts_code", "trade_date"))
    assert original["observed_at"] == "2026-09-02T11:00:00+08:00"
    assert len(provider.calls) == 2

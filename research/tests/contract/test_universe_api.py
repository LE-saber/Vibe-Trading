import pytest
from fastapi.testclient import TestClient

from competition_core.api import app, get_snapshot_cache, get_tushare_client
from competition_core.storage.snapshots import SnapshotCache
from tests.fakes import FakeTushare


def _client(tmp_path, tushare=None) -> TestClient:
    app.dependency_overrides[get_tushare_client] = lambda: tushare or FakeTushare()
    app.dependency_overrides[get_snapshot_cache] = lambda: SnapshotCache(tmp_path / "cache")
    return TestClient(app)


def test_snapshot_api_returns_phase_reference_and_members(tmp_path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/universe/snapshot", params={"date": "2026-08-31", "phase": "preopen"}
        )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["snapshot_phase"] == "preopen"
    assert payload["market_data_reference_date"] == "2026-08-28"
    assert any(item["ts_code"] == "300750.SZ" for item in payload["members"])


def test_candidates_api_returns_compact_deterministic_ranking(tmp_path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/candidates",
            params={"date": "2026-08-31", "phase": "postclose", "sort_by": "amount", "limit": 3},
        )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["returned"] == 3
    assert [row["symbol"] for row in payload["rows"]] == [
        "688001.SH", "300750.SZ", "600000.SH"
    ]


@pytest.mark.parametrize("requested", [100, 50, 40, 30])
def test_candidates_api_rejects_limits_above_twenty_with_structured_error(
    tmp_path, requested
) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/candidates",
            params={
                "date": "2026-08-31",
                "phase": "postclose",
                "sort_by": "amount",
                "limit": requested,
            },
        )
    app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json() == {
        "code": "LIMIT_EXCEEDED",
        "message": "limit must be <= 20",
        "details": {"requested": requested, "maximum": 20},
    }


def test_candidates_api_accepts_limit_twenty_and_advertises_same_maximum(tmp_path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/candidates",
            params={
                "date": "2026-08-31",
                "phase": "postclose",
                "sort_by": "amount",
                "limit": 20,
            },
        )
    app.dependency_overrides.clear()

    parameter = next(
        item
        for item in app.openapi()["paths"]["/candidates"]["get"]["parameters"]
        if item["name"] == "limit"
    )
    assert response.status_code == 200
    assert response.json()["limit"] == 20
    assert parameter["schema"]["maximum"] == 20


def test_candidates_api_rejects_invalid_sort_key(tmp_path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/candidates",
            params={"date": "2026-08-31", "phase": "postclose", "sort_by": "future_return"},
        )
    app.dependency_overrides.clear()

    assert response.status_code == 422


class FailedTushare:
    def __getattr__(self, _name):
        return lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("provider down"))


class SecretFailedTushare:
    def __getattr__(self, _name):
        return lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("TUSHARE_TOKEN=core-secret")
        )


def test_tushare_failure_uses_last_valid_snapshot_and_marks_it_stale(tmp_path) -> None:
    with _client(tmp_path) as client:
        fresh = client.get(
            "/universe/snapshot", params={"date": "2026-08-31", "phase": "postclose"}
        )
    app.dependency_overrides.clear()
    assert fresh.status_code == 200 and fresh.json()["stale"] is False

    with _client(tmp_path, FailedTushare()) as client:
        stale = client.get(
            "/universe/snapshot", params={"date": "2026-09-01", "phase": "postclose"}
        )
    app.dependency_overrides.clear()
    assert stale.status_code == 200
    assert stale.json()["stale"] is True
    assert stale.json()["requested_date"] == "2026-09-01"
    assert stale.json()["cached_date"] == "2026-08-31"
    assert stale.json()["as_of"] == "2026-08-31"


def test_no_cached_snapshot_returns_503_instead_of_fake_fresh_data(tmp_path) -> None:
    with _client(tmp_path, FailedTushare()) as client:
        response = client.get(
            "/universe/snapshot", params={"date": "2026-09-01", "phase": "preopen"}
        )
    app.dependency_overrides.clear()
    assert response.status_code == 503


def test_candidate_source_error_does_not_echo_credentials(tmp_path) -> None:
    with _client(tmp_path, SecretFailedTushare()) as client:
        response = client.get(
            "/candidates",
            params={"date": "2026-09-01", "phase": "postclose", "limit": 20},
        )
    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert "core-secret" not in response.text
    assert response.json() == {
        "code": "SOURCE_UNAVAILABLE",
        "message": "candidate data source is unavailable",
        "details": {"exception_type": "RuntimeError"},
    }


def test_tushare_failure_uses_matching_candidate_cache_and_applies_new_limit(tmp_path) -> None:
    with _client(tmp_path) as client:
        fresh = client.get(
            "/candidates",
            params={
                "date": "2026-08-31",
                "phase": "postclose",
                "sort_by": "amount",
                "limit": 3,
            },
        )
    app.dependency_overrides.clear()
    assert fresh.status_code == 200 and fresh.json()["stale"] is False

    with _client(tmp_path, FailedTushare()) as client:
        stale = client.get(
            "/candidates",
            params={
                "date": "2026-09-01",
                "phase": "postclose",
                "sort_by": "amount",
                "limit": 2,
            },
        )
    app.dependency_overrides.clear()

    payload = stale.json()
    assert stale.status_code == 200
    assert payload["stale"] is True
    assert payload["fallback_used"] is True
    assert payload["requested_date"] == "2026-09-01"
    assert payload["cached_date"] == "2026-08-31"
    assert payload["returned"] == 2
    assert len(payload["rows"]) == 2


def test_symbol_api_returns_core_owned_identity_and_rejects_non_stock() -> None:
    client = TestClient(app)

    response = client.get("/symbols/300750.SZ")
    rejected = client.get("/symbols/510300.SH")

    assert response.status_code == 200
    assert response.json() == {
        "symbol": "300750.SZ",
        "code": "300750",
        "exchange": "SZSE",
        "market": "CN_A",
        "board": "CHINEXT",
        "currency": "CNY",
        "lot_size": 100,
    }
    assert rejected.status_code == 404

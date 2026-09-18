from fastapi.testclient import TestClient

from market_runtime.api import app, get_provider
from tests.test_provider import FakeClient
from market_runtime.provider import MootdxProvider


def client() -> TestClient:
    app.dependency_overrides[get_provider] = lambda: MootdxProvider(FakeClient())
    return TestClient(app)


def test_health_quotes_and_bars_contracts() -> None:
    with client() as test_client:
        health = test_client.get("/health")
        readiness = test_client.get("/readiness")
        quotes = test_client.post("/quotes", json={"symbols": ["600519.SH"]})
        bars = test_client.post(
            "/bars", json={"symbols": ["600519.SH"], "interval": "1m", "offset": 100}
        )
    app.dependency_overrides.clear()

    assert health.json()["service"] == "competition-market-runtime"
    assert readiness.json()["ready"] is True
    assert quotes.status_code == 200 and len(quotes.json()["quotes"]) == 1
    assert bars.status_code == 200 and bars.json()["bars"]["600519.SH"]


def test_invalid_symbol_and_interval_fail_closed() -> None:
    with client() as test_client:
        symbol = test_client.post("/quotes", json={"symbols": ["600519.SZ"]})
        interval = test_client.post(
            "/bars", json={"symbols": ["600519.SH"], "interval": "1d"}
        )
    app.dependency_overrides.clear()

    assert symbol.status_code == 422
    assert interval.status_code == 422

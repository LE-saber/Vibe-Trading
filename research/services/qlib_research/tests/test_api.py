from fastapi.testclient import TestClient

from qlib_research.api import app


def test_health_version_and_explicit_not_ready(monkeypatch) -> None:
    monkeypatch.delenv("QLIB_CANONICAL_PARQUET", raising=False)
    monkeypatch.delenv("QLIB_UNIVERSE_PARQUET", raising=False)
    with TestClient(app) as client:
        assert client.get("/health").json()["status"] == "ok"
        assert client.get("/version").json()["framework_version"] == "0.9.7"
        readiness = client.get("/readiness").json()
    assert readiness["ready"] is False
    assert "not configured" in readiness["reason"]

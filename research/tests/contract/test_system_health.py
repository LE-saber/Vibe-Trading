from fastapi.testclient import TestClient

import competition_core.api as api


def test_system_health_reports_component_failures_without_failing_request(monkeypatch) -> None:
    monkeypatch.setattr(
        api,
        "_SYSTEM_ENDPOINTS",
        {
            "market_runtime": "http://127.0.0.1:1",
            "ai_decision": "http://127.0.0.1:2",
        },
    )
    monkeypatch.setattr(
        api,
        "readiness",
        lambda: {
            "service": api.SERVICE_NAME,
            "status": api.STATUS,
            "version": api.__version__,
            "ready": True,
            "reasons": [],
        },
    )

    response = TestClient(api.app).get("/system/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert set(payload["unavailable"]) == {"market_runtime", "ai_decision"}
    assert payload["components"]["competition_core"]["status"] == "ok"
    assert payload["components"]["market_runtime"]["status"] == "unavailable"


def test_readiness_fails_closed_without_tushare_token(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setenv("COMPETITION_CORE_SERVICE_TOKEN", "synthetic-core-service-token-0001")

    payload = api.readiness()

    assert payload["ready"] is False
    assert "TUSHARE_TOKEN is not configured" in payload["reasons"]


def test_readiness_fails_closed_without_proxy_service_identity(monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "synthetic-token")
    monkeypatch.delenv("COMPETITION_CORE_SERVICE_TOKEN", raising=False)

    payload = api.readiness()

    assert payload["ready"] is False
    assert "Competition Core service identity is not configured" in payload["reasons"]

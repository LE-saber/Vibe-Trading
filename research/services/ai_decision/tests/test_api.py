from __future__ import annotations

from fastapi.testclient import TestClient

from ai_decision.api import app, get_runner


class NotReadyRunner:
    def provider_ready(self) -> bool:
        return False

    def analyze(self, _request):
        from ai_decision.runner import DecisionConfigurationError

        raise DecisionConfigurationError("not configured")


def test_health_readiness_and_unconfigured_analysis(analyze_request) -> None:
    app.dependency_overrides[get_runner] = lambda: NotReadyRunner()
    try:
        with TestClient(app) as client:
            health = client.get("/health")
            readiness = client.get("/readiness")
            analysis = client.post(
                "/decisions/analyze", json=analyze_request.model_dump(mode="json")
            )
    finally:
        app.dependency_overrides.clear()

    assert health.status_code == 200
    assert readiness.json()["ready"] is False
    assert analysis.status_code == 503


def test_batch_more_than_ten_is_rejected(analyze_request) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/decisions/batch",
            json={"items": [analyze_request.model_dump(mode="json")] * 11},
        )
    assert response.status_code == 422

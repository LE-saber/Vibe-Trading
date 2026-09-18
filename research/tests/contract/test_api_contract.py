from fastapi.testclient import TestClient

from competition_core.api import app


EXPECTED_STATUS = {
    "service": "competition-core",
    "status": "ok",
    "version": "0.1.0",
}

client = TestClient(app)


def test_health_contract():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == EXPECTED_STATUS


def test_version_contract():
    response = client.get("/version")

    assert response.status_code == 200
    assert response.json() == EXPECTED_STATUS

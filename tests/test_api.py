from fastapi.testclient import TestClient

from src.serving.api import app


client = TestClient(app)


def test_root() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["project"] == "streaming-feature-platform"


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_quality_summary() -> None:
    response = client.get("/quality/summary")
    assert response.status_code == 200
    payload = response.json()
    assert "raw_events" in payload
    assert "validations" in payload
    assert "freshness" in payload

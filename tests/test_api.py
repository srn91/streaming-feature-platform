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
    assert "schema_compatibility" in payload
    assert "freshness" in payload


def test_training_dataset_summary() -> None:
    response = client.get("/training-dataset/summary")
    assert response.status_code == 200
    payload = response.json()
    assert "rows" in payload
    assert "positive_labels" in payload


def test_gcp_readiness_endpoint() -> None:
    response = client.get("/gcp/readiness")
    assert response.status_code == 200
    payload = response.json()
    assert payload["project_id"] == "streaming-feature-platform"
    assert payload["pubsub_messages"] > 0
    assert payload["feature_snapshot_rows"] > 0


def test_metrics_endpoint() -> None:
    client.get("/quality/summary")
    client.get("/training-dataset/summary")
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "streaming_feature_platform_http_requests_total" in response.text
    assert "streaming_feature_platform_training_dataset_rows" in response.text

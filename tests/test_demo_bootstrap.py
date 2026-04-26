from fastapi.testclient import TestClient

from src.demo.bootstrap import bootstrap_hosted_demo
from src.features.offline_store import latest_feature_snapshot
from src.pipelines.raw_event_store import raw_event_count
from src.serving.api import app


def test_hosted_demo_bootstrap_seeds_and_materializes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    result = bootstrap_hosted_demo(
        num_users=2,
        events_per_user=3,
        sample_events_path=tmp_path / "sample_events.jsonl",
    )

    assert result.seeded_events == 6
    assert result.raw_event_count == 6
    assert result.feature_snapshot_count == 2
    assert raw_event_count() == 6
    assert latest_feature_snapshot("user_0001") is not None


def test_hosted_demo_root_exposes_bootstrap_state(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr("src.config.settings.hosted_demo", True)
    monkeypatch.setattr("src.config.settings.demo_seed_users", 1)
    monkeypatch.setattr("src.config.settings.demo_events_per_user", 2)
    monkeypatch.setattr("src.config.settings.demo_sample_events_path", str(tmp_path / "sample_events.jsonl"))

    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        payload = response.json()
        assert payload["mode"] == "hosted_demo"
        assert payload["demo_bootstrapped"] is True
        assert client.get("/features/user_0001").status_code == 200

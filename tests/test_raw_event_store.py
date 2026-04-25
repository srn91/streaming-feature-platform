from src.connectors.generate_sample_events import generate_events
from src.pipelines.raw_event_store import RAW_DB_PATH, persist_raw_event, raw_event_count


def test_persist_raw_event(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")

    event = generate_events(num_users=1, events_per_user=1)[0]
    persist_raw_event(event)

    assert raw_event_count() == 1

from datetime import UTC, datetime, timedelta

from src.connectors.schemas import EventMetadata, EventType, UserEvent
from src.pipelines.raw_event_store import persist_raw_event
from src.training.export_dataset import build_training_dataset, export_training_dataset


def _event(
    event_id: str,
    event_type: EventType,
    event_ts: datetime,
    session_id: str,
    category: str = "electronics",
    price: float | None = None,
) -> UserEvent:
    return UserEvent(
        event_id=event_id,
        entity_id="user_0001",
        event_type=event_type,
        event_ts=event_ts,
        metadata=EventMetadata(
            session_id=session_id,
            page="/product",
            product_id="prod_1001",
            category=category,
            price=price,
        ),
    )


def test_training_dataset_export_contains_features_and_label(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr("src.training.export_dataset.TRAINING_DATASET_PATH", tmp_path / "training_dataset.csv")

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    events = [
        _event("evt_1", EventType.PAGE_VIEW, now - timedelta(minutes=40), "s1"),
        _event("evt_2", EventType.CLICK, now - timedelta(minutes=20), "s1"),
        _event("evt_3", EventType.PURCHASE, now - timedelta(minutes=5), "s2", price=59.99),
    ]
    for event in events:
        persist_raw_event(event)

    from src.features.materialize_features import materialize_features

    materialize_features(write_online=False)
    dataset = build_training_dataset()
    summary = export_training_dataset()

    assert len(dataset.index) == 1
    assert int(dataset.iloc[0]["purchased_24h"]) == 1
    assert summary.rows == 1
    assert summary.positive_labels == 1

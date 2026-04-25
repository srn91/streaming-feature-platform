from datetime import UTC, datetime, timedelta

from src.connectors.schemas import EventMetadata, EventType, UserEvent
from src.features.materialize_features import materialize_features
from src.features.offline_store import compact_feature_snapshots, feature_snapshot_count, latest_feature_snapshot, persist_feature_snapshot
from src.features.definitions import FeatureSnapshot
from src.pipelines.raw_event_store import persist_raw_event


def _event(
    event_id: str,
    event_type: EventType,
    event_ts: datetime,
    session_id: str,
    category: str | None = "electronics",
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


def test_materialize_features_from_raw_events(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    events = [
        _event("evt_1", EventType.PAGE_VIEW, now - timedelta(minutes=40), "s1"),
        _event("evt_2", EventType.CLICK, now - timedelta(minutes=35), "s1"),
        _event("evt_3", EventType.PURCHASE, now - timedelta(minutes=20), "s2", price=49.99),
        _event("evt_4", EventType.CLICK, now - timedelta(minutes=5), "s2"),
    ]

    for event in events:
        persist_raw_event(event)

    count = materialize_features(write_online=False)
    snapshot = latest_feature_snapshot("user_0001")

    assert count == 1
    assert snapshot is not None
    assert snapshot.clicks_1h == 2
    assert snapshot.sessions_24h == 2
    assert snapshot.purchase_value_24h == 49.99
    assert snapshot.click_to_impression_ratio_24h == 2.0
    assert snapshot.dominant_category == "electronics"


def test_materialize_features_is_idempotent_for_same_as_of_ts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    events = [
        _event("evt_1", EventType.PAGE_VIEW, now - timedelta(minutes=30), "s1"),
        _event("evt_2", EventType.CLICK, now - timedelta(minutes=10), "s1"),
    ]

    for event in events:
        persist_raw_event(event)

    first_count = materialize_features(write_online=False)
    second_count = materialize_features(write_online=False)

    assert first_count == 1
    assert second_count == 1
    assert feature_snapshot_count() == 1


def test_compact_feature_snapshots_removes_preexisting_duplicates(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    as_of_ts = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    snapshot = FeatureSnapshot(
        entity_id="user_0001",
        as_of_ts=as_of_ts,
        clicks_1h=2,
        sessions_24h=1,
        purchase_value_24h=10.0,
        last_active_ts=as_of_ts,
        click_to_impression_ratio_24h=1.0,
        dominant_category="electronics",
        dominant_category_share=1.0,
    )

    persist_feature_snapshot(snapshot)
    persist_feature_snapshot(snapshot.model_copy())

    # Simulate duplicates that existed before idempotent writes by inserting an exact duplicate directly.
    from src.features import offline_store

    with offline_store._connect() as conn:
        conn.execute(
            """
            INSERT INTO feature_snapshots (
                entity_id,
                as_of_ts,
                feature_version,
                clicks_1h,
                sessions_24h,
                purchase_value_24h,
                last_active_ts,
                click_to_impression_ratio_24h,
                dominant_category,
                dominant_category_share
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot.entity_id,
                snapshot.as_of_ts,
                snapshot.feature_version,
                snapshot.clicks_1h,
                snapshot.sessions_24h,
                snapshot.purchase_value_24h,
                snapshot.last_active_ts,
                snapshot.click_to_impression_ratio_24h,
                snapshot.dominant_category,
                snapshot.dominant_category_share,
            ],
        )

    assert feature_snapshot_count() == 3
    compact_feature_snapshots()
    assert feature_snapshot_count() == 1

from datetime import UTC, datetime, timedelta

from src.connectors.schemas import EventMetadata, EventType, UserEvent
from src.config import settings
from src.features.materialize_features import materialize_features
from src.quality.checks import build_quality_summary
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


def test_build_quality_summary(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr("src.quality.checks.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.quality.checks.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    now = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    events = [
        _event("evt_1", EventType.PAGE_VIEW, now - timedelta(minutes=15), "s1"),
        _event("evt_2", EventType.CLICK, now - timedelta(minutes=10), "s1"),
        _event("evt_3", EventType.PURCHASE, now - timedelta(minutes=5), "s2", price=15.5),
    ]

    for event in events:
        persist_raw_event(event)

    materialize_features(write_online=False)
    summary = build_quality_summary()

    assert summary["raw_events"]["total_events"] == 3
    assert summary["features"]["total_feature_snapshots"] == 1
    assert summary["features"]["latest_snapshot_entities"] == 1
    assert summary["schema_compatibility"]["status"] == "ok"
    assert summary["freshness"]["status"] == "ok"
    assert summary["reconciliation"]["status"] in {"skipped", "ok", "mismatch"}


def test_freshness_summary_warns_when_feature_lag_exceeds_threshold(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr("src.quality.checks.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.quality.checks.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr(settings, "freshness_warning_lag_seconds", 60)
    monkeypatch.setattr(settings, "freshness_error_lag_seconds", 601)

    base = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    persist_raw_event(_event("evt_1", EventType.PAGE_VIEW, base - timedelta(minutes=20), "s1"))
    persist_raw_event(_event("evt_2", EventType.CLICK, base - timedelta(minutes=10), "s1"))
    materialize_features(write_online=False)
    persist_raw_event(_event("evt_3", EventType.PURCHASE, base, "s2", price=20.0))

    summary = build_quality_summary()

    assert summary["freshness"]["status"] == "warning"
    assert summary["freshness"]["lag_seconds"] == 600.0


def test_feature_summary_distinguishes_historical_rows_from_latest_snapshot_coverage(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.features.offline_store.FEATURE_DB_PATH", tmp_path / "features.duckdb")
    monkeypatch.setattr("src.quality.checks.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.quality.checks.FEATURE_DB_PATH", tmp_path / "features.duckdb")

    base = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)
    persist_raw_event(_event("evt_1", EventType.PAGE_VIEW, base - timedelta(minutes=20), "s1"))
    persist_raw_event(_event("evt_2", EventType.CLICK, base - timedelta(minutes=10), "s1"))
    materialize_features(write_online=False)

    persist_raw_event(_event("evt_3", EventType.PURCHASE, base + timedelta(minutes=5), "s2", price=20.0))
    materialize_features(write_online=False)

    summary = build_quality_summary()

    assert summary["features"]["total_feature_snapshots"] == 2
    assert summary["features"]["distinct_entities"] == 1
    assert summary["features"]["latest_snapshot_entities"] == 1

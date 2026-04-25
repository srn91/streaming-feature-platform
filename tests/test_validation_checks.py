from datetime import UTC, datetime, timedelta

import duckdb

from src.connectors.schemas import EventMetadata, EventType, UserEvent
from src.config import settings
from src.pipelines import raw_event_store
from src.pipelines.raw_event_store import persist_raw_event
from src.quality.checks import build_quality_summary


def _event(
    event_id: str,
    event_ts: datetime,
    session_id: str,
    page: str = "/product",
) -> UserEvent:
    return UserEvent(
        event_id=event_id,
        entity_id="user_0001",
        event_type=EventType.CLICK,
        event_ts=event_ts,
        metadata=EventMetadata(
            session_id=session_id,
            page=page,
            product_id="prod_1001",
            category="electronics",
        ),
    )


def test_validation_summary_detects_duplicate_like_rows_and_nulls(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.quality.checks.RAW_DB_PATH", tmp_path / "raw_events.duckdb")

    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    event = _event("evt_1", now - timedelta(minutes=5), "s1")
    persist_raw_event(event)

    # Insert a duplicate-like row with a different event id but same event fingerprint.
    with duckdb.connect(str(raw_event_store.RAW_DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO raw_events (
                event_id,
                entity_id,
                event_type,
                event_ts,
                schema_version,
                session_id,
                page,
                product_id,
                category,
                price,
                attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "evt_2",
                "user_0001",
                "click",
                event.event_ts,
                "1.0.0",
                "s1",
                "/product",
                "prod_1001",
                "electronics",
                None,
                "{}",
            ],
        )
        conn.execute(
            """
            INSERT INTO raw_events (
                event_id,
                entity_id,
                event_type,
                event_ts,
                schema_version,
                session_id,
                page,
                product_id,
                category,
                price,
                attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "evt_3",
                "user_0002",
                "page_view",
                now,
                "1.0.0",
                "s2",
                None,
                "prod_1002",
                "fashion",
                None,
                "{}",
            ],
        )

    summary = build_quality_summary()

    assert summary["validations"]["status"] == "warning"
    assert summary["validations"]["potential_duplicate_event_count"] == 1
    assert summary["validations"]["null_page_count"] == 1


def test_validation_summary_detects_unsupported_schema_versions(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.raw_event_store.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr("src.quality.checks.RAW_DB_PATH", tmp_path / "raw_events.duckdb")
    monkeypatch.setattr(settings, "supported_schema_versions", "1.0.0")

    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    persist_raw_event(_event("evt_1", now - timedelta(minutes=5), "s1"))

    with duckdb.connect(str(raw_event_store.RAW_DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO raw_events (
                event_id,
                entity_id,
                event_type,
                event_ts,
                schema_version,
                session_id,
                page,
                product_id,
                category,
                price,
                attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "evt_unsupported",
                "user_0003",
                "click",
                now,
                "2.0.0",
                "s3",
                "/product",
                "prod_1003",
                "fashion",
                None,
                "{}",
            ],
        )

    summary = build_quality_summary()

    assert summary["validations"]["status"] == "warning"
    assert summary["validations"]["unsupported_schema_version_count"] == 1
    assert summary["validations"]["schema_versions_seen"] == ["1.0.0", "2.0.0"]
    assert summary["validations"]["unsupported_schema_versions"] == ["2.0.0"]

from pathlib import Path

import duckdb

from src.connectors.schemas import UserEvent


RAW_DB_PATH = Path("data/generated/raw_events.duckdb")


def _connect() -> duckdb.DuckDBPyConnection:
    RAW_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(RAW_DB_PATH))


def initialize_raw_events_table() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id VARCHAR PRIMARY KEY,
                entity_id VARCHAR,
                event_type VARCHAR,
                event_ts TIMESTAMP,
                schema_version VARCHAR,
                session_id VARCHAR,
                page VARCHAR,
                product_id VARCHAR,
                category VARCHAR,
                price DOUBLE,
                attributes_json VARCHAR
            )
            """
        )


def persist_raw_event(event: UserEvent) -> None:
    initialize_raw_events_table()
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO raw_events (
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
                event.event_id,
                event.entity_id,
                event.event_type.value,
                event.event_ts,
                event.schema_version,
                event.metadata.session_id,
                event.metadata.page,
                event.metadata.product_id,
                event.metadata.category,
                event.metadata.price,
                str(event.metadata.attributes),
            ],
        )


def raw_event_count() -> int:
    initialize_raw_events_table()
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]

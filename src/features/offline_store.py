from pathlib import Path

import duckdb

from src.features.definitions import FeatureSnapshot


FEATURE_DB_PATH = Path("data/generated/features.duckdb")


def _connect() -> duckdb.DuckDBPyConnection:
    FEATURE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(FEATURE_DB_PATH))


def initialize_feature_table() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                entity_id VARCHAR,
                as_of_ts TIMESTAMP,
                feature_version VARCHAR,
                clicks_1h INTEGER,
                sessions_24h INTEGER,
                purchase_value_24h DOUBLE,
                last_active_ts TIMESTAMP,
                click_to_impression_ratio_24h DOUBLE,
                dominant_category VARCHAR,
                dominant_category_share DOUBLE
            )
            """
        )


def persist_feature_snapshot(snapshot: FeatureSnapshot) -> None:
    initialize_feature_table()
    with _connect() as conn:
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


def feature_snapshot_count() -> int:
    initialize_feature_table()
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()[0]


def compact_feature_snapshots() -> None:
    initialize_feature_table()
    with _connect() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE feature_snapshots AS
            SELECT
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
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY entity_id, as_of_ts, feature_version
                        ORDER BY last_active_ts DESC
                    ) AS rn
                FROM feature_snapshots
            )
            WHERE rn = 1
            """
        )


def latest_feature_snapshot(entity_id: str) -> FeatureSnapshot | None:
    initialize_feature_table()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
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
            FROM feature_snapshots
            WHERE entity_id = ?
            ORDER BY as_of_ts DESC
            LIMIT 1
            """,
            [entity_id],
        ).fetchone()

    if row is None:
        return None

    return FeatureSnapshot(
        entity_id=row[0],
        as_of_ts=row[1],
        feature_version=row[2],
        clicks_1h=row[3],
        sessions_24h=row[4],
        purchase_value_24h=row[5],
        last_active_ts=row[6],
        click_to_impression_ratio_24h=row[7],
        dominant_category=row[8],
        dominant_category_share=row[9],
    )

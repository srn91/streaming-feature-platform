from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb

from src.config import settings
from src.features.offline_store import FEATURE_DB_PATH, initialize_feature_table
from src.features.online_store import read_feature_snapshot
from src.pipelines.raw_event_store import RAW_DB_PATH, initialize_raw_events_table


@dataclass
class RawEventSummary:
    total_events: int
    distinct_entities: int
    min_event_ts: str | None
    max_event_ts: str | None


@dataclass
class FeatureSummary:
    total_feature_snapshots: int
    distinct_entities: int
    latest_as_of_ts: str | None
    latest_snapshot_entities: int


@dataclass
class ValidationSummary:
    status: str
    duplicate_event_id_count: int
    potential_duplicate_event_count: int
    null_event_id_count: int
    null_entity_id_count: int
    null_event_type_count: int
    null_event_ts_count: int
    null_schema_version_count: int
    null_session_id_count: int
    null_page_count: int
    unsupported_schema_version_count: int
    schema_versions_seen: list[str]
    unsupported_schema_versions: list[str]


@dataclass
class SchemaCompatibilitySummary:
    supported_versions: list[str]
    schema_versions_seen: list[str]
    compatible_versions: list[str]
    incompatible_versions: list[str]
    compatible_event_count: int
    incompatible_event_count: int
    status: str
    recommended_action: str


def _supported_schema_versions() -> set[str]:
    return {
        version.strip()
        for version in settings.supported_schema_versions.split(",")
        if version.strip()
    }


def _format_ts(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def raw_event_summary() -> RawEventSummary:
    initialize_raw_events_table()
    with duckdb.connect(str(RAW_DB_PATH), read_only=False) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_events,
                COUNT(DISTINCT entity_id) AS distinct_entities,
                MIN(event_ts) AS min_event_ts,
                MAX(event_ts) AS max_event_ts
            FROM raw_events
            """
        ).fetchone()

    return RawEventSummary(
        total_events=row[0],
        distinct_entities=row[1],
        min_event_ts=_format_ts(row[2]),
        max_event_ts=_format_ts(row[3]),
    )


def feature_summary() -> FeatureSummary:
    initialize_feature_table()
    with duckdb.connect(str(FEATURE_DB_PATH), read_only=False) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_feature_snapshots,
                COUNT(DISTINCT entity_id) AS distinct_entities,
                MAX(as_of_ts) AS latest_as_of_ts
            FROM feature_snapshots
            """
        ).fetchone()

        latest_snapshot_entities = conn.execute(
            """
            SELECT COUNT(*)
            FROM feature_snapshots
            WHERE as_of_ts = (SELECT MAX(as_of_ts) FROM feature_snapshots)
            """
        ).fetchone()[0]

    return FeatureSummary(
        total_feature_snapshots=row[0],
        distinct_entities=row[1],
        latest_as_of_ts=_format_ts(row[2]),
        latest_snapshot_entities=int(latest_snapshot_entities or 0),
    )


def validation_summary() -> ValidationSummary:
    initialize_raw_events_table()
    with duckdb.connect(str(RAW_DB_PATH), read_only=False) as conn:
        duplicate_event_id_count = conn.execute(
            """
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT event_id, COUNT(*) AS cnt
                FROM raw_events
                GROUP BY event_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        potential_duplicate_event_count = conn.execute(
            """
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT
                    entity_id,
                    event_type,
                    event_ts,
                    session_id,
                    COALESCE(product_id, '') AS product_id,
                    COUNT(*) AS cnt
                FROM raw_events
                GROUP BY entity_id, event_type, event_ts, session_id, COALESCE(product_id, '')
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]

        null_counts = conn.execute(
            """
            SELECT
                SUM(CASE WHEN event_id IS NULL OR event_id = '' THEN 1 ELSE 0 END) AS null_event_id_count,
                SUM(CASE WHEN entity_id IS NULL OR entity_id = '' THEN 1 ELSE 0 END) AS null_entity_id_count,
                SUM(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 ELSE 0 END) AS null_event_type_count,
                SUM(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END) AS null_event_ts_count,
                SUM(CASE WHEN schema_version IS NULL OR schema_version = '' THEN 1 ELSE 0 END) AS null_schema_version_count,
                SUM(CASE WHEN session_id IS NULL OR session_id = '' THEN 1 ELSE 0 END) AS null_session_id_count,
                SUM(CASE WHEN page IS NULL OR page = '' THEN 1 ELSE 0 END) AS null_page_count
            FROM raw_events
            """
        ).fetchone()

        schema_versions_seen = [
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT schema_version
                FROM raw_events
                WHERE schema_version IS NOT NULL AND schema_version != ''
                ORDER BY schema_version
                """
            ).fetchall()
        ]

        supported_versions = _supported_schema_versions()
        unsupported_schema_versions = [
            version for version in schema_versions_seen if version not in supported_versions
        ]
        unsupported_schema_version_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM raw_events
            WHERE schema_version IS NOT NULL
              AND schema_version != ''
              AND schema_version IN ({placeholders})
            """.format(
                placeholders=", ".join("?" for _ in unsupported_schema_versions)
            ),
            unsupported_schema_versions,
        ).fetchone()[0] if unsupported_schema_versions else 0

    issues = (
        duplicate_event_id_count
        + potential_duplicate_event_count
        + unsupported_schema_version_count
        + sum(int(value or 0) for value in null_counts)
    )

    return ValidationSummary(
        status="ok" if issues == 0 else "warning",
        duplicate_event_id_count=int(duplicate_event_id_count or 0),
        potential_duplicate_event_count=int(potential_duplicate_event_count or 0),
        null_event_id_count=int(null_counts[0] or 0),
        null_entity_id_count=int(null_counts[1] or 0),
        null_event_type_count=int(null_counts[2] or 0),
        null_event_ts_count=int(null_counts[3] or 0),
        null_schema_version_count=int(null_counts[4] or 0),
        null_session_id_count=int(null_counts[5] or 0),
        null_page_count=int(null_counts[6] or 0),
        unsupported_schema_version_count=int(unsupported_schema_version_count or 0),
        schema_versions_seen=schema_versions_seen,
        unsupported_schema_versions=unsupported_schema_versions,
    )


def schema_compatibility_summary() -> SchemaCompatibilitySummary:
    initialize_raw_events_table()
    supported_versions = sorted(_supported_schema_versions())
    with duckdb.connect(str(RAW_DB_PATH), read_only=False) as conn:
        schema_versions_seen = [
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT schema_version
                FROM raw_events
                WHERE schema_version IS NOT NULL AND schema_version != ''
                ORDER BY schema_version
                """
            ).fetchall()
        ]

        compatible_versions = [version for version in schema_versions_seen if version in supported_versions]
        incompatible_versions = [version for version in schema_versions_seen if version not in supported_versions]

        compatible_event_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM raw_events
            WHERE schema_version IN ({placeholders})
            """.format(placeholders=", ".join("?" for _ in compatible_versions)),
            compatible_versions,
        ).fetchone()[0] if compatible_versions else 0

        incompatible_event_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM raw_events
            WHERE schema_version IS NOT NULL
              AND schema_version != ''
              AND schema_version IN ({placeholders})
            """.format(placeholders=", ".join("?" for _ in incompatible_versions)),
            incompatible_versions,
        ).fetchone()[0] if incompatible_versions else 0

    status = "ok" if not incompatible_versions else "warning"
    recommended_action = (
        "All observed schema versions are supported."
        if status == "ok"
        else "Block new producers until the unsupported schema versions are remediated."
    )

    return SchemaCompatibilitySummary(
        supported_versions=supported_versions,
        schema_versions_seen=schema_versions_seen,
        compatible_versions=compatible_versions,
        incompatible_versions=incompatible_versions,
        compatible_event_count=int(compatible_event_count or 0),
        incompatible_event_count=int(incompatible_event_count or 0),
        status=status,
        recommended_action=recommended_action,
    )


def _latest_feature_rows(limit: int = 25) -> list[tuple]:
    with duckdb.connect(str(FEATURE_DB_PATH), read_only=False) as conn:
        return conn.execute(
            """
            WITH ranked AS (
                SELECT
                    entity_id,
                    as_of_ts,
                    clicks_1h,
                    sessions_24h,
                    purchase_value_24h,
                    click_to_impression_ratio_24h,
                    dominant_category,
                    dominant_category_share,
                    ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY as_of_ts DESC) AS rn
                FROM feature_snapshots
            )
            SELECT
                entity_id,
                as_of_ts,
                clicks_1h,
                sessions_24h,
                purchase_value_24h,
                click_to_impression_ratio_24h,
                dominant_category,
                dominant_category_share
            FROM ranked
            WHERE rn = 1
            ORDER BY entity_id
            LIMIT ?
            """,
            [limit],
        ).fetchall()


def online_offline_reconciliation(limit: int = 25) -> dict[str, Any]:
    try:
        rows = _latest_feature_rows(limit=limit)
    except duckdb.Error as exc:
        return {
            "status": "unavailable",
            "reason": f"offline feature store not ready: {exc}",
            "checked_entities": 0,
        }

    if not rows:
        return {"status": "empty", "checked_entities": 0}

    checked = 0
    mismatches: list[dict[str, Any]] = []
    online_found = 0

    for row in rows:
        entity_id = row[0]
        try:
            online_snapshot = read_feature_snapshot(entity_id)
        except ModuleNotFoundError:
            return {
                "status": "skipped",
                "reason": "redis client is not installed in the current interpreter",
                "checked_entities": checked,
            }
        except Exception as exc:
            return {
                "status": "skipped",
                "reason": f"unable to read online feature store: {exc}",
                "checked_entities": checked,
            }
        checked += 1
        if online_snapshot is None:
            continue

        online_found += 1
        offline_projection = {
            "clicks_1h": row[2],
            "sessions_24h": row[3],
            "purchase_value_24h": round(float(row[4]), 2),
            "click_to_impression_ratio_24h": round(float(row[5]), 4),
            "dominant_category": row[6],
            "dominant_category_share": round(float(row[7]), 4),
        }
        online_projection = {
            "clicks_1h": online_snapshot.clicks_1h,
            "sessions_24h": online_snapshot.sessions_24h,
            "purchase_value_24h": round(float(online_snapshot.purchase_value_24h), 2),
            "click_to_impression_ratio_24h": round(float(online_snapshot.click_to_impression_ratio_24h), 4),
            "dominant_category": online_snapshot.dominant_category,
            "dominant_category_share": round(float(online_snapshot.dominant_category_share), 4),
        }

        if offline_projection != online_projection:
            mismatches.append(
                {
                    "entity_id": entity_id,
                    "offline": offline_projection,
                    "online": online_projection,
                }
            )

    if online_found == 0:
        return {
            "status": "skipped",
            "reason": "no online feature snapshots found in Redis",
            "checked_entities": checked,
        }

    return {
        "status": "ok" if not mismatches else "mismatch",
        "checked_entities": checked,
        "online_entities_found": online_found,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:5],
    }


def freshness_summary() -> dict[str, Any]:
    raw = raw_event_summary()
    features = feature_summary()

    if raw.max_event_ts is None or features.latest_as_of_ts is None:
        return {"status": "unavailable"}

    raw_ts = datetime.fromisoformat(raw.max_event_ts)
    feature_ts = datetime.fromisoformat(features.latest_as_of_ts)
    lag_seconds = abs((raw_ts - feature_ts).total_seconds())
    warning_threshold = settings.freshness_warning_lag_seconds
    error_threshold = settings.freshness_error_lag_seconds

    if lag_seconds >= error_threshold:
        status = "error"
    elif lag_seconds >= warning_threshold:
        status = "warning"
    else:
        status = "ok"

    return {
        "status": status,
        "raw_event_max_ts": raw.max_event_ts,
        "feature_max_ts": features.latest_as_of_ts,
        "lag_seconds": lag_seconds,
        "warning_lag_seconds": warning_threshold,
        "error_lag_seconds": error_threshold,
    }


def build_quality_summary() -> dict[str, Any]:
    raw = raw_event_summary()
    features = feature_summary()
    validations = validation_summary()
    schema_compatibility = schema_compatibility_summary()
    return {
        "raw_events": asdict(raw),
        "features": asdict(features),
        "validations": asdict(validations),
        "schema_compatibility": asdict(schema_compatibility),
        "freshness": freshness_summary(),
        "reconciliation": online_offline_reconciliation(),
    }

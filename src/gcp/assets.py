from __future__ import annotations

import base64
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path

from src.config import settings
from src.connectors.generate_sample_events import generate_events
from src.connectors.schemas import EventType, UserEvent
from src.features.definitions import FeatureSnapshot


@dataclass(frozen=True)
class GcpDryRunSummary:
    output_dir: str
    project_id: str
    pubsub_topic: str
    pubsub_messages: int
    raw_event_rows: int
    feature_snapshot_rows: int
    files: dict[str, str]


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _event_payload(event: UserEvent) -> dict[str, object]:
    payload = event.model_dump(mode="json")
    payload["metadata"]["attributes_json"] = json.dumps(
        payload["metadata"].get("attributes", {}),
        sort_keys=True,
    )
    return payload


def build_pubsub_messages(events: list[UserEvent]) -> list[dict[str, object]]:
    topic_path = f"projects/{settings.gcp_project_id}/topics/{settings.gcp_pubsub_topic}"
    messages: list[dict[str, object]] = []
    for event in events:
        payload = _event_payload(event)
        payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
        messages.append(
            {
                "topic_path": topic_path,
                "ordering_key": event.entity_id,
                "message_id": event.event_id,
                "attributes": {
                    "entity_id": event.entity_id,
                    "event_type": event.event_type.value,
                    "schema_version": event.schema_version,
                    "category": event.metadata.category,
                },
                "data_base64": base64.b64encode(payload_bytes).decode("utf-8"),
                "payload": payload,
            }
        )
    return messages


def build_raw_event_rows(events: list[UserEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.event_id,
                "entity_id": event.entity_id,
                "event_type": event.event_type.value,
                "event_ts": event.event_ts.isoformat(),
                "schema_version": event.schema_version,
                "session_id": event.metadata.session_id,
                "page": event.metadata.page,
                "product_id": event.metadata.product_id,
                "category": event.metadata.category,
                "price": event.metadata.price,
                "attributes_json": json.dumps(event.metadata.attributes, sort_keys=True),
                "source": "pubsub_dry_run",
            }
        )
    return rows


def build_feature_snapshots_from_events(events: list[UserEvent]) -> list[FeatureSnapshot]:
    if not events:
        return []

    as_of_ts = max(event.event_ts for event in events)
    last_1h_cutoff = as_of_ts - timedelta(hours=1)
    last_24h_cutoff = as_of_ts - timedelta(hours=24)
    snapshots: list[FeatureSnapshot] = []

    grouped: dict[str, list[UserEvent]] = defaultdict(list)
    for event in events:
        grouped[event.entity_id].append(event)

    for entity_id, entity_events in grouped.items():
        events_1h = [event for event in entity_events if event.event_ts >= last_1h_cutoff]
        events_24h = [event for event in entity_events if event.event_ts >= last_24h_cutoff]

        clicks_1h = sum(1 for event in events_1h if event.event_type == EventType.CLICK)
        clicks_24h = sum(1 for event in events_24h if event.event_type == EventType.CLICK)
        impressions_24h = sum(1 for event in events_24h if event.event_type == EventType.PAGE_VIEW)
        sessions_24h = len({event.metadata.session_id for event in events_24h})
        purchase_value_24h = round(
            sum(float(event.metadata.price or 0.0) for event in events_24h if event.event_type == EventType.PURCHASE),
            2,
        )
        categories = [event.metadata.category for event in events_24h if event.metadata.category]
        if categories:
            counts: dict[str, int] = defaultdict(int)
            for category in categories:
                counts[category] += 1
            dominant_category = max(counts, key=counts.get)
            dominant_category_share = round(counts[dominant_category] / sum(counts.values()), 4)
        else:
            dominant_category = None
            dominant_category_share = 0.0
        ratio = round(clicks_24h / impressions_24h, 4) if impressions_24h else 0.0

        snapshots.append(
            FeatureSnapshot(
                entity_id=entity_id,
                as_of_ts=as_of_ts,
                clicks_1h=clicks_1h,
                sessions_24h=sessions_24h,
                purchase_value_24h=purchase_value_24h,
                last_active_ts=max(event.event_ts for event in entity_events),
                click_to_impression_ratio_24h=ratio,
                dominant_category=dominant_category,
                dominant_category_share=dominant_category_share,
            )
        )

    return sorted(snapshots, key=lambda snapshot: snapshot.entity_id)


def build_feature_snapshot_rows(snapshots: list[FeatureSnapshot]) -> list[dict[str, object]]:
    return [
        {
            "entity_id": snapshot.entity_id,
            "as_of_ts": snapshot.as_of_ts.isoformat(),
            "feature_version": snapshot.feature_version,
            "clicks_1h": snapshot.clicks_1h,
            "sessions_24h": snapshot.sessions_24h,
            "purchase_value_24h": snapshot.purchase_value_24h,
            "last_active_ts": snapshot.last_active_ts.isoformat(),
            "click_to_impression_ratio_24h": snapshot.click_to_impression_ratio_24h,
            "dominant_category": snapshot.dominant_category,
            "dominant_category_share": snapshot.dominant_category_share,
            "source": "feature_materialization_dry_run",
        }
        for snapshot in snapshots
    ]


def render_bigquery_raw_events_sql() -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{settings.gcp_project_id}.{settings.gcp_bigquery_dataset}.{settings.gcp_bigquery_raw_events_table}` (
  event_id STRING,
  entity_id STRING,
  event_type STRING,
  event_ts TIMESTAMP,
  schema_version STRING,
  session_id STRING,
  page STRING,
  product_id STRING,
  category STRING,
  price FLOAT64,
  attributes_json STRING,
  source STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY entity_id, event_type;"""


def render_bigquery_feature_snapshots_sql() -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{settings.gcp_project_id}.{settings.gcp_bigquery_dataset}.{settings.gcp_bigquery_feature_snapshots_table}` (
  entity_id STRING,
  as_of_ts TIMESTAMP,
  feature_version STRING,
  clicks_1h INT64,
  sessions_24h INT64,
  purchase_value_24h FLOAT64,
  last_active_ts TIMESTAMP,
  click_to_impression_ratio_24h FLOAT64,
  dominant_category STRING,
  dominant_category_share FLOAT64,
  source STRING
)
PARTITION BY DATE(as_of_ts)
CLUSTER BY entity_id;"""


def build_pubsub_topic_spec() -> dict[str, object]:
    return {
        "project_id": settings.gcp_project_id,
        "topic_id": settings.gcp_pubsub_topic,
        "message_format": "json",
        "ordering_enabled": True,
        "ordering_key_field": "entity_id",
        "payload_shape": "UserEvent",
        "subscription_hint": f"{settings.gcp_pubsub_topic}-dry-run-subscription",
    }


def _write_assets(
    output_dir: Path,
    events: list[UserEvent],
    snapshots: list[FeatureSnapshot],
) -> GcpDryRunSummary:
    output_dir.mkdir(parents=True, exist_ok=True)
    pubsub_messages = build_pubsub_messages(events)
    raw_event_rows = build_raw_event_rows(events)
    feature_rows = build_feature_snapshot_rows(snapshots)

    files = {
        "pubsub_topic": str(output_dir / "pubsub_topic.json"),
        "pubsub_messages": str(output_dir / "pubsub_messages.jsonl"),
        "raw_events_sql": str(output_dir / "bigquery_raw_events.sql"),
        "feature_snapshots_sql": str(output_dir / "bigquery_feature_snapshots.sql"),
        "raw_event_rows": str(output_dir / "bigquery_raw_events.jsonl"),
        "feature_snapshot_rows": str(output_dir / "bigquery_feature_snapshots.jsonl"),
        "summary": str(output_dir / "dry_run_summary.json"),
    }

    (output_dir / "pubsub_topic.json").write_text(
        json.dumps(build_pubsub_topic_spec(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_jsonl(output_dir / "pubsub_messages.jsonl", pubsub_messages)
    (output_dir / "bigquery_raw_events.sql").write_text(render_bigquery_raw_events_sql(), encoding="utf-8")
    (output_dir / "bigquery_feature_snapshots.sql").write_text(
        render_bigquery_feature_snapshots_sql(),
        encoding="utf-8",
    )
    _write_jsonl(output_dir / "bigquery_raw_events.jsonl", raw_event_rows)
    _write_jsonl(output_dir / "bigquery_feature_snapshots.jsonl", feature_rows)

    summary = GcpDryRunSummary(
        output_dir=str(output_dir),
        project_id=settings.gcp_project_id,
        pubsub_topic=settings.gcp_pubsub_topic,
        pubsub_messages=len(pubsub_messages),
        raw_event_rows=len(raw_event_rows),
        feature_snapshot_rows=len(feature_rows),
        files=files,
    )
    (output_dir / "dry_run_summary.json").write_text(json.dumps(asdict(summary), indent=2, sort_keys=True), encoding="utf-8")
    return summary


def run_gcp_dry_run(
    *,
    output_dir: str | Path | None = None,
    num_users: int | None = None,
    events_per_user: int | None = None,
    events: list[UserEvent] | None = None,
) -> dict[str, object]:
    if events is None:
        events = generate_events(
            num_users=num_users or settings.demo_seed_users,
            events_per_user=events_per_user or settings.demo_events_per_user,
        )
    snapshots = build_feature_snapshots_from_events(events)
    summary = _write_assets(Path(output_dir or settings.gcp_asset_output_dir), events, snapshots)
    return asdict(summary)

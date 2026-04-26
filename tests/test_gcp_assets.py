from __future__ import annotations

import json
from pathlib import Path

from src.connectors.generate_sample_events import generate_events
from src.gcp.assets import (
    build_feature_snapshots_from_events,
    build_pubsub_messages,
    render_bigquery_feature_snapshots_sql,
    render_bigquery_raw_events_sql,
    run_gcp_dry_run,
)


def test_gcp_dry_run_writes_pubsub_and_bigquery_assets(tmp_path) -> None:
    events = generate_events(num_users=2, events_per_user=3)
    summary = run_gcp_dry_run(output_dir=tmp_path / "gcp", events=events)

    assert summary["pubsub_messages"] == 6
    assert summary["raw_event_rows"] == 6
    assert summary["feature_snapshot_rows"] == 2

    files = summary["files"]
    pubsub_messages_path = Path(files["pubsub_messages"])
    raw_events_sql_path = Path(files["raw_events_sql"])
    feature_snapshots_sql_path = Path(files["feature_snapshots_sql"])
    raw_event_rows_path = Path(files["raw_event_rows"])
    feature_snapshot_rows_path = Path(files["feature_snapshot_rows"])
    topic_path = Path(files["pubsub_topic"])
    summary_path = Path(files["summary"])

    assert pubsub_messages_path.exists()
    assert raw_events_sql_path.exists()
    assert feature_snapshots_sql_path.exists()
    assert raw_event_rows_path.exists()
    assert feature_snapshot_rows_path.exists()
    assert topic_path.exists()
    assert summary_path.exists()

    first_message = json.loads(pubsub_messages_path.read_text().splitlines()[0])
    assert first_message["ordering_key"] == events[0].entity_id
    assert first_message["attributes"]["event_type"] == events[0].event_type.value
    assert first_message["topic_path"].endswith("/topics/feature-events")

    topic_spec = json.loads(topic_path.read_text())
    assert topic_spec["ordering_enabled"] is True
    assert topic_spec["payload_shape"] == "UserEvent"

    raw_events_sql = raw_events_sql_path.read_text()
    feature_snapshots_sql = feature_snapshots_sql_path.read_text()
    assert "PARTITION BY DATE(event_ts)" in raw_events_sql
    assert "CLUSTER BY entity_id, event_type" in raw_events_sql
    assert "PARTITION BY DATE(as_of_ts)" in feature_snapshots_sql
    assert "CLUSTER BY entity_id" in feature_snapshots_sql

    summary_payload = json.loads(summary_path.read_text())
    assert summary_payload["pubsub_messages"] == 6
    assert summary_payload["feature_snapshot_rows"] == 2


def test_feature_snapshot_builder_tracks_all_entities() -> None:
    events = generate_events(num_users=3, events_per_user=2)
    snapshots = build_feature_snapshots_from_events(events)
    pubsub_messages = build_pubsub_messages(events)

    assert len(snapshots) == 3
    assert len(pubsub_messages) == 6
    assert {snapshot.entity_id for snapshot in snapshots} == {"user_0001", "user_0002", "user_0003"}
    assert render_bigquery_raw_events_sql().startswith("CREATE TABLE IF NOT EXISTS")
    assert render_bigquery_feature_snapshots_sql().startswith("CREATE TABLE IF NOT EXISTS")

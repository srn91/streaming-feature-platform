from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from src.connectors.generate_sample_events import generate_events, write_jsonl
from src.features.materialize_features import materialize_features
from src.features.offline_store import feature_snapshot_count
from src.pipelines.raw_event_store import raw_event_count, persist_raw_event
from src.training.export_dataset import export_training_dataset


@dataclass(frozen=True)
class DemoBootstrapResult:
    seeded_events: int
    raw_event_count: int
    feature_snapshot_count: int
    sample_events_path: str
    materialized_features: int
    training_dataset_rows: int


def bootstrap_hosted_demo(
    *,
    num_users: int = 12,
    events_per_user: int = 16,
    sample_events_path: str | Path = "data/generated/sample_events.jsonl",
) -> DemoBootstrapResult:
    path = Path(sample_events_path)
    seeded_events = 0

    if raw_event_count() == 0:
        events = generate_events(num_users=num_users, events_per_user=events_per_user)
        write_jsonl(path, events)
        for event in events:
            persist_raw_event(event)
        seeded_events = len(events)
    elif not path.exists():
        # Keep a deterministic fixture on disk for inspection even if the database already exists.
        events = generate_events(num_users=num_users, events_per_user=events_per_user)
        write_jsonl(path, events)

    materialized_features = 0
    if raw_event_count() > 0 and feature_snapshot_count() == 0:
        materialized_features = materialize_features(write_online=False)
    training_dataset = export_training_dataset()

    return DemoBootstrapResult(
        seeded_events=seeded_events,
        raw_event_count=raw_event_count(),
        feature_snapshot_count=feature_snapshot_count(),
        sample_events_path=str(path),
        materialized_features=materialized_features,
        training_dataset_rows=training_dataset.rows,
    )


def bootstrap_hosted_demo_as_dict(**kwargs) -> dict[str, object]:
    return asdict(bootstrap_hosted_demo(**kwargs))

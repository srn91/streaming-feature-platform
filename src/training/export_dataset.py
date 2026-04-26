from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb
import pandas as pd

from src.features import offline_store
from src.pipelines import raw_event_store


TRAINING_DATASET_PATH = Path("data/generated/training_dataset.csv")


@dataclass(frozen=True)
class TrainingDatasetSummary:
    path: str
    rows: int
    positive_labels: int
    label_rate: float
    as_of_ts: str | None


def build_training_dataset() -> pd.DataFrame:
    raw_event_store.initialize_raw_events_table()
    offline_store.initialize_feature_table()
    with duckdb.connect(str(offline_store.FEATURE_DB_PATH), read_only=False) as feature_conn:
        feature_snapshots = feature_conn.execute(
            """
            SELECT
                entity_id,
                as_of_ts,
                feature_version,
                clicks_1h,
                sessions_24h,
                purchase_value_24h,
                click_to_impression_ratio_24h,
                dominant_category,
                dominant_category_share
            FROM feature_snapshots
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY entity_id
                ORDER BY as_of_ts DESC
            ) = 1
            ORDER BY entity_id
            """
        ).fetch_df()
    with duckdb.connect(str(raw_event_store.RAW_DB_PATH), read_only=False) as raw_conn:
        labels = raw_conn.execute(
            """
            SELECT
                entity_id,
                MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased_24h,
                ROUND(SUM(COALESCE(price, 0.0)), 2) AS purchase_value_24h_label
            FROM raw_events
            GROUP BY entity_id
            ORDER BY entity_id
            """
        ).fetch_df()
    dataset = feature_snapshots.merge(labels, on="entity_id", how="left")
    dataset["purchased_24h"] = dataset["purchased_24h"].fillna(0).astype(int)
    dataset["purchase_value_24h_label"] = dataset["purchase_value_24h_label"].fillna(0.0).round(2)
    return dataset


def export_training_dataset() -> TrainingDatasetSummary:
    dataset = build_training_dataset()
    TRAINING_DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(TRAINING_DATASET_PATH, index=False)
    rows = len(dataset.index)
    positives = int(dataset["purchased_24h"].sum()) if rows else 0
    as_of_ts = None if rows == 0 else str(dataset["as_of_ts"].max())
    return TrainingDatasetSummary(
        path=str(TRAINING_DATASET_PATH),
        rows=rows,
        positive_labels=positives,
        label_rate=round((positives / rows), 4) if rows else 0.0,
        as_of_ts=as_of_ts,
    )


def export_training_dataset_as_dict() -> dict[str, object]:
    return asdict(export_training_dataset())


if __name__ == "__main__":
    print(export_training_dataset_as_dict())

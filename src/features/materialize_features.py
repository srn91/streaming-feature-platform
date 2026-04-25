from datetime import timedelta

import duckdb
import pandas as pd

from src.connectors.schemas import EventType
from src.features.definitions import FeatureSnapshot
from src.features.offline_store import compact_feature_snapshots, persist_feature_snapshot
from src.features.online_store import write_feature_snapshot
from src.pipelines import raw_event_store


def _load_raw_events() -> pd.DataFrame:
    raw_event_store.initialize_raw_events_table()
    with duckdb.connect(str(raw_event_store.RAW_DB_PATH)) as conn:
        return conn.execute("SELECT * FROM raw_events ORDER BY event_ts").fetch_df()


def _category_stats(events_24h: pd.DataFrame) -> tuple[str | None, float]:
    categorized = events_24h.dropna(subset=["category"])
    if categorized.empty:
        return None, 0.0

    counts = categorized["category"].value_counts()
    top_category = counts.index[0]
    share = float(counts.iloc[0] / counts.sum())
    return top_category, round(share, 4)


def build_feature_snapshots() -> list[FeatureSnapshot]:
    raw_events = _load_raw_events()
    if raw_events.empty:
        return []

    raw_events["event_ts"] = pd.to_datetime(raw_events["event_ts"], utc=False)
    as_of_ts = raw_events["event_ts"].max().to_pydatetime()
    last_1h_cutoff = as_of_ts - timedelta(hours=1)
    last_24h_cutoff = as_of_ts - timedelta(hours=24)

    snapshots: list[FeatureSnapshot] = []

    for entity_id, entity_events in raw_events.groupby("entity_id"):
        events_1h = entity_events[entity_events["event_ts"] >= last_1h_cutoff]
        events_24h = entity_events[entity_events["event_ts"] >= last_24h_cutoff]

        clicks_1h = int((events_1h["event_type"] == EventType.CLICK.value).sum())
        clicks_24h = int((events_24h["event_type"] == EventType.CLICK.value).sum())
        impressions_24h = int((events_24h["event_type"] == EventType.PAGE_VIEW.value).sum())
        sessions_24h = int(events_24h["session_id"].nunique())
        purchase_value_24h = round(
            float(
                events_24h.loc[events_24h["event_type"] == EventType.PURCHASE.value, "price"]
                .fillna(0.0)
                .sum()
            ),
            2,
        )
        dominant_category, dominant_category_share = _category_stats(events_24h)
        ratio = round(clicks_24h / impressions_24h, 4) if impressions_24h else 0.0

        snapshots.append(
            FeatureSnapshot(
                entity_id=entity_id,
                as_of_ts=as_of_ts,
                clicks_1h=clicks_1h,
                sessions_24h=sessions_24h,
                purchase_value_24h=purchase_value_24h,
                last_active_ts=entity_events["event_ts"].max().to_pydatetime(),
                click_to_impression_ratio_24h=ratio,
                dominant_category=dominant_category,
                dominant_category_share=dominant_category_share,
            )
        )

    return snapshots


def materialize_features(write_online: bool = False) -> int:
    snapshots = build_feature_snapshots()
    for snapshot in snapshots:
        persist_feature_snapshot(snapshot)
        if write_online:
            write_feature_snapshot(snapshot)
    compact_feature_snapshots()
    return len(snapshots)


if __name__ == "__main__":
    count = materialize_features(write_online=True)
    print(f"Materialized {count} feature snapshots")

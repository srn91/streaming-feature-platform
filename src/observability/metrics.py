from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


REQUEST_COUNT = Counter(
    "streaming_feature_platform_http_requests_total",
    "HTTP requests served by the feature platform API",
    ["method", "path", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "streaming_feature_platform_http_request_duration_seconds",
    "HTTP request latency for the feature platform API",
    ["method", "path"],
)
RAW_EVENTS_GAUGE = Gauge(
    "streaming_feature_platform_raw_events_total",
    "Total raw events currently persisted in the local raw event store",
)
FEATURE_SNAPSHOTS_GAUGE = Gauge(
    "streaming_feature_platform_feature_snapshots_total",
    "Total feature snapshots currently persisted in the offline store",
)
TRAINING_ROWS_GAUGE = Gauge(
    "streaming_feature_platform_training_dataset_rows",
    "Total rows available in the exported training dataset",
)


def observe_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    REQUEST_COUNT.labels(method=method, path=path, status_code=str(status_code)).inc()
    REQUEST_LATENCY.labels(method=method, path=path).observe(duration_seconds)


def update_quality_metrics(summary: dict[str, object]) -> None:
    RAW_EVENTS_GAUGE.set(int(summary["raw_events"]["total_events"]))
    FEATURE_SNAPSHOTS_GAUGE.set(int(summary["features"]["total_feature_snapshots"]))


def update_training_metrics(summary: dict[str, object]) -> None:
    TRAINING_ROWS_GAUGE.set(int(summary["rows"]))


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST

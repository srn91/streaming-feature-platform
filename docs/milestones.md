# Service Walkthrough

## Infrastructure layer

- Docker Compose wires the local Redpanda, Redis, and API stack
- the `Dockerfile` packages the hosted-demo service path
- Kubernetes, AWS ECS, Azure Container Apps, and Jenkins assets live under `infra/` and the repo root

## Streaming and storage layer

- the producer generates deterministic sample events
- the consumer persists raw events into DuckDB
- materialization writes offline snapshots and updates Redis

## Quality and reconciliation layer

- schema version checks gate incompatible events
- null, duplicate, freshness, and reconciliation checks surface pipeline health
- the training-dataset export is derived from the latest offline snapshot plus event labels

## Serving and observability layer

- FastAPI exposes feature lookups, quality summaries, training-dataset summaries, and metrics
- `/metrics` publishes request, ingestion, feature-snapshot, and training-dataset counters/gauges for scraping

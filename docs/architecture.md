# Architecture

## Problem statement

ML systems often need the same feature values in two places:

- online serving for low-latency inference
- offline storage for training and evaluation

If those two paths diverge, teams get training-serving skew, broken models, and low trust in the platform.

This project is a small but production-style system that shows how to reduce that problem.

## High-level components

### 1. Event producer

Generates or ingests raw events such as:

- page views
- clicks
- add-to-cart events
- purchases

Each event has:

- event id
- entity id
- event type
- timestamp
- metadata payload
- schema version

### 2. Message bus

Use Redpanda locally because:

- Kafka-compatible
- easier local setup
- realistic enough for portfolio work

### 3. Streaming pipeline

Consumes events and computes fast-updating aggregates such as:

- rolling counts
- recency metrics
- rolling sums
- category aggregates

Outputs:

- online feature updates
- offline append-only feature records

### 4. Online feature store

Redis stores the latest feature values for low-latency serving.

Use cases:

- inference-time feature lookup
- API retrieval
- debugging latest values

### 5. Offline feature store

Use Parquet + DuckDB for local analytics and backfill validation.

Later, Postgres can be used for metadata and job tracking.

### 6. Quality and reconciliation layer

This is one of the most important parts of the project.

Checks should include:

- null rates
- duplicate event ids
- missing required fields
- freshness lag
- online vs offline feature diffs
- schema compatibility violations

### 7. Serving API

FastAPI service exposing:

- current features by entity
- freshness metadata
- reconciliation status

## Shipped platform shape

The current repo packages:

- synthetic events for deterministic local and hosted-demo runs
- Redpanda ingestion for the full local stack
- Python consumers and materialization jobs
- Redis online feature reads
- DuckDB offline feature snapshots
- FastAPI serving plus a Prometheus-style `/metrics` surface
- container, Kubernetes, AWS ECS, Azure Container Apps, and Jenkins deployment assets for the hosted-demo API

## Design choices

### Why Redpanda over full Kafka first

The goal is to learn streaming concepts fast without spending too much time on setup friction.

### Why Redis for online serving

It is simple, widely understood, and immediately recognizable to hiring teams.

### Why DuckDB for offline storage

It gives a fast local analytics path and makes the project easy to run on a laptop.

### Why reconciliation matters

Many candidates build pipelines that move data.

Far fewer build systems make the data-quality checks explicit.

That is one of the strongest signals this project should send.

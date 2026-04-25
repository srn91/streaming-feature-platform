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

## Versioned build path

### V1

- synthetic events
- Redpanda ingestion
- Python consumers
- Redis online store
- DuckDB offline store
- FastAPI serving

### V2

- schema registry style validation
- drift and freshness dashboards
- batch backfill job
- more realistic feature definitions

### V3

- Spark Structured Streaming version
- CI tests
- container health checks
- observability stack

## Design choices

### Why Redpanda over full Kafka first

The goal is to learn streaming concepts fast without spending too much time on setup friction.

### Why Redis for online serving

It is simple, widely understood, and immediately recognizable to hiring teams.

### Why DuckDB for offline storage

It gives a fast local analytics path and makes the project easy to run on a laptop.

### Why reconciliation matters

Many candidates build pipelines that move data.

Far fewer build systems that prove the data is correct.

That is one of the strongest signals this project should send.

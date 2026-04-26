# System Concepts

This note explains the concepts the repo exercises so the implementation choices are easier to follow.

## Core concepts

### Data Engineer skills

- what event streams are
- how Kafka or Redpanda topics work
- what partitions and consumers do
- batch vs streaming pipelines
- data modeling for event data
- schema evolution
- data quality and reconciliation

### ML Engineer skills

- what a feature store is
- online vs offline features
- training-serving skew
- feature freshness
- why low-latency serving matters

## Concept flow

### Step 1: event streaming basics

Learn:

- producer
- topic
- partition
- consumer group
- offset

Outcome:

You can trace how an event moves from a producer into a consumer.

### Step 2: feature platform basics

Learn:

- entity
- feature
- aggregation window
- online feature store
- offline feature store

Outcome:

You can explain why the same feature needs online and offline paths.

### Step 3: data quality

Learn:

- null checks
- uniqueness checks
- freshness checks
- reconciliation
- schema validation

Outcome:

You can explain how the repo checks that the pipeline output is correct.

### Step 4: serving and reliability

Learn:

- low-latency reads
- API serving
- cache-like access patterns
- monitoring

Outcome:

You can explain how downstream systems consume feature values.

## Practical summary

This repository demonstrates a feature platform that ingests raw user events, computes rolling features, stores them in Redis for online serving and DuckDB for offline use, and runs reconciliation checks to reduce training-serving skew.

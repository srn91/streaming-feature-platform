# Learning Roadmap

This document is for you to learn the project like a beginner while building it.

## What you need to understand by the end

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

## Learning order

### Step 1: event streaming basics

Learn:

- producer
- topic
- partition
- consumer group
- offset

Goal:

You should be able to explain how an event gets from a producer into a consumer.

### Step 2: feature platform basics

Learn:

- entity
- feature
- aggregation window
- online feature store
- offline feature store

Goal:

You should be able to explain why the same feature needs two paths.

### Step 3: data quality

Learn:

- null checks
- uniqueness checks
- freshness checks
- reconciliation
- schema validation

Goal:

You should be able to explain how you know the pipeline is correct.

### Step 4: serving and reliability

Learn:

- low-latency reads
- API serving
- cache-like access patterns
- monitoring

Goal:

You should be able to explain how downstream systems consume features.

## How we will work

For each milestone, do three things:

1. build the component
2. explain it in plain English
3. write down interview-style talking points

## Minimum interview explanation you should be able to give

`I built a streaming feature platform that ingests raw user events, computes rolling features, stores them in Redis for online serving and DuckDB/Parquet for offline use, and runs reconciliation checks to reduce training-serving skew.`

If you can explain that cleanly, you are already much stronger for DE and ML Engineer interviews.

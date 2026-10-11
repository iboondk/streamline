# Streamline: effectively-once analytics on Kafka

## Problem

Shop order and inventory events must reach analytics correctly. Kafka delivers at least once, so a consumer that crashes before committing sees records again, and each redelivery risks a double-counted order or stock change. The question is whether an Avro-contracted Kafka pipeline with two consumer languages and ClickHouse can give correct counts under at-least-once delivery.

## Architecture

A seeded producer sends Avro events, encoded through Schema Registry, to separate orders and inventory topics. A Go consumer (franz-go) reads orders and a Node consumer (kafkajs) reads inventory. Each batches per partition and inserts into a ClickHouse `ReplacingMergeTree` table. Materialized views roll orders up per minute and stock up per SKU, and a provisioned Grafana dashboard reads them. The README has the full diagram.

## Key decisions

- **Avro with BACKWARD compatibility** (ADR-0001). The registry refuses breaking schema changes.
- **Three idempotent layers** (ADR-0002). Offsets are committed only after a successful insert. Each insert carries a token built from topic, partition and offset range, so ClickHouse drops an identical retry. Rows are keyed by a producer-assigned UUID, so duplicates collapse on merge.
- **Dead-letter topics with commit-past** (ADR-0004). A poison record is parked with its source offset, and the partition keeps moving.
- **A stated limit** (ADR-0005). Revenue in the materialized view is a plain sum and can overcount after a replay. The exact revenue panel reads the raw table with `FINAL` instead.

## Results

Verified so far:

- `make check` passes: JSON, YAML, Avro fields, SQL table names and shell syntax.
- Unit tests pass: 18 Go test functions, 20 Node consumer tests, 11 producer tests and 15 measure tests.
- CI is configured to run these checks, but it has not yet run on a hosted runner.

The authoring machine has no Docker, so the live stack, the replay proof and the failure behaviour have not run. This write-up reports no throughput, lag or revenue numbers.

## What I would do next

1. Run the full stack in CI with service containers, so the replay proof runs on every change.
2. Make the Node consumer separate registry outages from bad records, as the Go consumer does.
3. Key revenue by `event_id`, or rebuild the revenue view, so replays cannot inflate it.
4. Add a DLQ replay tool with an audit trail.
5. Run `make measure` under load, save the output in `results/`, and only then tune batch sizes and partitions.

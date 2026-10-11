# ADR-0002: Effectively-once results from three idempotent layers

Status: Accepted

## Context

Kafka delivers at least once. A consumer can crash between the ClickHouse insert and the offset commit, a ClickHouse insert can time out after it has landed, and a rebalance can move a partition mid-batch. Every one of these redelivers records. The analytics must still count each order once. Kafka transactions would not cover the ClickHouse side, and multi-broker or replicated ClickHouse is out of scope.

## Decision

Duplicates are removed at three layers:

1. **Commit after write.** Both consumers disable auto-commit and commit offsets only after the insert for that batch succeeds.
2. **Block-level dedupe.** Each insert covers one partition's contiguous offset range and carries `insert_deduplication_token = "<topic>-<partition>-<firstOffset>-<lastOffset>"`. The raw tables set `non_replicated_deduplication_window = 10000`, and inserts set `deduplicate_blocks_in_dependent_materialized_views = 1`.
3. **Row-level dedupe.** The raw tables are `ReplacingMergeTree(ingested_at)` ordered by the producer-assigned `event_id` UUID. Exact queries use `FINAL` or `uniqExact(event_id)`.

## Consequences

- Layer 2 only protects a retry whose batch boundaries match and whose token is still in the 10000-block window. Layer 3 covers the rest, but only after a merge, so plain `count()` can be temporarily high.
- The order-count MV uses `uniqExactState(event_id)`, which stays correct on replay. Revenue in the MV is a plain sum, which does not. That limit is recorded in ADR-0005 and in `docs/FAILURE_HANDLING.md`.
- `make replay` checks that `uniqExact(event_id)` and the `FINAL` count are unchanged after a full replay. This check needs a live stack and has not been run in this environment.

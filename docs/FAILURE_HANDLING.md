# Failure handling

This document lists the failure modes the orders and inventory pipeline is designed for, how each one is detected, what the consumers do, and how to recover. It also explains the three dedupe layers and the one known over-count limit.

Scope: the local compose stack (one Kafka broker in KRaft mode, one Schema Registry, one ClickHouse node). Replication, multi-broker setups and Kafka transactions are out of scope.

## Dedupe layers

Kafka delivers at least once. Results are effectively once because duplicates are removed at three layers.

1. **Commit after write.** Both consumers turn off auto-commit. An offset is committed only after the ClickHouse insert for its batch succeeds. A crash between insert and commit redelivers the batch. It does not lose it.
   - consumer-go: `ProcessWithMax` returns an error on any insert failure. The loop rewinds the fetch position to the first record of the poll and does not commit. `kgo.DisableAutoCommit()` is set.
   - consumer-node: `eachBatch` throws when the insert fails. The offsets are resolved and committed only after the insert returns.

2. **Block-level dedupe.** Each insert is one partition's contiguous offset range and carries `insert_deduplication_token = "<topic>-<partition>-<firstOffset>-<lastOffset>"`. The raw tables set `non_replicated_deduplication_window = 10000`. Inserts set `deduplicate_blocks_in_dependent_materialized_views = 1`, so a block dropped from the raw table does not reach the MVs either. A retry of the same offset range is dropped by ClickHouse.
   - This layer only works while the token is still in the window. ClickHouse keeps the hashes of the last 10000 blocks per table. Past that, a repeat block is inserted again, and layers 1 and 3 are the only protection.
   - The token depends on batch boundaries. A redelivered poll can form different batches (different first or last offset), so its token differs and layer 2 does not match. Layer 3 handles those.

3. **Row-level dedupe.** The raw tables are `ReplacingMergeTree(ingested_at)` ordered by `event_id`, which is a producer-assigned UUID. Duplicate rows collapse when parts merge. Any query that needs exact results uses `FINAL`, or `uniqExact(event_id)` for counts. Merges are asynchronous, so plain `count()` can still include duplicates until a merge runs. `OPTIMIZE TABLE ... FINAL` forces the merge and is used by the replay script.

## Failure modes

| Failure mode | Detection | Behaviour | Recovery |
|---|---|---|---|
| **Consumer crash before commit.** The process dies after the insert and before the commit, or during a batch. | Consumer group lag does not drop to the expected value. The restarted consumer logs its first fetch from the last committed offset. In the raw table, `count()` can be higher than `uniqExact(event_id)` until a merge runs. | The uncommitted offsets are not advanced. On restart the group resumes from the last committed offset and redelivers the records. Layer 2 drops the redelivered block if the batch boundaries match. Otherwise layer 3 collapses the duplicate rows on merge. | Automatic on restart: `docker compose up -d consumer-go`. No manual step. Check that `FINAL` count equals `uniqExact(event_id)` after `OPTIMIZE TABLE ... FINAL`. |
| **ClickHouse down, or an insert timeout.** The server is unreachable, or the insert returns an error or times out. The insert may still have landed on the server. | consumer-go logs `process failed, not committing` with the error and the backoff. consumer-node's kafkajs logs the failed batch and retries it. Lag grows while ClickHouse is down. | No commit happens. consumer-go rewinds to the first record of the poll and retries with exponential backoff from 200 ms up to 10 s. consumer-node retries the batch through kafkajs. If the earlier insert did land, the retry carries the same token (if the boundaries match) and is dropped by layer 2, or collapsed by layer 3. | Automatic once ClickHouse is reachable again. Lag returns to zero. No data is lost. Duplicate rows, if any, merge away. |
| **Broker restart.** The Kafka container restarts, or the broker becomes unreachable for a while. | Client errors in consumer and producer logs. The consumer group shows no progress. | franz-go and kafkajs reconnect and retry. Producer sends are idempotent with `maxInFlightRequests: 1` and `acks: -1`, so a retried send does not duplicate records on the broker. Committed offsets are kept by the broker. | `docker compose restart kafka` keeps the log data, since it stays in the container filesystem. `docker compose down` removes the container and its data, so it counts as a full reset. Kafka has no named volume in the compose file. Recreate topics and schemas with `make bootstrap` after a `down`. |
| **Poison message: bad framing or bad magic byte.** The value does not start with the Confluent wire header (magic byte 0 and a 4-byte schema id). | consumer-go: `wire.Parse` returns an error. consumer-node: `parseWire` throws `bad magic`. | Sent to the topic's DLQ (`shop.orders.dlq` or `shop.inventory.dlq`) with the original key and value, plus headers `x-error`, `x-source-partition` and `x-source-offset`. The source offset is then committed, so the partition is not blocked. | Inspect the DLQ records. The source record is not replayed automatically. Fix the producer and re-send, or leave the record in the DLQ. |
| **Poison message: unknown schema id.** The schema id is not in the registry (HTTP 404). | consumer-go: `registry.ErrNotFound`, sent to the DLQ. consumer-node: the registry decode call fails, sent to the DLQ. | Same as above: DLQ with headers, then commit. | Check which producer wrote the id, and whether the registry was reset. Re-register the schema if it was lost. Re-send the events if needed. |
| **Poison message: Avro payload does not match the schema.** | consumer-go: `avro decode (schema N)` error. consumer-node: decode or row-mapping error. | Sent to the DLQ, then commit. | Same as above. |
| **Registry unreachable (not 404).** | consumer-go: `RetryableError`. The poll is not committed and is retried with backoff. consumer-node: the decode call throws, and the record goes to the DLQ. | consumer-go retries the whole poll. consumer-node currently treats any decode error as poison and sends the message to the DLQ. | consumer-go: automatic once the registry is back. consumer-node: records routed to the DLQ during the outage must be re-sent from the DLQ or from the source topic by replay. This is a known gap, see below. |
| **Schema evolution: BACKWARD compatibility, adding an optional field.** A new schema version adds a field with a default. | The registry rejects an incompatible registration with HTTP 409. `register-schemas.sh` sets `BACKWARD` per subject. | Consumers decode each record with its writer schema, which the registry returns by id. The Go model and the Node row mapper read only the fields they know. Extra fields are ignored. Old data read with the new schema fills the default. | Register the new version with `curl` against the registry. Restart consumers only if you want them to use new fields. A new ClickHouse column is a separate `ALTER TABLE` that the pipeline does not run automatically. |
| **Consumer rebalance mid-batch.** A consumer joins or leaves the group while a batch is in flight. | Rebalance events in consumer logs. Commit errors after reassignment. | consumer-go sets `kgo.BlockRebalanceOnPoll()`, so the rebalance waits until the in-flight poll is processed and committed (`cl.AllowRebalance()`). consumer-node (kafkajs) does not hold the rebalance. If the commit fails after the partition moves, the new owner redelivers the records. | Automatic. The new owner resumes from the last committed offset. The dedupe layers handle the overlap. |
| **Full replay.** The group is reset to the earliest offset and the whole topic is reprocessed. | `make replay` (scripts/replay-demo.sh) prints counts before and after. | With the same consumer settings, the replayed batches keep the same offset ranges, so layer 2 drops them while the window still covers them. After the window, layer 3 collapses the duplicates on merge. The MVs are only protected by layer 2 (see the next section). | Run `make replay`, which resets `streamline-orders` to earliest on `shop.orders.v1` and checks that `uniqExact(event_id)` and the `FINAL` count stay the same. The script exits 1 if they differ. |
| **Duplicate producer sends.** The producer resends a message after a timeout, or the application sends the same event twice. | Compare `count()` with `uniqExact(event_id)` in `orders_raw`. | The idempotent producer removes broker-level duplicates from retries within one session. Application-level resends keep the same `event_id`, so they collapse in layer 3. Their Kafka offsets are different, so layer 2 does not catch them. | Automatic at read time with `FINAL` or `uniqExact`. Merges remove the physical duplicates. |

## Known limit: sum-based MVs can over-count revenue

`orders_per_minute` is an `AggregatingMergeTree` fed by `orders_per_minute_mv`.

- `orders` is `uniqExactState(event_id)`. Merging these states is exact, so the order count per minute is replay-safe.
- `revenue_cents` is a plain `sumIf(total_cents, event_type = 'ORDER_PAID')`. The MV sums every row it receives. It has no notion of `event_id`. If a full replay inserts a block that the raw table still accepts (for example after the dedupe window has passed, or with different batch boundaries), the MV adds that revenue again. The raw table collapses the rows on merge, but the MV does not.

So revenue in `orders_per_minute` can be higher than the true value after a replay. The dashboard's "exact revenue" panel reads `orders_raw FINAL` instead.

Reconciliation query. It compares the MV total with the raw FINAL total and prints the difference. `scripts/replay-demo.sh` runs it and prints the result without failing the run:

```sql
SELECT mv, raw, mv - raw AS difference_cents FROM (
  SELECT
    (SELECT sum(revenue_cents) FROM streamline.orders_per_minute) AS mv,
    (SELECT sumIf(total_cents, event_type = 'ORDER_PAID') FROM streamline.orders_raw FINAL) AS raw
)
```

A difference of zero means the MV and the raw table agree. A non-zero difference after a replay is the known limit. It is not a pipeline failure. The fix would be to move revenue to a table keyed by `event_id` or to rebuild the MV. Neither is part of this scope.

## Known gaps

- consumer-node sends every decode error to the DLQ, including a Schema Registry outage. consumer-go retries registry outages instead. The Node consumer needs the same split between "bad record" and "retryable error" to match the Go consumer.
- Dedupe layer 2 only covers the last 10000 blocks per table. Replays older than that depend on layer 3 and the MV limit above.
- The Go consumer's Dockerfile builds with `golang:1.22-alpine` while `consumer-go/go.mod` declares `go 1.24`. The official image sets `GOTOOLCHAIN=local`, which would refuse that go.mod. The Dockerfile now sets `ENV GOTOOLCHAIN=auto` so the build stage fetches the Go 1.24 toolchain. The image build has not been run here because Docker is unavailable.

## Verified here

The authoring machine has Go 1.26.5, Node 24.15.0, npm 11.12.1, Python 3.10.5 and GNU Make 3.81. Docker is not installed.

Ran locally and passed:

- `go test ./...` in `consumer-go`: 18 tests across 6 packages with tests (`app`, `batch`, `dlq`, `model`, `registry`, `wire`), all passing. These are unit tests and need no Docker.
- `npm test` in `tools/producer` (11 tests), `tools/measure` (15 tests) and `consumer-node` (20 tests), all passing.
- `make test`, which runs the Go and Node unit tests above, exits 0.
- `make check` (`python3 scripts/check-static.py`): JSON, YAML, Avro schema fields, SQL table names and `bash -n` on every script.
- `bash -n scripts/replay-demo.sh`.
- `make -n` for `up`, `bootstrap`, `test`, `check` and `replay` prints the commands without errors.

Not run, because Docker is unavailable here:

- `docker compose up`, the health checks, and both consumer image builds.
- `make bootstrap` (topic creation and schema registration against a live broker and registry).
- `make produce`, `make measure`, and the replay proof in `make replay`.
- Every failure mode above. The behaviours are described from the code and have not been exercised against a broker, a registry or ClickHouse.
- The ClickHouse DDL in `sql/` and the materialized views, which were checked for table names only.
- The reconciliation query and its output.

No throughput, lag, latency or revenue figures are given in this document. Those numbers come only from a live run of `make measure` and are written to `results/`.

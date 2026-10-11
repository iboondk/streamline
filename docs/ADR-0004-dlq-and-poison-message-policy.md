# ADR-0004: Poison messages go to a DLQ and are committed past

Status: Accepted

## Context

A single bad record must not block a partition. Some failures are permanent for a record: a bad magic byte, an unknown schema id, or a payload that does not match its schema. Other failures are temporary: the registry or ClickHouse is unreachable. Treating both the same way either blocks the partition forever or loses data.

## Decision

- Permanent per-record failures are written to the topic's DLQ (`shop.orders.dlq`, `shop.inventory.dlq`) with the original key and value and the headers `x-error`, `x-source-partition` and `x-source-offset`. The source offset is then committed.
- Temporary failures are not committed. The consumer rewinds and retries with backoff. consumer-go retries the whole poll with exponential backoff from 200 ms to 10 s.
- Replay of DLQ records is manual. The pipeline does not re-inject them.

## Consequences

- A partition keeps moving when one record is bad, and the bad record is kept for inspection.
- The split depends on the consumer correctly classifying errors. The Node consumer does not yet separate registry outages from bad records and sends both to the DLQ. This is the open item in `docs/FAILURE_HANDLING.md`.
- DLQ topics have one partition each, so inspecting them has no ordering guarantee across source partitions. The source partition and offset headers preserve the origin.

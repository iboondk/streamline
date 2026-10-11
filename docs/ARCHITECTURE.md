# Streamline architecture

This document describes the pipeline design: components, delivery semantics, pinned technology choices and the data contracts.

## 1. Architecture

```
 tools/producer (Node)                     Schema Registry (Avro, BACKWARD compat)
   seeded generator ──Avro──► Kafka (KRaft, single broker)
                               ├─ shop.orders.v1     (6 partitions, key=order_id)
                               ├─ shop.inventory.v1  (6 partitions, key=sku)
                               ├─ shop.orders.dlq
                               └─ shop.inventory.dlq
                                   │                         │
                    consumer-go (franz-go)        consumer-node (kafkajs)
                    group: streamline-orders      group: streamline-inventory
                    topic: shop.orders.v1         topic: shop.inventory.v1
                                   │                         │
                    per-partition batch → INSERT with insert_deduplication_token
                                   ▼                         ▼
                ClickHouse  streamline.orders_raw   streamline.inventory_raw
                            (ReplacingMergeTree(ingested_at) ORDER BY event_id)
                                   │ MV                       │ MV
                            orders_per_minute_mv       inventory_by_sku_mv
                            (AggregatingMergeTree)     (SummingMergeTree)
                                   ▼
                              Grafana (ClickHouse datasource, provisioned dashboard)

 tools/measure (Node): samples consumer-group lag (Kafka admin) and ClickHouse row growth → results/*.json
 scripts/replay-demo.sh: resets a consumer group to earliest and shows deduplicated counts are unchanged
```

### Delivery semantics ("exactly-once-ish")

Kafka gives at-least-once delivery here. Effectively-once results come from idempotent writes at three layers:

1. **Commit after write.** Consumers disable auto-commit. Offsets are committed only after the ClickHouse insert for that batch succeeds. A crash between insert and commit causes redelivery, not loss.
2. **Block-level dedupe.** Each insert is one partition's contiguous offset range. It carries `insert_deduplication_token = "<topic>-<partition>-<firstOffset>-<lastOffset>"`. Tables set `non_replicated_deduplication_window = 10000`. Inserts set `deduplicate_blocks_in_dependent_materialized_views = 1`. A retried identical batch is therefore dropped by ClickHouse, both in the raw table and in its MVs.
3. **Row-level dedupe.** Raw tables are `ReplacingMergeTree` keyed by `event_id` (a producer-assigned UUID). Any duplicate that gets past layer 2 (for example a full replay with different batch boundaries) collapses on merge. Queries that need exact answers use `FINAL`.

Known limit, which `docs/FAILURE_HANDLING.md` must state: SummingMergeTree/sum-based MVs can double-count on a full replay whose batch boundaries differ from the original ones. Order *counts* in the MV use `uniqExactState(event_id)`, so they are replay-safe. *Revenue* in the MV is a plain sum, so it can overcount. The dashboard's "exact revenue" panel reads `orders_raw FINAL`, and the doc includes a reconciliation query that compares the MV with the raw table.

Poison messages (bad magic byte, unknown schema id, decode failure) go to the topic's `.dlq` with headers `x-error`, `x-source-partition` and `x-source-offset`. Their offset is then committed so the partition is not blocked.

## 2. Tech choices (pinned)

| Concern | Choice | Why |
|---|---|---|
| Broker | `confluentinc/cp-kafka:7.7.1` in KRaft mode | Real Kafka, no ZooKeeper, ships admin CLIs |
| Registry | `confluentinc/cp-schema-registry:7.7.1` | Standard Confluent wire format (magic 0 + 4-byte schema id) |
| Analytics DB | `clickhouse/clickhouse-server:24.8` | LTS. ReplacingMergeTree, dedupe tokens, MVs |
| Dashboard | `grafana/grafana:11.2.0` + plugin `grafana-clickhouse-datasource` 4.4.0 | Provisioned from files |
| Producer / measure / Node consumer | Node 20+ ESM JavaScript, `kafkajs@2.2.4`, `@kafkajs/confluent-schema-registry@3.3.0`, `@clickhouse/client@1.6.0`, tests via `node:test` | Plain JS keeps the services small. No build step. |
| Go consumer | Go 1.24+ module, `github.com/twmb/franz-go` (kgo), `github.com/hamba/avro/v2`, `github.com/ClickHouse/clickhouse-go/v2` | Pure Go, no cgo/librdkafka |
| Config | env vars, documented in `.env.example` with local-only dummy values | No real credentials |

## 3. Data contracts

`schemas/order_event.avsc` (namespace `com.streamline.shop`, record `OrderEvent`):
`event_id: string (uuid)`, `event_type: enum OrderEventType {ORDER_CREATED, ORDER_PAID, ORDER_CANCELLED}`, `order_id: string`, `customer_id: string`, `shop_id: string`, `currency: string`, `total_cents: long`, `line_items: array<record LineItem {sku: string, quantity: int, unit_price_cents: long}>`, `occurred_at: long (logicalType timestamp-millis)`.

`schemas/inventory_event.avsc` (record `InventoryEvent`):
`event_id: string (uuid)`, `event_type: enum InventoryEventType {STOCK_RESERVED, STOCK_RELEASED, STOCK_RESTOCKED}`, `sku: string`, `warehouse_id: string`, `delta: int` (negative for RESERVED), `occurred_at: long (timestamp-millis)`.

Registry subjects use TopicNameStrategy: `shop.orders.v1-value`, `shop.inventory.v1-value`.

ClickHouse database `streamline`:
- `orders_raw(event_id String, event_type LowCardinality(String), order_id String, customer_id String, shop_id LowCardinality(String), currency LowCardinality(String), total_cents Int64, item_count UInt32, occurred_at DateTime64(3), kafka_partition UInt16, kafka_offset UInt64, ingested_at DateTime64(3) DEFAULT now64(3))`, with `ReplacingMergeTree(ingested_at) PARTITION BY toYYYYMM(occurred_at) ORDER BY event_id SETTINGS non_replicated_deduplication_window = 10000`
- `inventory_raw(event_id, event_type, sku, warehouse_id, delta Int32, occurred_at, kafka_partition, kafka_offset, ingested_at)`, with the same engine pattern
- `orders_per_minute` AggregatingMergeTree `ORDER BY (minute, shop_id, event_type)`: `orders AggregateFunction(uniqExact, String)`, `revenue_cents SimpleAggregateFunction(sum, Int64)`, fed by `orders_per_minute_mv`
- `inventory_by_sku` SummingMergeTree `ORDER BY (sku, warehouse_id)`: `net_delta Int64`, fed by `inventory_by_sku_mv`


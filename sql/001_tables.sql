CREATE DATABASE IF NOT EXISTS streamline;

CREATE TABLE IF NOT EXISTS streamline.orders_raw
(
    event_id       String,
    event_type     LowCardinality(String),
    order_id       String,
    customer_id    String,
    shop_id        LowCardinality(String),
    currency       LowCardinality(String),
    total_cents    Int64,
    item_count     UInt32,
    occurred_at    DateTime64(3),
    kafka_partition UInt16,
    kafka_offset   UInt64,
    ingested_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY event_id
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS streamline.inventory_raw
(
    event_id        String,
    event_type      LowCardinality(String),
    sku             String,
    warehouse_id    LowCardinality(String),
    delta           Int32,
    occurred_at     DateTime64(3),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY event_id
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS streamline.orders_per_minute
(
    minute        DateTime,
    shop_id       LowCardinality(String),
    event_type    LowCardinality(String),
    orders        AggregateFunction(uniqExact, String),
    revenue_cents SimpleAggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree
ORDER BY (minute, shop_id, event_type);

CREATE TABLE IF NOT EXISTS streamline.inventory_by_sku
(
    sku          String,
    warehouse_id LowCardinality(String),
    net_delta    Int64
)
ENGINE = SummingMergeTree
ORDER BY (sku, warehouse_id);

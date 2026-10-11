CREATE MATERIALIZED VIEW IF NOT EXISTS streamline.orders_per_minute_mv
TO streamline.orders_per_minute
AS
SELECT
    toStartOfMinute(occurred_at) AS minute,
    shop_id,
    event_type,
    uniqExactState(event_id) AS orders,
    sumIf(total_cents, event_type = 'ORDER_PAID') AS revenue_cents
FROM streamline.orders_raw
GROUP BY minute, shop_id, event_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS streamline.inventory_by_sku_mv
TO streamline.inventory_by_sku
AS
SELECT
    sku,
    warehouse_id,
    sum(delta) AS net_delta
FROM streamline.inventory_raw
GROUP BY sku, warehouse_id;

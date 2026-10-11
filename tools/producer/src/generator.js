import { mulberry32 } from './rng.js';

// Weights must sum to 1. Order is the iteration order used for the pick.
const ORDER_EVENT_WEIGHTS = [
  ['ORDER_CREATED', 0.6],
  ['ORDER_PAID', 0.3],
  ['ORDER_CANCELLED', 0.1],
];
const INVENTORY_EVENT_WEIGHTS = [
  ['STOCK_RESERVED', 0.5],
  ['STOCK_RELEASED', 0.2],
  ['STOCK_RESTOCKED', 0.3],
];

// PAID and CANCELLED events refer to orders that were created earlier.
const RECENT_ORDER_LIMIT = 500;
const CUSTOMER_POOL = 5000;
const MIN_UNIT_PRICE_CENTS = 100;
const MAX_UNIT_PRICE_CENTS = 20000;
const MAX_LINE_ITEMS = 4;
const MAX_QUANTITY = 5;

function pickWeighted(weights, r) {
  let acc = 0;
  for (const [name, weight] of weights) {
    acc += weight;
    if (r < acc) return name;
  }
  return weights[weights.length - 1][0];
}

function pad(n, width) {
  return String(n).padStart(width, '0');
}

// UUID v4 layout built from the seeded rng, so it is reproducible.
function uuidV4(rng) {
  const bytes = new Array(16);
  for (let i = 0; i < 16; i++) bytes[i] = Math.floor(rng() * 256);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.map((b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Deterministic generator of order and inventory events.
 * The same options always produce the same sequence of events.
 * Field names match schemas/order_event.avsc and schemas/inventory_event.avsc.
 *
 * @param {{seed?: number, shops?: number, skus?: number, warehouses?: number}} [options]
 * @returns {{nextOrder: (nowMs: number) => object, nextInventory: (nowMs: number) => object}}
 */
export function createGenerator({ seed = 42, shops = 3, skus = 50, warehouses = 2 } = {}) {
  const rng = mulberry32(seed);
  const randInt = (lo, hi) => lo + Math.floor(rng() * (hi - lo + 1)); // inclusive bounds
  let orderSeq = 0;
  const recentOrders = [];

  function newOrderRecord() {
    orderSeq += 1;
    const lineCount = randInt(1, MAX_LINE_ITEMS);
    const lineItems = [];
    for (let i = 0; i < lineCount; i++) {
      lineItems.push({
        sku: `sku_${pad(randInt(1, skus), 4)}`,
        quantity: randInt(1, MAX_QUANTITY),
        unit_price_cents: randInt(MIN_UNIT_PRICE_CENTS, MAX_UNIT_PRICE_CENTS),
      });
    }
    const order = {
      order_id: `ord_${pad(orderSeq, 6)}`,
      customer_id: `cus_${pad(randInt(1, CUSTOMER_POOL), 5)}`,
      shop_id: `shop_${randInt(1, shops)}`,
      line_items: lineItems,
    };
    recentOrders.push(order);
    if (recentOrders.length > RECENT_ORDER_LIMIT) recentOrders.shift();
    return order;
  }

  function nextOrder(nowMs) {
    const eventId = uuidV4(rng);
    const eventType = pickWeighted(ORDER_EVENT_WEIGHTS, rng());
    let order;
    let resolvedType = eventType;
    if (eventType === 'ORDER_CREATED' || recentOrders.length === 0) {
      order = newOrderRecord();
      resolvedType = 'ORDER_CREATED';
    } else {
      order = recentOrders[randInt(0, recentOrders.length - 1)];
    }
    const lineItems = order.line_items.map((li) => ({ ...li }));
    const totalCents = lineItems.reduce((sum, li) => sum + li.quantity * li.unit_price_cents, 0);
    return {
      event_id: eventId,
      event_type: resolvedType,
      order_id: order.order_id,
      customer_id: order.customer_id,
      shop_id: order.shop_id,
      currency: 'USD',
      total_cents: totalCents,
      line_items: lineItems,
      occurred_at: nowMs,
    };
  }

  function nextInventory(nowMs) {
    const eventId = uuidV4(rng);
    const eventType = pickWeighted(INVENTORY_EVENT_WEIGHTS, rng());
    const sku = `sku_${pad(randInt(1, skus), 4)}`;
    const warehouseId = `wh_${randInt(1, warehouses)}`;
    let delta;
    if (eventType === 'STOCK_RESERVED') {
      delta = -randInt(1, 5);
    } else if (eventType === 'STOCK_RELEASED') {
      delta = randInt(1, 5);
    } else {
      delta = randInt(1, 100);
    }
    return {
      event_id: eventId,
      event_type: eventType,
      sku,
      warehouse_id: warehouseId,
      delta,
      occurred_at: nowMs,
    };
  }

  return { nextOrder, nextInventory };
}

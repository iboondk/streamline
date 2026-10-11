import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createGenerator } from '../src/generator.js';
import { mulberry32 } from '../src/rng.js';

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const NOW = 1767225600000;

function sequence(seed, count) {
  const gen = createGenerator({ seed });
  const out = [];
  for (let i = 0; i < count; i++) {
    out.push(i % 2 === 0 ? gen.nextOrder(NOW + i) : gen.nextInventory(NOW + i));
  }
  return out;
}

test('mulberry32 returns floats in [0, 1) and is repeatable per seed', () => {
  const a = mulberry32(7);
  const b = mulberry32(7);
  for (let i = 0; i < 1000; i++) {
    const x = a();
    assert.ok(x >= 0 && x < 1, `out of range: ${x}`);
    assert.equal(x, b());
  }
});

test('same seed gives an identical sequence', () => {
  assert.deepEqual(sequence(42, 500), sequence(42, 500));
});

test('different seeds differ', () => {
  assert.notDeepEqual(sequence(42, 200), sequence(43, 200));
});

test('every order total_cents equals the line_items sum', () => {
  const gen = createGenerator({ seed: 9 });
  for (let i = 0; i < 2000; i++) {
    const order = gen.nextOrder(NOW);
    assert.ok(order.line_items.length >= 1 && order.line_items.length <= 4);
    const sum = order.line_items.reduce((s, li) => s + li.quantity * li.unit_price_cents, 0);
    assert.equal(order.total_cents, sum);
    assert.equal(order.currency, 'USD');
  }
});

test('reserved inventory delta is negative and within 1..5 in magnitude', () => {
  const gen = createGenerator({ seed: 3 });
  let reserved = 0;
  for (let i = 0; i < 2000; i++) {
    const inv = gen.nextInventory(NOW);
    if (inv.event_type === 'STOCK_RESERVED') {
      reserved++;
      assert.ok(inv.delta < 0, `delta should be negative, got ${inv.delta}`);
      assert.ok(inv.delta >= -5);
    } else {
      assert.ok(inv.delta > 0, `delta should be positive for ${inv.event_type}`);
    }
  }
  assert.ok(reserved > 0);
});

test('all required fields are present with the expected types', () => {
  const [order] = sequence(1, 1);
  for (const field of [
    'event_id', 'event_type', 'order_id', 'customer_id', 'shop_id',
    'currency', 'total_cents', 'line_items', 'occurred_at',
  ]) {
    assert.ok(field in order, `order missing ${field}`);
  }
  assert.equal(typeof order.total_cents, 'number');
  assert.equal(order.occurred_at, NOW);
  assert.match(order.order_id, /^ord_\d{6}$/);
  assert.match(order.customer_id, /^cus_\d{5}$/);
  assert.match(order.shop_id, /^shop_\d+$/);
  for (const li of order.line_items) {
    assert.match(li.sku, /^sku_\d{4}$/);
    assert.equal(typeof li.quantity, 'number');
    assert.equal(typeof li.unit_price_cents, 'number');
  }

  const inv = sequence(1, 2)[1];
  for (const field of ['event_id', 'event_type', 'sku', 'warehouse_id', 'delta', 'occurred_at']) {
    assert.ok(field in inv, `inventory missing ${field}`);
  }
  assert.match(inv.warehouse_id, /^wh_\d+$/);
  assert.equal(inv.occurred_at, NOW + 1);
});

test('order event_type values come from the schema enum', () => {
  const allowed = new Set(['ORDER_CREATED', 'ORDER_PAID', 'ORDER_CANCELLED']);
  const gen = createGenerator({ seed: 5 });
  for (let i = 0; i < 500; i++) {
    assert.ok(allowed.has(gen.nextOrder(NOW).event_type));
  }
});

test('event_id matches the UUID v4 format', () => {
  for (const e of sequence(11, 200)) {
    assert.match(e.event_id, UUID_V4);
  }
});

test('10k events have unique event_ids', () => {
  const gen = createGenerator({ seed: 2026 });
  const ids = new Set();
  for (let i = 0; i < 10000; i++) {
    const e = i % 2 === 0 ? gen.nextOrder(NOW) : gen.nextInventory(NOW);
    ids.add(e.event_id);
  }
  assert.equal(ids.size, 10000);
});

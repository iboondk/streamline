import { test } from 'node:test';
import assert from 'node:assert/strict';
import { toInventoryRow, formatOccurredAt } from '../src/rows.js';

const EVENT = {
  event_id: '3f2b8c1e-0000-4000-8000-000000000001',
  event_type: 'STOCK_RESERVED',
  sku: 'SKU-100',
  warehouse_id: 'WH-1',
  delta: -3,
  occurred_at: 1767225600123, // 2026-01-01T00:00:00.123Z
};

test('formats occurred_at as UTC YYYY-MM-DD HH:MM:SS.mmm', () => {
  assert.equal(formatOccurredAt(1767225600123), '2026-01-01 00:00:00.123');
  assert.equal(formatOccurredAt(0), '1970-01-01 00:00:00.000');
});

test('maps a numeric timestamp-millis event to the inventory_raw row', () => {
  const row = toInventoryRow(EVENT, 3, 1234);
  assert.deepEqual(row, {
    event_id: EVENT.event_id,
    event_type: 'STOCK_RESERVED',
    sku: 'SKU-100',
    warehouse_id: 'WH-1',
    delta: -3,
    occurred_at: '2026-01-01 00:00:00.123',
    kafka_partition: 3,
    kafka_offset: '1234',
  });
});

test('accepts occurred_at as a Date (avsc default for timestamp-millis)', () => {
  const row = toInventoryRow({ ...EVENT, occurred_at: new Date(1767225600123) }, 0, '7');
  assert.equal(row.occurred_at, '2026-01-01 00:00:00.123');
  assert.equal(row.kafka_offset, '7');
  assert.equal(typeof row.kafka_partition, 'number');
});

test('kafka_offset is always a string, kafka_partition always a number', () => {
  const row = toInventoryRow(EVENT, '5', 99n);
  assert.equal(row.kafka_partition, 5);
  assert.equal(row.kafka_offset, '99');
});

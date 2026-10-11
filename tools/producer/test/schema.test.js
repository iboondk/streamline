import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import avsc from 'avsc';
import { createGenerator } from '../src/generator.js';

const { Type } = avsc;
const SCHEMA_DIR = new URL('../../../schemas/', import.meta.url);
const orderType = Type.forSchema(JSON.parse(readFileSync(new URL('order_event.avsc', SCHEMA_DIR), 'utf8')));
const inventoryType = Type.forSchema(JSON.parse(readFileSync(new URL('inventory_event.avsc', SCHEMA_DIR), 'utf8')));
const NOW = 1767225600000;

test('1000 generated orders pass the order_event schema', () => {
  const gen = createGenerator({ seed: 42 });
  for (let i = 0; i < 1000; i++) {
    const order = gen.nextOrder(NOW + i);
    assert.ok(orderType.isValid(order), `order ${i} failed validation: ${JSON.stringify(order)}`);
  }
});

test('1000 generated inventory events pass the inventory_event schema', () => {
  const gen = createGenerator({ seed: 42 });
  for (let i = 0; i < 1000; i++) {
    const inv = gen.nextInventory(NOW + i);
    assert.ok(inventoryType.isValid(inv), `inventory ${i} failed validation: ${JSON.stringify(inv)}`);
  }
});

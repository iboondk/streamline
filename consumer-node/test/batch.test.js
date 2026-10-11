import { test } from 'node:test';
import assert from 'node:assert/strict';
import { dedupToken } from '../src/batch.js';

test('dedupToken joins topic, partition, first and last offset', () => {
  assert.equal(
    dedupToken('shop.inventory.v1', 2, 100, 149),
    'shop.inventory.v1-2-100-149',
  );
});

test('dedupToken is stable for the same range and differs for other ranges', () => {
  const a = dedupToken('t', 0, '10', '19');
  assert.equal(a, dedupToken('t', 0, '10', '19'));
  assert.notEqual(a, dedupToken('t', 1, '10', '19'));
  assert.notEqual(a, dedupToken('t', 0, '10', '20'));
  assert.notEqual(a, dedupToken('u', 0, '10', '19'));
});

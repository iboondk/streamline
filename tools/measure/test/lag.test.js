import { test } from 'node:test';
import assert from 'node:assert/strict';
import { computeLag } from '../src/lag.js';

test('single partition: lag is high minus committed offset', () => {
  const result = computeLag(
    [{ partition: 0, high: '120' }],
    [{ topic: 'shop.orders.v1', partitions: [{ partition: 0, offset: '100' }] }],
  );
  assert.deepEqual(result, { perPartition: [{ partition: 0, lag: 20 }], total: 20 });
});

test('committed offset of -1 means nothing committed, so lag equals high', () => {
  const result = computeLag(
    [{ partition: 0, high: '57' }],
    [{ topic: 'shop.orders.v1', partitions: [{ partition: 0, offset: '-1' }] }],
  );
  assert.deepEqual(result, { perPartition: [{ partition: 0, lag: 57 }], total: 57 });
});

test('partition missing from the group offsets is treated as nothing committed', () => {
  const result = computeLag(
    [{ partition: 0, high: '10' }, { partition: 1, high: '8' }],
    [{ topic: 'shop.orders.v1', partitions: [{ partition: 0, offset: '4' }] }],
  );
  assert.deepEqual(result.perPartition, [
    { partition: 0, lag: 6 },
    { partition: 1, lag: 8 },
  ]);
  assert.equal(result.total, 14);
});

test('multi-partition: per-partition lags are sorted and summed', () => {
  const result = computeLag(
    [
      { partition: 2, high: '50' },
      { partition: 0, high: '30' },
      { partition: 1, high: '40' },
    ],
    [
      {
        topic: 'shop.orders.v1',
        partitions: [
          { partition: 0, offset: '30' },
          { partition: 1, offset: '-1' },
          { partition: 2, offset: '45' },
        ],
      },
    ],
  );
  assert.deepEqual(result.perPartition, [
    { partition: 0, lag: 0 },
    { partition: 1, lag: 40 },
    { partition: 2, lag: 5 },
  ]);
  assert.equal(result.total, 45);
});

test('caught-up group reports zero lag, never negative', () => {
  const result = computeLag(
    [{ partition: 0, high: '10' }],
    [{ topic: 't', partitions: [{ partition: 0, offset: '10' }] }],
  );
  assert.equal(result.total, 0);
  assert.equal(result.perPartition[0].lag, 0);
});

test('empty topic with no commits gives zero total', () => {
  assert.deepEqual(computeLag([], []), { perPartition: [], total: 0 });
});

test('duplicate partition across group offset entries is refused', () => {
  assert.throws(
    () => computeLag(
      [{ partition: 0, high: '5' }],
      [
        { topic: 'a', partitions: [{ partition: 0, offset: '1' }] },
        { topic: 'b', partitions: [{ partition: 0, offset: '2' }] },
      ],
    ),
    /more than one group offset entry/,
  );
});

test('non-numeric offsets are refused rather than guessed', () => {
  assert.throws(
    () => computeLag([{ partition: 0, high: 'abc' }], []),
    TypeError,
  );
});

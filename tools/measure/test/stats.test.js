import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize } from '../src/stats.js';

test('percentiles on a known array (nearest-rank, lags 1..100)', () => {
  const samples = Array.from({ length: 100 }, (_, i) => ({ t: 1000 * i, rows: i * 10, lag: i + 1 }));
  const s = summarize(samples);
  assert.equal(s.lagP50, 50);
  assert.equal(s.lagP95, 95);
  assert.equal(s.lagMax, 100);
  assert.equal(s.lagFinal, 100);
});

test('percentiles are order-independent', () => {
  const lags = [9, 1, 7, 3, 5];
  const samples = lags.map((lag, i) => ({ t: 1000 * i, rows: 0, lag }));
  const s = summarize(samples);
  assert.equal(s.lagP50, 5);
  assert.equal(s.lagP95, 9);
  assert.equal(s.lagMax, 9);
  assert.equal(s.lagFinal, 5); // last sample in time order, not last in array
});

test('rate and duration come from the first and last sample by time', () => {
  const samples = [
    { t: 0, rows: 100, lag: 0 },
    { t: 4000, rows: 500, lag: 3 },
    { t: 2000, rows: 300, lag: 1 },
  ];
  const s = summarize(samples);
  assert.equal(s.durationSec, 4);
  assert.equal(s.rowsDelta, 400);
  assert.equal(s.rowsPerSec, 100);
  assert.equal(s.lagFinal, 3);
});

test('throws on zero samples', () => {
  assert.throws(() => summarize([]), /at least 2 samples/);
});

test('throws on exactly one sample', () => {
  assert.throws(() => summarize([{ t: 0, rows: 1, lag: 1 }]), /at least 2 samples to summarize, got 1/);
});

test('throws when samples span no time', () => {
  assert.throws(
    () => summarize([{ t: 5, rows: 1, lag: 0 }, { t: 5, rows: 2, lag: 0 }]),
    /no time/,
  );
});

test('throws on a non-numeric field rather than summarizing it', () => {
  assert.throws(
    () => summarize([{ t: 0, rows: 1, lag: 0 }, { t: 1000, rows: 'x', lag: 0 }]),
    TypeError,
  );
});

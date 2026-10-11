import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseWire } from '../src/wire.js';

test('parses magic 0, big-endian schema id and payload', () => {
  const buf = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x2a, 0xaa, 0xbb]);
  const { schemaId, payload } = parseWire(buf);
  assert.equal(schemaId, 42);
  assert.deepEqual([...payload], [0xaa, 0xbb]);
});

test('reads a multi-byte big-endian schema id', () => {
  const buf = Buffer.from([0x00, 0x00, 0x00, 0x01, 0x00, 0x07]);
  assert.equal(parseWire(buf).schemaId, 256);
  assert.equal(parseWire(buf).payload.length, 1);
});

test('empty payload after the 5-byte header is allowed', () => {
  const { schemaId, payload } = parseWire(Buffer.from([0, 0, 0, 0, 9]));
  assert.equal(schemaId, 9);
  assert.equal(payload.length, 0);
});

test('throws bad magic when shorter than 5 bytes', () => {
  assert.throws(() => parseWire(Buffer.from([0, 0, 0, 1])), { message: 'bad magic' });
  assert.throws(() => parseWire(Buffer.alloc(0)), { message: 'bad magic' });
});

test('throws bad magic when the first byte is not 0', () => {
  assert.throws(() => parseWire(Buffer.from([1, 0, 0, 0, 1, 0x00])), { message: 'bad magic' });
});

test('throws bad magic for a null value (tombstone)', () => {
  assert.throws(() => parseWire(null), { message: 'bad magic' });
});

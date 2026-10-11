import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processBatch } from '../src/process.js';
import { createMemorySink } from '../src/clickhouse.js';

const TOPIC = 'shop.inventory.v1';
const PARTITION = 1;

const GOOD = (i) => ({
  event_id: `evt-${i}`,
  event_type: 'STOCK_RESTOCKED',
  sku: `SKU-${i}`,
  warehouse_id: 'WH-1',
  delta: i,
  occurred_at: 1767225600000 + i,
});

// Fake decode: buffers starting with 'POISON' fail, everything else is JSON.
async function decode(buf) {
  if (buf.toString().startsWith('POISON')) {
    throw new Error('bad payload');
  }
  return JSON.parse(buf.toString());
}

function makeDlq() {
  const sent = [];
  return {
    sent,
    async send(entry) {
      sent.push(entry);
    },
  };
}

function makeMessages() {
  return [
    { offset: '100', key: null, headers: {}, value: Buffer.from(JSON.stringify(GOOD(0))) },
    { offset: '101', key: null, headers: {}, value: Buffer.from('POISON-1') },
    { offset: '102', key: null, headers: {}, value: Buffer.from(JSON.stringify(GOOD(2))) },
    { offset: '103', key: null, headers: {}, value: Buffer.from('POISON-3') },
    { offset: '104', key: null, headers: {}, value: Buffer.from(JSON.stringify(GOOD(4))) },
  ];
}

test('poison messages go to the DLQ and good messages are inserted', async () => {
  const sink = createMemorySink();
  const dlq = makeDlq();
  const result = await processBatch({
    topic: TOPIC,
    partition: PARTITION,
    messages: makeMessages(),
    decode,
    sink,
    dlq,
  });

  assert.deepEqual(result, { inserted: 3, dlq: 2, lastOffset: '104' });
  assert.equal(sink.rows.length, 3);
  assert.deepEqual(
    sink.rows.map((r) => r.kafka_offset),
    ['100', '102', '104'],
  );
  assert.deepEqual(
    dlq.sent.map((s) => s.message.offset),
    ['101', '103'],
  );
  assert.equal(dlq.sent[0].partition, PARTITION);
  assert.equal(dlq.sent[0].error.message, 'bad payload');
});

test('the same batch processed twice inserts its rows once', async () => {
  const sink = createMemorySink();
  const dlq = makeDlq();
  const args = { topic: TOPIC, partition: PARTITION, decode, sink, dlq };

  await processBatch({ ...args, messages: makeMessages() });
  const second = await processBatch({ ...args, messages: makeMessages() });

  assert.equal(second.lastOffset, '104');
  assert.equal(sink.rows.length, 3);
  assert.equal(sink.tokens.size, 1);
  assert.ok(sink.tokens.has(`${TOPIC}-${PARTITION}-100-104`));
});

test('a batch with only poison messages inserts nothing and reports the last offset', async () => {
  const sink = createMemorySink();
  const dlq = makeDlq();
  const messages = [
    { offset: '7', key: null, headers: {}, value: Buffer.from('POISON-a') },
    { offset: '8', key: null, headers: {}, value: Buffer.from('POISON-b') },
  ];
  const result = await processBatch({ topic: TOPIC, partition: PARTITION, messages, decode, sink, dlq });
  assert.deepEqual(result, { inserted: 0, dlq: 2, lastOffset: '8' });
  assert.equal(sink.rows.length, 0);
  assert.equal(dlq.sent.length, 2);
});

test('an empty batch does nothing', async () => {
  const sink = createMemorySink();
  const dlq = makeDlq();
  const result = await processBatch({ topic: TOPIC, partition: PARTITION, messages: [], decode, sink, dlq });
  assert.deepEqual(result, { inserted: 0, dlq: 0, lastOffset: null });
  assert.equal(sink.rows.length, 0);
});

test('a sink that throws makes processBatch reject', async () => {
  const failing = {
    async insert() {
      throw new Error('clickhouse unavailable');
    },
  };
  await assert.rejects(
    processBatch({
      topic: TOPIC,
      partition: PARTITION,
      messages: makeMessages(),
      decode,
      sink: failing,
      dlq: makeDlq(),
    }),
    { message: 'clickhouse unavailable' },
  );
});

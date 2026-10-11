import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runConsumer, createDlq } from '../src/consumer.js';
import { createMemorySink } from '../src/clickhouse.js';

// Fake kafkajs client: no sockets. Captures the eachBatch handler and the run options.
function fakeKafka() {
  const state = { run: null, subscribed: null, committed: [] };
  const kafka = {
    consumer() {
      return {
        async connect() {},
        async subscribe(args) {
          state.subscribed = args;
        },
        async run(opts) {
          state.run = opts;
        },
      };
    },
  };
  return { kafka, state };
}

test('runConsumer uses manual commit and resolves then commits only after the batch', async () => {
  const { kafka, state } = fakeKafka();
  const sink = createMemorySink();
  const resolved = [];
  const committedArgs = [];

  await runConsumer({
    kafka,
    topic: 'shop.inventory.v1',
    decode: async (buf) => JSON.parse(buf.toString()),
    sink,
    dlq: { send: async () => {} },
  });

  assert.equal(state.subscribed.topic, 'shop.inventory.v1');
  assert.equal(state.run.autoCommit, false);
  assert.equal(state.run.eachBatchAutoResolve, false);

  const event = {
    event_id: 'e1',
    event_type: 'STOCK_RESERVED',
    sku: 'SKU-1',
    warehouse_id: 'WH-1',
    delta: -1,
    occurred_at: 1767225600000,
  };
  await state.run.eachBatch({
    batch: {
      topic: 'shop.inventory.v1',
      partition: 0,
      messages: [
        { offset: '5', value: Buffer.from(JSON.stringify(event)), key: null, headers: {} },
        { offset: '6', value: Buffer.from(JSON.stringify(event)), key: null, headers: {} },
      ],
    },
    resolveOffset: (o) => resolved.push(o),
    commitOffsetsIfNecessary: async (offsets) => committedArgs.push(offsets),
    uncommittedOffsets: () => ({ topics: [{ topic: 'shop.inventory.v1', partitions: [{ partition: 0, offset: '7' }] }] }),
    heartbeat: async () => {},
  });

  assert.deepEqual(resolved, ['6']);
  assert.equal(sink.rows.length, 2);
  assert.equal(committedArgs.length, 1);
  assert.equal(committedArgs[0].topics[0].partitions[0].offset, '7');
});

test('a failing insert throws from eachBatch so nothing is resolved or committed', async () => {
  const { kafka, state } = fakeKafka();
  const resolved = [];
  const committed = [];
  await runConsumer({
    kafka,
    topic: 't',
    decode: async () => ({ event_id: 'x', event_type: 'STOCK_RESTOCKED', sku: 's', warehouse_id: 'w', delta: 1, occurred_at: 0 }),
    sink: { insert: async () => { throw new Error('insert failed'); } },
    dlq: { send: async () => {} },
  });
  await assert.rejects(
    state.run.eachBatch({
      batch: { topic: 't', partition: 0, messages: [{ offset: '1', value: Buffer.from('{}'), key: null, headers: {} }] },
      resolveOffset: (o) => resolved.push(o),
      commitOffsetsIfNecessary: async (o) => committed.push(o),
      uncommittedOffsets: () => ({ topics: [] }),
      heartbeat: async () => {},
    }),
    { message: 'insert failed' },
  );
  assert.deepEqual(resolved, []);
  assert.deepEqual(committed, []);
});

test('createDlq writes value, key and x-error / x-source-* headers', async () => {
  const sent = [];
  const dlq = createDlq({ send: async (msg) => sent.push(msg) });
  await dlq.send({
    message: { offset: '42', key: Buffer.from('k'), value: Buffer.from('v') },
    error: new Error('bad magic'),
    partition: 3,
  });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].topic, 'shop.inventory.dlq');
  const [m] = sent[0].messages;
  assert.equal(m.value.toString(), 'v');
  assert.equal(m.headers['x-error'], 'bad magic');
  assert.equal(m.headers['x-source-partition'], '3');
  assert.equal(m.headers['x-source-offset'], '42');
});

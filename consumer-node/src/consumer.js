import { processBatch } from './process.js';

export const GROUP_ID = 'streamline-inventory';
export const DLQ_TOPIC = 'shop.inventory.dlq';

// DLQ producer: keeps the original value and key, adds error and source-position headers.
export function createDlq(producer, topic = DLQ_TOPIC) {
  return {
    async send({ message, error, partition }) {
      await producer.send({
        topic,
        messages: [
          {
            key: message.key ?? null,
            value: message.value ?? null,
            headers: {
              'x-error': String(error?.message ?? error),
              'x-source-partition': String(partition),
              'x-source-offset': String(message.offset),
            },
          },
        ],
      });
    },
  };
}

// Wires a kafkajs consumer to processBatch. Offsets are resolved and committed
// only after the ClickHouse insert succeeds. On failure the handler throws, so
// kafkajs retries the batch and nothing is committed.
export async function runConsumer({ kafka, topic, decode, sink, dlq, groupId = GROUP_ID }) {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, uncommittedOffsets, heartbeat }) => {
      const result = await processBatch({
        topic: batch.topic,
        partition: batch.partition,
        messages: batch.messages,
        decode,
        sink,
        dlq,
      });
      await heartbeat();
      if (result.lastOffset !== null) {
        resolveOffset(result.lastOffset);
      }
      // With autoCommit:false and no autoCommitInterval/Threshold, a bare
      // commitOffsetsIfNecessary() never commits in kafkajs 2.2.4. Passing the
      // resolved offsets explicitly commits them right after the insert.
      await commitOffsetsIfNecessary(uncommittedOffsets());
    },
  });

  return consumer;
}

import { toInventoryRow } from './rows.js';

// Processes one partition's batch of kafkajs messages ({offset, value, key, headers}).
//
// - decode(buffer) is async and returns the event. A decode or row-mapping failure
//   sends the message to dlq ({message, error, partition}) and excludes it.
// - Valid rows are inserted once, as one batch covering the whole messages array.
//   First and last offsets come from that array (not just the valid rows), so the
//   dedupe token is identical when the same batch is retried.
// - Insert errors propagate. The caller must not resolve offsets in that case.
export async function processBatch({ topic, partition, messages, decode, sink, dlq }) {
  if (!messages || messages.length === 0) {
    return { inserted: 0, dlq: 0, lastOffset: null };
  }

  const rows = [];
  let dlqCount = 0;

  for (const message of messages) {
    try {
      const event = await decode(message.value);
      rows.push(toInventoryRow(event, partition, message.offset));
    } catch (error) {
      dlqCount += 1;
      await dlq.send({ message, error, partition });
    }
  }

  const firstOffset = messages[0].offset;
  const lastOffset = messages[messages.length - 1].offset;

  if (rows.length > 0) {
    await sink.insert({ topic, partition, firstOffset, lastOffset, rows });
  }

  return { inserted: rows.length, dlq: dlqCount, lastOffset };
}

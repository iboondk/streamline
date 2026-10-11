// Pure lag arithmetic. No network access here, so it is unit-testable without Kafka.
//
// Inputs are the shapes returned by kafkajs admin methods:
//   topicOffsets: admin.fetchTopicOffsets(topic) -> [{ partition, high, low? }]
//   groupOffsets: admin.fetchOffsets({ groupId, topics }) -> [{ topic, partitions: [{ partition, offset }] }]
//
// Offsets arrive as strings (int64). A committed offset of '-1' (or no entry for the
// partition) means the group has nothing committed there, so the whole partition
// counts as lag: lag = high.

function toOffset(value, label) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw new TypeError(`${label} is not a number: ${JSON.stringify(value)}`);
  }
  return n;
}

export function computeLag(topicOffsets, groupOffsets) {
  if (!Array.isArray(topicOffsets)) {
    throw new TypeError('topicOffsets must be an array of { partition, high }');
  }
  if (!Array.isArray(groupOffsets)) {
    throw new TypeError('groupOffsets must be an array of { topic, partitions }');
  }

  // Flatten committed offsets across the returned topic entries. Partition numbers are
  // only unique within a topic, so duplicates across entries are refused rather than guessed.
  const committed = new Map();
  for (const entry of groupOffsets) {
    for (const p of entry.partitions ?? []) {
      if (committed.has(p.partition)) {
        throw new Error(`partition ${p.partition} appears in more than one group offset entry`);
      }
      committed.set(p.partition, p.offset);
    }
  }

  const perPartition = topicOffsets.map(({ partition, high }) => {
    const highOffset = toOffset(high, `high offset for partition ${partition}`);
    const raw = committed.get(partition);
    const hasCommit = raw !== undefined && raw !== null && String(raw) !== '-1';
    const lag = hasCommit
      ? Math.max(0, highOffset - toOffset(raw, `committed offset for partition ${partition}`))
      : highOffset;
    return { partition, lag };
  });

  perPartition.sort((a, b) => a.partition - b.partition);
  const total = perPartition.reduce((sum, p) => sum + p.lag, 0);
  return { perPartition, total };
}

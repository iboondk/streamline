// Dedupe token for one partition's contiguous offset range. Retrying the same
// batch (same first and last offset) produces the same token, so ClickHouse drops it.

export function dedupToken(topic, partition, firstOffset, lastOffset) {
  return `${topic}-${partition}-${firstOffset}-${lastOffset}`;
}

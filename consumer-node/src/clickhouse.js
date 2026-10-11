import { createClient } from '@clickhouse/client';
import { dedupToken } from './batch.js';

// Real sink. Each call inserts one partition's batch with a dedupe token, so a
// retried identical batch is dropped by ClickHouse (raw table and dependent MVs).
export function createSink({ url, username, password, database }) {
  const client = createClient({ url, username, password, database });
  return {
    async insert({ topic, partition, firstOffset, lastOffset, rows }) {
      await client.insert({
        table: 'inventory_raw',
        values: rows,
        format: 'JSONEachRow',
        clickhouse_settings: {
          insert_deduplication_token: dedupToken(topic, partition, firstOffset, lastOffset),
          deduplicate_blocks_in_dependent_materialized_views: 1,
        },
      });
    },
    async close() {
      await client.close();
    },
  };
}

// In-memory sink with the same token semantics, for tests. A repeated token is
// ignored, as ClickHouse would ignore it.
export function createMemorySink() {
  const rows = [];
  const tokens = new Set();
  return {
    rows,
    tokens,
    async insert({ topic, partition, firstOffset, lastOffset, rows: batch }) {
      const token = dedupToken(topic, partition, firstOffset, lastOffset);
      if (tokens.has(token)) return;
      tokens.add(token);
      rows.push(...batch);
    },
    async close() {},
  };
}

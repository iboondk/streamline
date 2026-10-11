// Live samplers. These talk to Kafka and ClickHouse and return the numbers they read.
// They never estimate or fill in a value: an error propagates to the caller.

import { computeLag } from './lag.js';

const TABLE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Total lag (sum over partitions) for one consumer group on one topic.
export async function sampleLag(admin, { group, topic }) {
  const topicOffsets = await admin.fetchTopicOffsets(topic);
  const groupOffsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
  return computeLag(topicOffsets, groupOffsets);
}

// Row count of streamline.<table>. The table name is checked so it cannot inject SQL.
export async function sampleRows(client, table) {
  if (!TABLE_NAME.test(table)) {
    throw new Error(`invalid table name: ${JSON.stringify(table)}`);
  }
  const result = await client.query({
    query: `SELECT count() AS rows FROM streamline.${table}`,
    format: 'JSONEachRow',
  });
  const [row] = await result.json();
  const rows = Number(row?.rows);
  if (!Number.isFinite(rows)) {
    throw new Error(`count() on streamline.${table} returned no number`);
  }
  return rows;
}

export { TABLE_NAME };

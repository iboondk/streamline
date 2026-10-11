// Maps a decoded InventoryEvent (inventory_event.avsc) to a ClickHouse inventory_raw row.

// occurred_at is timestamp-millis. avsc may hand back a Date, or a number of ms.
function toEpochMs(value) {
  if (value instanceof Date) return value.getTime();
  const ms = Number(value);
  if (!Number.isFinite(ms)) throw new Error(`invalid occurred_at: ${String(value)}`);
  return ms;
}

// 'YYYY-MM-DD HH:MM:SS.mmm' in UTC, the DateTime64(3) text form ClickHouse accepts.
export function formatOccurredAt(ms) {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '');
}

export function toInventoryRow(event, partition, offset) {
  return {
    event_id: event.event_id,
    event_type: event.event_type,
    sku: event.sku,
    warehouse_id: event.warehouse_id,
    delta: Number(event.delta),
    occurred_at: formatOccurredAt(toEpochMs(event.occurred_at)),
    kafka_partition: Number(partition),
    kafka_offset: String(offset),
  };
}

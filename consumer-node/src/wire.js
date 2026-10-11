// Confluent wire format: magic byte 0, 4-byte big-endian schema id, then the Avro payload.

export function parseWire(buf) {
  if (!buf || buf.length < 5 || buf[0] !== 0) {
    throw new Error('bad magic');
  }
  const b = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
  return {
    schemaId: b.readUInt32BE(1),
    payload: b.subarray(5),
  };
}

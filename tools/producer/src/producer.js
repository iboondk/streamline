import { readFileSync } from 'node:fs';
import { Kafka } from 'kafkajs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';

// This file lives in tools/producer/src, so the repo schemas dir is three levels up.
const SCHEMA_DIR = new URL('../../../schemas/', import.meta.url);
const ORDER_SCHEMA_TEXT = readFileSync(new URL('order_event.avsc', SCHEMA_DIR), 'utf8');
const INVENTORY_SCHEMA_TEXT = readFileSync(new URL('inventory_event.avsc', SCHEMA_DIR), 'utf8');

function parseBrokers(brokers) {
  if (Array.isArray(brokers)) return brokers;
  return String(brokers)
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
}

async function registerSubject(registry, topic, schemaText) {
  const { id } = await registry.register(
    { type: SchemaType.AVRO, schema: schemaText },
    { subject: `${topic}-value` },
  );
  return id;
}

/**
 * Creates an idempotent Kafka producer that writes Avro-encoded events through
 * Schema Registry. Subjects follow TopicNameStrategy: `<topic>-value`.
 *
 * @param {{brokers: string|string[], registryUrl: string, ordersTopic: string, inventoryTopic: string}} config
 */
export async function createProducer({ brokers, registryUrl, ordersTopic, inventoryTopic }) {
  const registry = new SchemaRegistry({ host: registryUrl });
  const ordersSchemaId = await registerSubject(registry, ordersTopic, ORDER_SCHEMA_TEXT);
  const inventorySchemaId = await registerSubject(registry, inventoryTopic, INVENTORY_SCHEMA_TEXT);

  const kafka = new Kafka({ clientId: 'streamline-producer', brokers: parseBrokers(brokers) });
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();

  async function sendBatch(topic, schemaId, events, keyOf) {
    if (events.length === 0) return 0;
    const messages = await Promise.all(
      events.map(async (event) => ({
        key: keyOf(event),
        value: await registry.encode(schemaId, event),
      })),
    );
    await producer.send({ topic, acks: -1, messages });
    return messages.length;
  }

  return {
    sendOrders(events) {
      return sendBatch(ordersTopic, ordersSchemaId, events, (e) => e.order_id);
    },
    sendInventory(events) {
      return sendBatch(inventoryTopic, inventorySchemaId, events, (e) => e.sku);
    },
    async close() {
      await producer.disconnect();
    },
  };
}

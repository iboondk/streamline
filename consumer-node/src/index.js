#!/usr/bin/env node
import { Kafka } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { createSink } from './clickhouse.js';
import { createDlq, runConsumer, GROUP_ID } from './consumer.js';

// Defaults mirror .env.example (local dummy values only).
const env = process.env;
const config = {
  brokers: (env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((s) => s.trim()).filter(Boolean),
  schemaRegistryUrl: env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081',
  clickhouseUrl: env.CLICKHOUSE_URL ?? 'http://localhost:8123',
  clickhouseUser: env.CLICKHOUSE_USER ?? 'streamline',
  clickhousePassword: env.CLICKHOUSE_PASSWORD ?? 'change-me-local-only',
  clickhouseDb: env.CLICKHOUSE_DB ?? 'streamline',
  inventoryTopic: env.INVENTORY_TOPIC ?? 'shop.inventory.v1',
};

async function main() {
  const kafka = new Kafka({
    clientId: 'streamline-inventory-node',
    brokers: config.brokers,
  });
  const registry = new SchemaRegistry({ host: config.schemaRegistryUrl });
  const sink = createSink({
    url: config.clickhouseUrl,
    username: config.clickhouseUser,
    password: config.clickhousePassword,
    database: config.clickhouseDb,
  });

  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  const dlq = createDlq(producer);

  const consumer = await runConsumer({
    kafka,
    topic: config.inventoryTopic,
    decode: (buf) => registry.decode(buf),
    sink,
    dlq,
    groupId: GROUP_ID,
  });

  console.log(`streamline-inventory consumer running: group=${GROUP_ID} topic=${config.inventoryTopic}`);

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    console.log(`received ${signal}, disconnecting`);
    try {
      await consumer.disconnect();
      await producer.disconnect();
      await sink.close();
    } catch (err) {
      console.error('error during shutdown', err);
      process.exitCode = 1;
    }
    process.exit(process.exitCode ?? 0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('fatal', err);
  process.exit(1);
});

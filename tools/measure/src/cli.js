#!/usr/bin/env node
// Samples consumer-group lag (Kafka admin) and ClickHouse row growth on a fixed schedule,
// then writes results/measure-<ISO timestamp>.json and prints the summary.
//
// Usage:
//   node src/cli.js --group streamline-orders --topic shop.orders.v1 --table orders_raw \
//     [--interval 1000] [--duration 60]
//
// Connection settings come from the environment (see .env.example):
//   KAFKA_BROKERS (default localhost:9092), CLICKHOUSE_URL (default http://localhost:8123),
//   CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB (defaults match .env.example).
//
// Needs a live stack. Without one, the run fails with a connection error and writes nothing.

import { parseArgs } from 'node:util';
import { mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { Kafka } from 'kafkajs';
import { createClient } from '@clickhouse/client';
import { sampleLag, sampleRows, TABLE_NAME } from './sampler.js';
import { summarize } from './stats.js';

const RESULTS_DIR = fileURLToPath(new URL('../../../results/', import.meta.url));

const USAGE = `usage: node src/cli.js --group <consumer-group> --topic <topic> --table <table>
                  [--interval <ms>=1000] [--duration <sec>=60]`;

function parsePositiveInt(raw, name) {
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`--${name} must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return n;
}

export function parseCliArgs(argv) {
  const { values } = parseArgs({
    args: argv,
    strict: true,
    options: {
      group: { type: 'string' },
      topic: { type: 'string' },
      table: { type: 'string' },
      interval: { type: 'string', default: '1000' },
      duration: { type: 'string', default: '60' },
      help: { type: 'boolean', short: 'h' },
    },
  });
  if (values.help) return { help: true };

  for (const key of ['group', 'topic', 'table']) {
    if (!values[key]) throw new Error(`missing required --${key}`);
  }
  if (!TABLE_NAME.test(values.table)) {
    throw new Error(`invalid --table ${JSON.stringify(values.table)}`);
  }
  const intervalMs = parsePositiveInt(values.interval, 'interval');
  const durationSec = parsePositiveInt(values.duration, 'duration');
  if (Math.floor((durationSec * 1000) / intervalMs) + 1 < 2) {
    throw new Error('--duration and --interval give fewer than 2 samples; lengthen the duration or shorten the interval');
  }
  return {
    group: values.group,
    topic: values.topic,
    table: values.table,
    intervalMs,
    durationSec,
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function run(config, env = process.env) {
  const brokers = (env.KAFKA_BROKERS || 'localhost:9092').split(',').map((b) => b.trim()).filter(Boolean);
  const kafka = new Kafka({ clientId: 'streamline-measure', brokers });
  const admin = kafka.admin();
  const ch = createClient({
    url: env.CLICKHOUSE_URL || 'http://localhost:8123',
    username: env.CLICKHOUSE_USER || 'streamline',
    password: env.CLICKHOUSE_PASSWORD ?? 'change-me-local-only',
    database: env.CLICKHOUSE_DB || 'streamline',
  });

  const samples = [];
  const startedAt = Date.now();
  const endAt = startedAt + config.durationSec * 1000;
  try {
    await admin.connect();
    // Ticks are scheduled from the start time, so the schedule does not drift.
    for (let k = 0; startedAt + k * config.intervalMs <= endAt; k++) {
      const due = startedAt + k * config.intervalMs;
      const wait = due - Date.now();
      if (wait > 0) await sleep(wait);
      const t = Date.now();
      const { total: lag } = await sampleLag(admin, config);
      const rows = await sampleRows(ch, config.table);
      samples.push({ t, rows, lag });
      process.stderr.write(`sample ${samples.length}: lag=${lag} rows=${rows}\n`);
    }
  } finally {
    await ch.close().catch(() => {});
    await admin.disconnect().catch(() => {});
  }

  const summary = summarize(samples);
  const file = resolve(RESULTS_DIR, `measure-${new Date(startedAt).toISOString().replace(/:/g, '-')}.json`);
  const report = {
    config: { ...config, brokers },
    samples,
    summary,
    env: { node: process.version, platform: process.platform },
  };
  mkdirSync(RESULTS_DIR, { recursive: true });
  writeFileSync(file, `${JSON.stringify(report, null, 2)}\n`, { flag: 'wx' });
  return { file, summary };
}

async function main(argv) {
  let config;
  try {
    config = parseCliArgs(argv);
  } catch (err) {
    console.error(`${err.message}\n${USAGE}`);
    return 2;
  }
  if (config.help) {
    console.log(USAGE);
    return 0;
  }
  try {
    const { file, summary } = await run(config);
    console.log(JSON.stringify(summary, null, 2));
    console.log(`wrote ${file}`);
    return 0;
  } catch (err) {
    console.error(`measurement failed: ${err.message}`);
    return 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  process.exitCode = await main(process.argv.slice(2));
}

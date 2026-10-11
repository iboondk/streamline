#!/usr/bin/env node
import { createGenerator } from './generator.js';
import { createProducer } from './producer.js';

const TICK_MS = 100;
// Fixed epoch (2026-01-01T00:00:00Z) so --dry-run output is reproducible.
const DRY_RUN_EPOCH_MS = 1767225600000;

const USAGE = `Usage: node src/cli.js [options]
  --rate <n>      events per second (default 500)
  --duration <s>  run length in seconds (default 30)
  --seed <n>      generator seed (default 42)
  --mix <f>       fraction of events that are orders, 0..1 (default 0.5)
  --dry-run       print generated events as JSON lines to stdout, no Kafka connection

Env: KAFKA_BROKERS, SCHEMA_REGISTRY_URL, ORDERS_TOPIC, INVENTORY_TOPIC
(defaults match .env.example)`;

function parseArgs(argv) {
  const opts = { rate: 500, duration: 30, seed: 42, mix: 0.5, dryRun: false };
  const numericFlags = { '--rate': 'rate', '--duration': 'duration', '--seed': 'seed', '--mix': 'mix' };

  for (let i = 0; i < argv.length; i++) {
    let flag = argv[i];
    let inlineValue;
    const eq = flag.indexOf('=');
    if (flag.startsWith('--') && eq !== -1) {
      inlineValue = flag.slice(eq + 1);
      flag = flag.slice(0, eq);
    }
    if (flag === '--dry-run') {
      opts.dryRun = true;
      continue;
    }
    if (flag === '--help' || flag === '-h') {
      console.log(USAGE);
      process.exit(0);
    }
    const key = numericFlags[flag];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    const raw = inlineValue ?? argv[++i];
    if (raw === undefined) throw new Error(`${flag} needs a value`);
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error(`${flag} must be a number, got "${raw}"`);
    opts[key] = value;
  }

  if (!(opts.rate > 0)) throw new Error('--rate must be > 0');
  if (!(opts.duration > 0)) throw new Error('--duration must be > 0');
  if (!Number.isInteger(opts.seed)) throw new Error('--seed must be an integer');
  if (!(opts.mix >= 0 && opts.mix <= 1)) throw new Error('--mix must be between 0 and 1');
  return opts;
}

// Deterministic split of events into orders and inventory at the requested fraction.
function makeMixer(mix) {
  let acc = 0;
  return () => {
    acc += mix;
    if (acc >= 1 - 1e-9) {
      acc -= 1;
      return true;
    }
    return false;
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function round(n, digits) {
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

function runDryRun({ rate, duration, seed, mix }) {
  const total = Math.round(rate * duration);
  const gen = createGenerator({ seed });
  const isOrder = makeMixer(mix);
  const lines = [];
  for (let i = 0; i < total; i++) {
    const nowMs = DRY_RUN_EPOCH_MS + Math.floor((i * 1000) / rate);
    const event = isOrder() ? gen.nextOrder(nowMs) : gen.nextInventory(nowMs);
    lines.push(JSON.stringify(event));
  }
  if (lines.length > 0) process.stdout.write(lines.join('\n') + '\n');
}

async function runLive({ rate, duration, seed, mix }) {
  const producer = await createProducer({
    brokers: process.env.KAFKA_BROKERS ?? 'localhost:9092',
    registryUrl: process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081',
    ordersTopic: process.env.ORDERS_TOPIC ?? 'shop.orders.v1',
    inventoryTopic: process.env.INVENTORY_TOPIC ?? 'shop.inventory.v1',
  });

  const gen = createGenerator({ seed });
  const isOrder = makeMixer(mix);
  const ticks = Math.round((duration * 1000) / TICK_MS);
  const perTickRate = (rate * TICK_MS) / 1000;
  let carry = 0;
  let sent = 0;
  const startWall = Date.now();
  let nextTickAt = startWall;

  try {
    for (let t = 0; t < ticks; t++) {
      const want = perTickRate + carry;
      const count = Math.floor(want);
      carry = want - count;

      const nowMs = Date.now();
      const orders = [];
      const inventory = [];
      for (let k = 0; k < count; k++) {
        if (isOrder()) orders.push(gen.nextOrder(nowMs));
        else inventory.push(gen.nextInventory(nowMs));
      }
      // Count only what the broker acknowledged.
      sent += await producer.sendOrders(orders);
      sent += await producer.sendInventory(inventory);

      nextTickAt += TICK_MS;
      const wait = nextTickAt - Date.now();
      if (wait > 0) await sleep(wait);
    }
  } finally {
    await producer.close();
  }

  const elapsedSec = (Date.now() - startWall) / 1000;
  const achievedRate = elapsedSec > 0 ? sent / elapsedSec : 0;
  console.log(
    JSON.stringify({
      sent,
      durationSec: round(elapsedSec, 3),
      achievedRate: round(achievedRate, 2),
    }),
  );
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.dryRun) {
    runDryRun(opts);
    return;
  }
  await runLive(opts);
}

// Exit quietly when the reader closes the pipe (for example `| head -3`).
process.stdout.on('error', (err) => {
  if (err.code === 'EPIPE') process.exit(0);
  throw err;
});

main().catch((err) => {
  console.error(`streamline-producer: ${err.message}`);
  process.exit(1);
});

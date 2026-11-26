// Repo benchmark. Spawns the real processor (child process) against a fresh topic, with the fake ETA
// dependency in-process, and measures:
//   drain:  N events already in the topic -> time until all are committed (events/s)
//   steady: a fixed produce rate for a few seconds -> processing lag (Kafka append to DB commit) p50/p99
// Each scenario runs RUNS times; the report is the median. Usage: node scripts/bench.ts [--runs 3]
import { spawn } from "node:child_process";
import { openSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { writeFileSync, mkdirSync } from "node:fs";
import { parseArgs } from "node:util";
import os from "node:os";
import { createPool } from "../src/db/pool.ts";
import { migrate } from "../src/db/migrate.ts";
import { createKafka, createProducer, ensureTopics } from "../src/kafka/client.ts";
import { buildDownstream } from "../src/downstream/server.ts";
import { journey } from "../src/sim/events.ts";

const { values } = parseArgs({ options: { runs: { type: "string", default: "3" }, journeys: { type: "string", default: "500" }, rate: { type: "string", default: "200" }, seconds: { type: "string", default: "10" } } });
const RUNS = Number(values.runs), JOURNEYS = Number(values.journeys), RATE = Number(values.rate), SECONDS = Number(values.seconds);
const DB = process.env.BENCH_DATABASE_URL ?? "postgres://shipevents:shipevents-dev@localhost:54310/shipevents_test";

const pool = createPool(DB, 4);
await migrate(pool);
const kafka = createKafka("bench");
const producer = createProducer(kafka);
await producer.connect();
const ds = buildDownstream();
const dsUrl = await ds.app.listen({ port: 0, host: "127.0.0.1" });

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function startProcessor(topic: string, group: string, concurrency: number) {
  const child = spawn(process.execPath, ["src/processor/main.ts"], {
    env: { ...process.env, DATABASE_URL: DB, EVENTS_TOPIC: topic, DLQ_TOPIC: `${topic}.dlq`, CONSUMER_GROUP: group, PROCESSOR_PORT: "0", DOWNSTREAM_URL: dsUrl, LOG_LEVEL: "warn", PARTITION_CONCURRENCY: String(concurrency) },
    stdio: ["ignore", openSync(".runtime/logs/bench-processor.log", "a"), openSync(".runtime/logs/bench-processor.log", "a")],
  });
  return child;
}

async function countDone(prefix: string): Promise<number> {
  const r = await pool.query("SELECT count(*)::int AS n FROM processed_events WHERE shipment_id LIKE $1", [`${prefix}%`]);
  return r.rows[0].n;
}

async function drain(concurrency: number): Promise<number> {
  const run = randomUUID().slice(0, 6).toUpperCase();
  const topic = `bench.${run}.events`;
  await ensureTopics(kafka, [topic, `${topic}.dlq`]);
  const prefix = `BD${run}`;
  const events = Array.from({ length: JOURNEYS }, (_, i) => journey(`${prefix}-${i}`)).flat();
  for (let i = 0; i < events.length; i += 500) {
    await producer.send({ topic, messages: events.slice(i, i + 500).map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })) });
  }
  const child = startProcessor(topic, `bench-${run}`, concurrency);
  let started = 0;
  while ((await countDone(prefix)) === 0) await sleep(20); // clock starts at the first commit, not at group join
  started = performance.now();
  while ((await countDone(prefix)) < events.length) await sleep(50);
  const secs = (performance.now() - started) / 1000;
  child.kill("SIGTERM");
  return Math.round(events.length / secs);
}

async function steady(concurrency: number): Promise<{ p50: number; p99: number; sent: number }> {
  const run = randomUUID().slice(0, 6).toUpperCase();
  const topic = `bench.${run}.events`;
  await ensureTopics(kafka, [topic, `${topic}.dlq`]);
  const prefix = `BS${run}`;
  const child = startProcessor(topic, `bench-${run}`, concurrency);
  // warm up: wait for the group to be live
  const warm = journey(`${prefix}-WARM`, 1);
  await producer.send({ topic, messages: warm.map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })) });
  while ((await countDone(`${prefix}-WARM`)) < 1) await sleep(50);
  const total = RATE * SECONDS;
  const all = Array.from({ length: Math.ceil(total / 6) }, (_, i) => journey(`${prefix}-${i}`)).flat().slice(0, total);
  const t0 = performance.now();
  for (let i = 0; i < all.length; i += RATE / 10) {
    await producer.send({ topic, messages: all.slice(i, i + RATE / 10).map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })) });
    const due = t0 + ((i + RATE / 10) / RATE) * 1000;
    const wait = due - performance.now();
    if (wait > 0) await sleep(wait);
  }
  while ((await countDone(prefix)) < all.length + 1) await sleep(50);
  const r = await pool.query(
    `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50, percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99
     FROM (SELECT extract(epoch FROM (processed_at - kafka_ts)) * 1000 AS ms FROM shipment_history WHERE shipment_id LIKE $1 AND shipment_id NOT LIKE $2) x`,
    [`${prefix}%`, `${prefix}-WARM`],
  );
  child.kill("SIGTERM");
  return { p50: Math.round(r.rows[0].p50), p99: Math.round(r.rows[0].p99), sent: all.length };
}

const report: Record<string, unknown> = {
  when: new Date().toISOString(),
  environment: `${os.cpus()[0]?.model ?? "cpu"} x${os.cpus().length}, node ${process.version}; Kafka+Postgres in a shared colima VM (4 vCPU, 6 GiB) with other projects running`,
  params: { runs: RUNS, drainEvents: JOURNEYS * 6, steadyRate: RATE, steadySeconds: SECONDS },
};
for (const c of [1, 3]) {
  const d: number[] = [], p50: number[] = [], p99: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    d.push(await drain(c));
    const s = await steady(c);
    p50.push(s.p50);
    p99.push(s.p99);
    console.log(`concurrency ${c} run ${i + 1}: drain ${d.at(-1)} ev/s, steady p50 ${s.p50}ms p99 ${s.p99}ms`);
  }
  report[`partitionConcurrency${c}`] = { drainEventsPerSec: { runs: d, median: median(d) }, steadyLagMs: { p50Runs: p50, p99Runs: p99, p50Median: median(p50), p99Median: median(p99) } };
}
mkdirSync("docs/bench", { recursive: true });
writeFileSync("docs/bench/results.json", JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
await producer.disconnect();
await ds.app.close();
await pool.end();

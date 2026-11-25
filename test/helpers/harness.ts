import type { Producer } from "kafkajs";
import { createPool, type Pool } from "../../src/db/pool.ts";
import { createKafka, createProducer, ensureTopics } from "../../src/kafka/client.ts";
import { log } from "../../src/log.ts";
import { startProcessor, type RunningProcessor } from "../../src/processor/consumer.ts";
import type { ShipmentEvent } from "../../src/events/types.ts";
import { sendEvents } from "../../src/sim/produce.ts";
import { TEST_BROKERS, TEST_DB_URL, uniqueNames, waitFor } from "./env.ts";
import { buildDownstream, defaultFaults, type FaultConfig } from "../../src/downstream/server.ts";
import type { RetryPolicy } from "../../src/processor/retry.ts";

/** Fast but real retry settings for tests: 4 attempts, 50/100/200ms backoff, 300ms per-attempt timeout. */
export const TEST_RETRY: RetryPolicy = { maxAttempts: 4, baseDelayMs: 50, maxDelayMs: 400, timeoutMs: 300, jitter: false };
export const TEST_RETRY_ENV = { RETRY_MAX_ATTEMPTS: "4", RETRY_BASE_MS: "50", RETRY_MAX_MS: "400", DOWNSTREAM_TIMEOUT_MS: "300", RETRY_JITTER: "0" };

export interface Harness {
  names: ReturnType<typeof uniqueNames>;
  pool: Pool;
  producer: Producer;
  proc: RunningProcessor | undefined;
  downstream: { url: string; faults: FaultConfig; stats: Record<string, number>; reset(): void };
  send(events: (ShipmentEvent | { raw: string; shipmentId: string })[], headers?: Record<string, string>): ReturnType<typeof sendEvents>;
  startProc(): Promise<void>;
  stopProc(): Promise<void>;
  shipment(id: string): Promise<Record<string, any> | undefined>;
  history(id: string): Promise<Record<string, any>[]>;
  waitStatus(id: string, status: string, timeoutMs?: number): Promise<Record<string, any>>;
  close(): Promise<void>;
}

export async function createHarness(label: string): Promise<Harness> {
  const names = uniqueNames(label);
  const pool = createPool(TEST_DB_URL, 4);
  const kafka = createKafka(`test-${label}`, TEST_BROKERS);
  await ensureTopics(kafka, [names.topic, names.dlqTopic]);
  const producer = createProducer(kafka);
  await producer.connect();
  const ds = buildDownstream();
  const url = await ds.app.listen({ port: 0, host: "127.0.0.1" });
  const h: Harness = {
    names, pool, producer, proc: undefined,
    downstream: {
      url, faults: ds.faults, stats: ds.stats,
      reset() {
        Object.assign(ds.faults, defaultFaults());
        for (const k of Object.keys(ds.stats)) delete ds.stats[k];
      },
    },
    send: (events, headers) => sendEvents(producer, names.topic, events, headers),
    async startProc() {
      h.proc = await startProcessor({ pool, log: log.child({ test: label }) }, { brokers: TEST_BROKERS, topic: names.topic, dlqTopic: names.dlqTopic, groupId: names.group, downstreamUrl: url, retry: TEST_RETRY });
    },
    async stopProc() {
      await h.proc?.stop();
      h.proc = undefined;
    },
    async shipment(id) {
      return (await pool.query("SELECT * FROM shipments WHERE shipment_id = $1", [id])).rows[0];
    },
    async history(id) {
      return (await pool.query("SELECT * FROM shipment_history WHERE shipment_id = $1 ORDER BY id", [id])).rows;
    },
    waitStatus(id, status, timeoutMs = 20_000) {
      return waitFor(async () => {
        const s = await h.shipment(id);
        return s?.status === status ? s : undefined;
      }, timeoutMs);
    },
    async close() {
      await h.stopProc();
      await producer.disconnect();
      await ds.app.close();
      await pool.end();
    },
  };
  return h;
}

/** Reads every message currently on a topic (from the beginning) with a throwaway consumer group. */
export async function readTopic(topic: string, expectAtLeast: number, timeoutMs = 15_000): Promise<{ key: string | null; value: any; headers: Record<string, string> }[]> {
  const kafka = createKafka("test-reader", TEST_BROKERS);
  const consumer = kafka.consumer({ groupId: `reader-${Math.random().toString(36).slice(2)}` });
  const out: { key: string | null; value: any; headers: Record<string, string> }[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const headers: Record<string, string> = {};
      for (const [k, v] of Object.entries(message.headers ?? {})) headers[k] = String(v);
      out.push({ key: message.key?.toString() ?? null, value: JSON.parse(String(message.value)), headers });
    },
  });
  try {
    await waitFor(async () => out.length >= expectAtLeast, timeoutMs);
  } finally {
    await consumer.disconnect();
  }
  return out;
}

import type { Producer } from "kafkajs";
import { createPool, type Pool } from "../../src/db/pool.ts";
import { createKafka, createProducer, ensureTopics } from "../../src/kafka/client.ts";
import { log } from "../../src/log.ts";
import { startProcessor, type RunningProcessor } from "../../src/processor/consumer.ts";
import type { ShipmentEvent } from "../../src/events/types.ts";
import { sendEvents } from "../../src/sim/produce.ts";
import { TEST_BROKERS, TEST_DB_URL, uniqueNames, waitFor } from "./env.ts";

export interface Harness {
  names: ReturnType<typeof uniqueNames>;
  pool: Pool;
  producer: Producer;
  proc: RunningProcessor | undefined;
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
  const h: Harness = {
    names, pool, producer, proc: undefined,
    send: (events, headers) => sendEvents(producer, names.topic, events, headers),
    async startProc() {
      h.proc = await startProcessor({ pool, log: log.child({ test: label }) }, { brokers: TEST_BROKERS, topic: names.topic, dlqTopic: names.dlqTopic, groupId: names.group });
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

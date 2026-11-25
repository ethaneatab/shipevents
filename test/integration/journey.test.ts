import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPool, type Pool } from "../../src/db/pool.ts";
import { createKafka, createProducer, ensureTopics } from "../../src/kafka/client.ts";
import { log } from "../../src/log.ts";
import { startProcessor, type RunningProcessor } from "../../src/processor/consumer.ts";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import { sendEvents } from "../../src/sim/produce.ts";
import { TEST_BROKERS, TEST_DB_URL, uniqueNames, waitFor } from "../helpers/env.ts";
import type { Producer } from "kafkajs";

const names = uniqueNames("journey");
let pool: Pool;
let proc: RunningProcessor;
let producer: Producer;

beforeAll(async () => {
  pool = createPool(TEST_DB_URL, 4);
  const kafka = createKafka("test", TEST_BROKERS);
  await ensureTopics(kafka, [names.topic]);
  producer = createProducer(kafka);
  await producer.connect();
  proc = await startProcessor({ pool, log: log.child({ test: "journey" }) }, { brokers: TEST_BROKERS, topic: names.topic, groupId: names.group });
});

afterAll(async () => {
  await proc?.stop();
  await producer?.disconnect();
  await pool?.end();
});

describe("happy path", () => {
  it("a full journey ends DELIVERED with six history rows", async () => {
    const id = newShipmentId("T");
    const recs = await sendEvents(producer, names.topic, journey(id));
    expect(new Set(recs.map((r) => r.partition)).size).toBe(1); // keyed by shipmentId -> one partition
    const row = await waitFor(async () => {
      const r = await pool.query("SELECT status, last_seq FROM shipments WHERE shipment_id = $1", [id]);
      return r.rows[0]?.status === "DELIVERED" ? r.rows[0] : undefined;
    });
    expect(row.last_seq).toBe(6);
    const h = await pool.query("SELECT count(*)::int AS n FROM shipment_history WHERE shipment_id = $1", [id]);
    expect(h.rows[0].n).toBe(6);
  });
});

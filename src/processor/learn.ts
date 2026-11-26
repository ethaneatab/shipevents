import type { FastifyInstance } from "fastify";
import type { Producer } from "kafkajs";
import type { Pool } from "../db/pool.ts";
import type { ShipmentEvent } from "../events/types.ts";
import { executeReplay, planReplay, recordDryRun } from "../replay/replay.ts";
import { journey, makeEvent, newShipmentId } from "../sim/events.ts";
import { getSteps, posKey } from "./steps.ts";

export interface LearnDeps {
  pool: Pool;
  producer: Producer;
  eventsTopic: string;
  downstreamUrl: string;
}

const PACE = "700";
const samples = new Map<string, ShipmentEvent[]>();
type Kind = "main" | "duplicate" | "stale" | "malformed" | "outage";

/**
 * Teaching endpoints (LEARN_MODE=1 only). They produce real events to the real topic and read back
 * real processor steps and database rows. Nothing here fakes a stage.
 */
export function registerLearn(app: FastifyInstance, deps: LearnDeps): void {
  const failNext = (shipmentId: string, count: number) =>
    fetch(`${deps.downstreamUrl}/faults/fail-next`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ shipmentId, count, status: 503 }) });

  async function produce(key: string, value: string, paced = true) {
    const [meta] = await deps.producer.send({ topic: deps.eventsTopic, messages: [{ key, value, headers: paced ? { "x-learn-pace-ms": PACE } : {} }] });
    return { partition: meta?.partition ?? -1, offset: meta?.baseOffset ?? "-1" };
  }

  app.post<{ Body: { kind?: Kind; shipmentId?: string } }>("/learn/sample", async (req, reply) => {
    const kind = req.body?.kind ?? "main";
    if (kind === "main") {
      const shipmentId = newShipmentId("LRN");
      const evs = journey(shipmentId, 3);
      samples.set(shipmentId, evs);
      // seq 1 goes first, unpaced, so the sample event (seq 2) has something to move from
      await produce(shipmentId, JSON.stringify(evs[0]), false);
      for (let i = 0; i < 50; i++) {
        const r = await deps.pool.query("SELECT 1 FROM shipments WHERE shipment_id = $1", [shipmentId]);
        if (r.rowCount) break;
        await new Promise((res) => setTimeout(res, 100));
      }
      await failNext(shipmentId, 1); // the first ETA call for this shipment fails once, so a retry is visible
      const value = JSON.stringify(evs[1]);
      return { kind, shipmentId, eventId: evs[1]!.eventId, key: shipmentId, body: evs[1], ...(await produce(shipmentId, value)) };
    }
    const shipmentId = req.body?.shipmentId ?? "";
    const evs = samples.get(shipmentId);
    if (!evs) return reply.code(400).send({ error: "submit the main sample first" });
    if (kind === "duplicate") {
      return { kind, shipmentId, eventId: evs[1]!.eventId, key: shipmentId, body: evs[1], ...(await produce(shipmentId, JSON.stringify(evs[1]))) };
    }
    if (kind === "stale") {
      const late = makeEvent(shipmentId, 1, "SHIPMENT_CREATED", { location: "Oakland, CA (resent late)" });
      return { kind, shipmentId, eventId: late.eventId, key: shipmentId, body: late, ...(await produce(shipmentId, JSON.stringify(late))) };
    }
    if (kind === "malformed") {
      const bad = { ...makeEvent(shipmentId, 3, "HUB_SCAN"), seq: "three" };
      return { kind, shipmentId, eventId: bad.eventId, key: shipmentId, body: bad, ...(await produce(shipmentId, JSON.stringify(bad))) };
    }
    // outage: the ETA service fails every attempt for this shipment's next event
    await failNext(shipmentId, 4);
    const scan = evs[2]!;
    return { kind, shipmentId, eventId: scan.eventId, key: shipmentId, body: scan, ...(await produce(shipmentId, JSON.stringify(scan))) };
  });

  app.get<{ Querystring: { partition: string; offset: string; eventId?: string; shipmentId?: string } }>("/learn/trace", async (req) => {
    const { partition, offset, eventId, shipmentId } = req.query;
    const q = async (sql: string, args: unknown[]) => (await deps.pool.query(sql, args)).rows[0] ?? null;
    return {
      steps: getSteps(posKey(Number(partition), offset)),
      rows: {
        processed_event: eventId ? await q("SELECT * FROM processed_events WHERE event_id = $1", [eventId]) : null,
        history: await q("SELECT * FROM shipment_history WHERE kafka_partition = $1 AND kafka_offset = $2 AND ($3::text IS NULL OR shipment_id = $3)", [Number(partition), offset, shipmentId ?? null]),
        dlq_entry: await q("SELECT * FROM dlq_entries WHERE source_topic = $1 AND source_partition = $2 AND source_offset = $3", [deps.eventsTopic, Number(partition), offset]),
        shipment: shipmentId ? await q("SELECT * FROM shipments WHERE shipment_id = $1", [shipmentId]) : null,
      },
    };
  });

  app.post<{ Body: { shipmentId?: string } }>("/learn/replay/plan", async (req) => {
    const filter = { shipmentIds: [req.body?.shipmentId ?? ""] };
    const plan = await planReplay(deps.pool, filter);
    return { dryRunId: await recordDryRun(deps.pool, "learn-page", filter, plan), plan };
  });

  app.post<{ Body: { dryRunId?: string } }>("/learn/replay/execute", async (req) => executeReplay(deps.pool, deps.producer, deps.eventsTopic, req.body?.dryRunId ?? "", "learn-page"));
}

import type { Pool } from "../db/pool.ts";
import { decodeEvent } from "../events/schema.ts";
import { TARGET_STATUS, type ShipmentEvent } from "../events/types.ts";
import type { Logger } from "../log.ts";
import { sendToDlq, type DlqSink } from "./dlq.ts";

export interface RawMessage {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
  key: string | null;
  value: Buffer | null;
  headers: Record<string, string>;
}

export interface Deps {
  pool: Pool;
  log: Logger;
  dlq: DlqSink;
}

export type Outcome = "applied" | "dlq";

export async function handleMessage(deps: Deps, msg: RawMessage): Promise<Outcome> {
  const decoded = decodeEvent(msg.value, msg.key);
  if (!decoded.ok) {
    const where = await sendToDlq(deps.pool, deps.dlq, {
      reason: decoded.reason,
      detail: { issues: decoded.issues },
      shipmentId: decoded.shipmentId,
      eventId: decoded.eventId,
      source: { topic: msg.topic, partition: msg.partition, offset: msg.offset, key: msg.key, value: String(msg.value ?? ""), headers: msg.headers },
    });
    deps.log.warn({ reason: decoded.reason, issues: decoded.issues, partition: msg.partition, offset: msg.offset, dlq: where }, "dead-lettered");
    return "dlq";
  }
  await applyEvent(deps, decoded.event, msg);
  return "applied";
}

async function applyEvent(deps: Deps, event: ShipmentEvent, msg: RawMessage): Promise<void> {
  const to = TARGET_STATUS[event.type];
  const client = await deps.pool.connect();
  try {
    await client.query("BEGIN");
    const prev = await client.query<{ status: string }>("SELECT status FROM shipments WHERE shipment_id = $1 FOR UPDATE", [event.shipmentId]);
    await client.query(
      `INSERT INTO shipments (shipment_id, status, last_event_id, last_seq, last_location)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (shipment_id) DO UPDATE SET status = $2, last_event_id = $3, last_seq = $4,
         last_location = $5, version = shipments.version + 1, updated_at = now()`,
      [event.shipmentId, to, event.eventId, event.seq, event.location ?? null],
    );
    await client.query(
      `INSERT INTO shipment_history (shipment_id, event_id, seq, event_type, status_from, status_to, outcome, occurred_at, kafka_partition, kafka_offset)
       VALUES ($1, $2, $3, $4, $5, $6, 'applied', $7, $8, $9)`,
      [event.shipmentId, event.eventId, event.seq, event.type, prev.rows[0]?.status ?? null, to, event.occurredAt, msg.partition, msg.offset],
    );
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  deps.log.info({ shipmentId: event.shipmentId, eventId: event.eventId, partition: msg.partition, offset: msg.offset, to }, "applied");
}

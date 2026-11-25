import type { Pool } from "../db/pool.ts";
import { TARGET_STATUS, type ShipmentEvent } from "../events/types.ts";
import type { Logger } from "../log.ts";

export interface RawMessage {
  value: Buffer | null;
  partition: number;
  offset: string;
  timestamp: string;
}

export interface Deps {
  pool: Pool;
  log: Logger;
}

export type Outcome = "applied";

/** Step 1: the simplest thing that works. Parse, overwrite the status, append history. */
export async function handleMessage(deps: Deps, msg: RawMessage): Promise<Outcome> {
  const event = JSON.parse(String(msg.value)) as ShipmentEvent;
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
  return "applied";
}

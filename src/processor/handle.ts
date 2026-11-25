import type { Pool, PoolClient } from "../db/pool.ts";
import { decide, decideLegacy, type Current, type Decision } from "../domain/stateMachine.ts";
import { decodeEvent } from "../events/schema.ts";
import type { ShipmentEvent, Status } from "../events/types.ts";
import type { Logger } from "../log.ts";
import { sabotaged } from "../lab/sabotage.ts";
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

export type Outcome = "applied" | "stale" | "rejected" | "duplicate" | "dlq";

function sourceOf(msg: RawMessage) {
  return { topic: msg.topic, partition: msg.partition, offset: msg.offset, key: msg.key, value: String(msg.value ?? ""), headers: msg.headers };
}

export async function handleMessage(deps: Deps, msg: RawMessage): Promise<Outcome> {
  const decoded = decodeEvent(msg.value, msg.key);
  if (!decoded.ok) {
    const where = await sendToDlq(deps.pool, deps.dlq, {
      reason: decoded.reason,
      detail: { issues: decoded.issues },
      shipmentId: decoded.shipmentId,
      eventId: decoded.eventId,
      source: sourceOf(msg),
    });
    deps.log.warn({ reason: decoded.reason, issues: decoded.issues, partition: msg.partition, offset: msg.offset, dlq: where }, "dead-lettered");
    return "dlq";
  }
  return applyEvent(deps, decoded.event, msg);
}

async function claim(client: PoolClient, event: ShipmentEvent, msg: RawMessage): Promise<boolean> {
  // @learn:dedupe-claim
  // Claim the event id inside the same transaction as the state change.
  // A second delivery of the same event finds the row and becomes a no-op.
  if (sabotaged("no_dedupe")) return true;
  const r = await client.query(
    `INSERT INTO processed_events (event_id, shipment_id, outcome, kafka_partition, kafka_offset)
     VALUES ($1, $2, 'pending', $3, $4) ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
    [event.eventId, event.shipmentId, msg.partition, msg.offset],
  );
  return r.rowCount === 1;
  // @learn:end dedupe-claim
}

async function applyEvent(deps: Deps, event: ShipmentEvent, msg: RawMessage): Promise<Outcome> {
  const ctx = { shipmentId: event.shipmentId, eventId: event.eventId, seq: event.seq, partition: msg.partition, offset: msg.offset, traceId: event.traceId };
  const client = await deps.pool.connect();
  let decision: Decision;
  try {
    await client.query("BEGIN");
    if (!(await claim(client, event, msg))) {
      await client.query("ROLLBACK");
      deps.log.info(ctx, "duplicate skipped");
      return "duplicate";
    }
    const cur = await client.query<{ status: Status; last_seq: number }>("SELECT status, last_seq FROM shipments WHERE shipment_id = $1 FOR UPDATE", [event.shipmentId]);
    const current: Current | null = cur.rows[0] ? { status: cur.rows[0].status, lastSeq: cur.rows[0].last_seq } : null;
    decision = sabotaged("no_transition_guard") ? decideLegacy(current, event) : decide(current, event);

    if (decision.kind === "apply") {
      await client.query(
        `INSERT INTO shipments (shipment_id, status, last_event_id, last_seq, last_location)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (shipment_id) DO UPDATE SET status = $2, last_event_id = $3, last_seq = $4,
           last_location = COALESCE($5, shipments.last_location), version = shipments.version + 1, updated_at = now()`,
        [event.shipmentId, decision.to, event.eventId, event.seq, event.location ?? null],
      );
    }
    const outcome: Outcome = decision.kind === "apply" ? "applied" : decision.kind === "stale" ? "stale" : "rejected";
    await client.query(
      `INSERT INTO shipment_history (shipment_id, event_id, seq, event_type, status_from, status_to, outcome, occurred_at, kafka_partition, kafka_offset, detail)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [event.shipmentId, event.eventId, event.seq, event.type, decision.from, decision.to, outcome, event.occurredAt, msg.partition, msg.offset,
       decision.kind === "apply" ? { location: event.location ?? null } : { reason: decision.reason, location: event.location ?? null }],
    );
    if (!sabotaged("no_dedupe")) await client.query("UPDATE processed_events SET outcome = $2 WHERE event_id = $1", [event.eventId, outcome]);
    if (decision.kind === "reject") {
      // Illegal transitions need a human: index + publish before COMMIT, so a crash re-runs both.
      await sendToDlq(client, deps.dlq, {
        reason: "illegal_transition",
        detail: { from: decision.from, to: decision.to, message: decision.reason },
        shipmentId: event.shipmentId, eventId: event.eventId, seq: event.seq, source: sourceOf(msg),
      });
    }
    await client.query("COMMIT");
    deps.log.info({ ...ctx, decision }, outcome);
    return outcome;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

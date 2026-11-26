import type { Pool, PoolClient } from "../db/pool.ts";
import { decide, decideLegacy, type Current, type Decision } from "../domain/stateMachine.ts";
import { decodeEvent } from "../events/schema.ts";
import type { ShipmentEvent, Status } from "../events/types.ts";
import type { Logger } from "../log.ts";
import { sabotaged } from "../lab/sabotage.ts";
import { sendToDlq, type DlqSink } from "./dlq.ts";
import { fetchEta, type EtaResponse } from "./etaClient.ts";
import { withRetry, type AttemptRecord, type RetryPolicy } from "./retry.ts";
import { pace, posKey, recordStep } from "./steps.ts";
import { downstreamDuration, downstreamRequests, failuresTotal, retriesTotal } from "../obs/metrics.ts";

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
  downstreamUrl: string;
  retry: RetryPolicy;
}

export type Outcome = "applied" | "stale" | "rejected" | "duplicate" | "dlq";

function sourceOf(msg: RawMessage) {
  return { topic: msg.topic, partition: msg.partition, offset: msg.offset, key: msg.key, value: String(msg.value ?? ""), headers: msg.headers };
}

export async function handleMessage(deps: Deps, msg: RawMessage): Promise<Outcome> {
  const key = posKey(msg.partition, msg.offset);
  recordStep(key, "consume", "ok", `partition ${msg.partition} offset ${msg.offset}`, { key: msg.key, headers: msg.headers });
  await pace(msg.headers);
  const decoded = decodeEvent(msg.value, msg.key);
  if (!decoded.ok) {
    recordStep(key, "schema", "fail", `${decoded.reason}: ${decoded.issues.join("; ")}`, { reason: decoded.reason, issues: decoded.issues });
    const where = await sendToDlq(deps.pool, deps.dlq, {
      reason: decoded.reason,
      detail: { issues: decoded.issues },
      shipmentId: decoded.shipmentId,
      eventId: decoded.eventId,
      source: sourceOf(msg),
    });
    failuresTotal.inc({ reason: decoded.reason });
    recordStep(key, "dlq", "info", `sent to DLQ partition ${where.partition} offset ${where.offset}`, { reason: decoded.reason, dlq: where });
    deps.log.warn({ reason: decoded.reason, issues: decoded.issues, partition: msg.partition, offset: msg.offset, dlq: where }, "dead-lettered");
    return "dlq";
  }
  recordStep(key, "schema", "ok", `v${decoded.event.schemaVersion} ${decoded.event.type} seq ${decoded.event.seq}`, decoded.event);
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

const NO_ETA: readonly Status[] = ["DELIVERED", "RETURNED"];

async function readCurrent(db: Pick<Pool, "query">, shipmentId: string, lock: boolean): Promise<Current | null> {
  const r = await db.query<{ status: Status; last_seq: number }>(`SELECT status, last_seq FROM shipments WHERE shipment_id = $1${lock ? " FOR UPDATE" : ""}`, [shipmentId]);
  return r.rows[0] ? { status: r.rows[0].status, lastSeq: r.rows[0].last_seq } : null;
}

async function applyEvent(deps: Deps, event: ShipmentEvent, msg: RawMessage): Promise<Outcome> {
  const ctx = { shipmentId: event.shipmentId, eventId: event.eventId, seq: event.seq, partition: msg.partition, offset: msg.offset, traceId: event.traceId };
  const guard = sabotaged("no_transition_guard") ? decideLegacy : decide;
  const key = posKey(msg.partition, msg.offset);
  await pace(msg.headers);

  // Cheap read-only checks first, so a duplicate or a stale event never costs a downstream call.
  if (!sabotaged("no_dedupe")) {
    const seen = await deps.pool.query("SELECT outcome FROM processed_events WHERE event_id = $1", [event.eventId]);
    if (seen.rowCount) {
      recordStep(key, "dedupe", "skip", `event id already processed (${seen.rows[0].outcome})`, { eventId: event.eventId, firstOutcome: seen.rows[0].outcome });
      deps.log.info({ ...ctx, firstOutcome: seen.rows[0].outcome }, "duplicate skipped");
      return "duplicate";
    }
  }
  recordStep(key, "dedupe", "ok", "new event id", { eventId: event.eventId });
  await pace(msg.headers);
  const preview = guard(await readCurrent(deps.pool, event.shipmentId, false), event);
  recordStep(key, "state", preview.kind === "apply" ? "ok" : preview.kind === "stale" ? "skip" : "fail",
    preview.kind === "apply" ? `${preview.from ?? "(new)"} -> ${preview.to}` : `${preview.kind}: ${preview.reason}`, preview);
  await pace(msg.headers);

  // @learn:downstream-call
  let eta: EtaResponse | undefined;
  let attempts: AttemptRecord[] = [];
  if (preview.kind === "apply" && !NO_ETA.includes(preview.to)) {
    const r = await withRetry(deps.retry, (signal) => fetchEta(deps.downstreamUrl, { shipmentId: event.shipmentId, status: preview.to, location: event.location, traceId: event.traceId }, signal),
      (a) => {
        downstreamRequests.inc({ result: a.ok ? "ok" : "error" });
        downstreamDuration.observe(a.ms / 1000);
        if (a.backoffMs !== undefined) retriesTotal.inc();
        recordStep(key, "downstream", a.ok ? "ok" : a.backoffMs !== undefined ? "retry" : "fail",
          a.ok ? `attempt ${a.attempt} ok in ${a.ms}ms` : `attempt ${a.attempt} failed: ${a.error}${a.backoffMs !== undefined ? `, retry in ${a.backoffMs}ms` : ""}`, a);
        deps.log.info({ ...ctx, attempt: a }, a.ok ? "eta ok" : "eta attempt failed");
      });
    attempts = r.attempts;
    if (!r.ok) {
      // Temporary failure that outlived the retry budget, or a permanent 4xx: park it, move the partition on.
      const reason = r.permanent ? "downstream_rejected" : "downstream_exhausted";
      await sendToDlq(deps.pool, deps.dlq, {
        reason, detail: { attempts: r.attempts, lastError: r.lastError, wouldApply: { from: preview.from, to: preview.to } },
        shipmentId: event.shipmentId, eventId: event.eventId, seq: event.seq, source: sourceOf(msg),
      });
      failuresTotal.inc({ reason });
      recordStep(key, "dlq", "info", `${reason} after ${r.attempts.length} attempts`, { reason, lastError: r.lastError });
      deps.log.warn({ ...ctx, reason, attempts: r.attempts.length, lastError: r.lastError }, "dead-lettered");
      return "dlq";
    }
    eta = r.value;
  } else {
    recordStep(key, "downstream", "skip", preview.kind === "apply" ? `no ETA needed for ${preview.to}` : "not called: event will not be applied");
  }
  await pace(msg.headers);
  // @learn:end downstream-call
  const downstreamMs = attempts.reduce((sum, a) => sum + a.ms + (a.backoffMs ?? 0), 0);

  const client = await deps.pool.connect();
  let decision: Decision;
  try {
    await client.query("BEGIN");
    if (!(await claim(client, event, msg))) {
      await client.query("ROLLBACK");
      deps.log.info(ctx, "duplicate skipped");
      return "duplicate";
    }
    // Authoritative decision under the row lock; the preview above was only to decide whether to call downstream.
    decision = guard(await readCurrent(client, event.shipmentId, true), event);

    // @learn:write-record
    if (decision.kind === "apply") {
      await client.query(
        `INSERT INTO shipments (shipment_id, status, last_event_id, last_seq, last_location, eta_at, eta_source, last_trace_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (shipment_id) DO UPDATE SET status = $2, last_event_id = $3, last_seq = $4,
           last_location = COALESCE($5, shipments.last_location), eta_at = $6, eta_source = $7, last_trace_id = $8,
           version = shipments.version + 1, updated_at = now()`,
        [event.shipmentId, decision.to, event.eventId, event.seq, event.location ?? null, eta?.etaAt ?? null, eta?.source ?? null, event.traceId ?? null],
      );
    }
    const outcome: Outcome = decision.kind === "apply" ? "applied" : decision.kind === "stale" ? "stale" : "rejected";
    await client.query(
      `INSERT INTO shipment_history (shipment_id, event_id, seq, event_type, status_from, status_to, outcome, occurred_at,
         kafka_partition, kafka_offset, detail, trace_id, attempts, downstream_ms, kafka_ts)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, to_timestamp($15::double precision / 1000))`,
      [event.shipmentId, event.eventId, event.seq, event.type, decision.from, decision.to, outcome, event.occurredAt, msg.partition, msg.offset,
       { ...(decision.kind === "apply" ? { location: event.location ?? null, eta: eta ?? null } : { reason: decision.reason, location: event.location ?? null }),
         ...(msg.headers["x-replay-id"] ? { replayId: msg.headers["x-replay-id"] } : {}) },
       event.traceId ?? null, attempts.length, downstreamMs, Number(msg.timestamp)],
    );
    if (!sabotaged("no_dedupe")) await client.query("UPDATE processed_events SET outcome = $2 WHERE event_id = $1", [event.eventId, outcome]);
    if (decision.kind === "reject") {
      // Illegal transitions need a human: index + publish before COMMIT, so a crash re-runs both.
      await sendToDlq(client, deps.dlq, {
        reason: "illegal_transition",
        detail: { from: decision.from, to: decision.to, message: decision.reason },
        shipmentId: event.shipmentId, eventId: event.eventId, seq: event.seq, source: sourceOf(msg),
      });
      recordStep(key, "dlq", "info", `illegal_transition ${decision.from} -> ${decision.to}`, { reason: "illegal_transition" });
    }
    // @learn:end write-record
    await client.query("COMMIT");
    recordStep(key, "write", decision.kind === "apply" ? "ok" : "info", decision.kind === "apply" ? "tracking record + history committed" : `history only (${outcome})`, { outcome, decision });
    deps.log.info({ ...ctx, decision }, outcome);
    return outcome;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

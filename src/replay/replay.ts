import { createHash, randomUUID } from "node:crypto";
import type { Producer } from "kafkajs";
import type { Pool } from "../db/pool.ts";
import { decide, type Current } from "../domain/stateMachine.ts";
import { decodeEvent } from "../events/schema.ts";
import type { Status } from "../events/types.ts";

export interface ReplayFilter {
  shipmentIds?: string[] | undefined;
  reasons?: string[] | undefined;
  dlqIds?: number[] | undefined;
}

export type Verdict = "apply" | "stale" | "reject" | "skip_invalid" | "skip_already_processed";

export interface PlanItem {
  dlqId: number;
  shipmentId: string | null;
  eventId: string | null;
  seq: number | null;
  reason: string;
  verdict: Verdict;
  from: Status | null;
  to: Status | null;
  note: string;
}

export interface Plan {
  items: PlanItem[];
  groups: { shipmentId: string; dlqIds: number[] }[];
  hash: string;
}

interface DlqRow {
  id: string;
  event_id: string | null;
  shipment_id: string | null;
  seq: number | null;
  reason: string;
  original_key: string | null;
  original_value: string;
}

// @learn:replay-plan
/**
 * Dry-run. Loads open DLQ entries, groups them by shipment, orders each group by seq,
 * and simulates the state machine against the *current* tracking record without writing anything.
 */
export async function planReplay(pool: Pool, filter: ReplayFilter): Promise<Plan> {
  const rows = (await pool.query<DlqRow>(
    `SELECT id, event_id, shipment_id, seq, reason, original_key, original_value FROM dlq_entries
     WHERE status = 'open'
       AND ($1::text[] IS NULL OR shipment_id = ANY($1))
       AND ($2::text[] IS NULL OR reason = ANY($2))
       AND ($3::bigint[] IS NULL OR id = ANY($3))
     ORDER BY shipment_id NULLS LAST, seq NULLS LAST, source_partition, source_offset, id`,
    [filter.shipmentIds ?? null, filter.reasons ?? null, filter.dlqIds ?? null],
  )).rows;

  const items: PlanItem[] = [];
  const sim = new Map<string, Current | null>();
  const seenEvent = new Set<string>();
  for (const r of rows) {
    const base = { dlqId: Number(r.id), shipmentId: r.shipment_id, eventId: r.event_id, seq: r.seq, reason: r.reason };
    const d = decodeEvent(r.original_value, r.original_key);
    if (!d.ok) {
      items.push({ ...base, verdict: "skip_invalid", from: null, to: null, note: `${d.reason}: ${d.issues.join("; ")}` });
      continue;
    }
    const e = d.event;
    const done = await pool.query("SELECT outcome FROM processed_events WHERE event_id = $1", [e.eventId]);
    if (done.rowCount || seenEvent.has(e.eventId)) {
      items.push({ ...base, verdict: "skip_already_processed", from: null, to: null, note: `event already ${done.rows[0]?.outcome ?? "planned earlier in this replay"}` });
      continue;
    }
    seenEvent.add(e.eventId);
    if (!sim.has(e.shipmentId)) {
      const cur = await pool.query<{ status: Status; last_seq: number }>("SELECT status, last_seq FROM shipments WHERE shipment_id = $1", [e.shipmentId]);
      sim.set(e.shipmentId, cur.rows[0] ? { status: cur.rows[0].status, lastSeq: cur.rows[0].last_seq } : null);
    }
    const decision = decide(sim.get(e.shipmentId) ?? null, e);
    if (decision.kind === "apply") sim.set(e.shipmentId, { status: decision.to, lastSeq: e.seq });
    items.push({ ...base, verdict: decision.kind, from: decision.from, to: decision.to, note: decision.kind === "apply" ? "will be applied" : decision.reason });
  }
  const groups = new Map<string, number[]>();
  for (const i of items) {
    const k = i.shipmentId ?? "(unknown)";
    groups.set(k, [...(groups.get(k) ?? []), i.dlqId]);
  }
  const hash = createHash("sha256").update(JSON.stringify(items.map((i) => [i.dlqId, i.verdict, i.from, i.to]))).digest("hex").slice(0, 16);
  return { items, groups: [...groups].map(([shipmentId, dlqIds]) => ({ shipmentId, dlqIds })), hash };
}
// @learn:end replay-plan

export async function recordDryRun(pool: Pool, actor: string, filter: ReplayFilter, plan: Plan): Promise<string> {
  const id = `dry-${randomUUID().slice(0, 8)}`;
  await pool.query(
    `INSERT INTO replay_audit (replay_id, kind, actor, filter, plan, plan_hash, status, counts, finished_at)
     VALUES ($1, 'dry_run', $2, $3, $4, $5, 'done', $6, now())`,
    [id, actor, filter, JSON.stringify(plan.items), plan.hash, countVerdicts(plan.items)],
  );
  return id;
}

function countVerdicts(items: PlanItem[]): Record<string, number> {
  return items.reduce<Record<string, number>>((m, i) => ((m[i.verdict] = (m[i.verdict] ?? 0) + 1), m), {});
}

export class ReplayRefused extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

export interface ReplayResult {
  replayId: string;
  published: { dlqId: number; eventId: string | null; seq: number | null; partition: number; offset: string }[];
  skipped: PlanItem[];
}

const DRY_RUN_TTL_MS = 15 * 60_000;

// @learn:replay-execute
/**
 * Replays exactly the plan a human reviewed. Refuses if the dry-run is missing, too old,
 * or no longer matches (new DLQ entries, or the tracking record moved). One replay at a time.
 * Events go back onto the main topic with their original key and eventId, so ordering,
 * dedupe and the state machine all apply again. Nothing is written to the tracking record directly.
 */
export async function executeReplay(pool: Pool, producer: Producer, topic: string, dryRunId: string, actor: string): Promise<ReplayResult> {
  const lock = await pool.connect();
  try {
    const got = await lock.query<{ ok: boolean }>("SELECT pg_try_advisory_lock(73200) AS ok");
    if (!got.rows[0]?.ok) throw new ReplayRefused("replay_in_progress", "another replay is running");
    const dry = (await pool.query("SELECT * FROM replay_audit WHERE replay_id = $1 AND kind = 'dry_run'", [dryRunId])).rows[0];
    if (!dry) throw new ReplayRefused("dry_run_required", `no dry-run ${dryRunId}`);
    if (Date.now() - new Date(dry.created_at).getTime() > DRY_RUN_TTL_MS) throw new ReplayRefused("dry_run_expired", "dry-run older than 15 minutes, run it again");
    const plan = await planReplay(pool, dry.filter as ReplayFilter);
    if (plan.hash !== dry.plan_hash) throw new ReplayRefused("plan_changed", `plan changed since ${dryRunId} (${dry.plan_hash} -> ${plan.hash}), run the dry-run again`);

    const replayId = `rpl-${randomUUID().slice(0, 8)}`;
    await pool.query(
      `INSERT INTO replay_audit (replay_id, kind, actor, filter, plan, plan_hash, based_on, status)
       VALUES ($1, 'execute', $2, $3, $4, $5, $6, 'running')`,
      [replayId, actor, dry.filter, JSON.stringify(plan.items), plan.hash, dryRunId],
    );
    const published: ReplayResult["published"] = [];
    const skipped: PlanItem[] = [];
    for (const item of plan.items) {
      if (item.verdict !== "apply" && item.verdict !== "stale") {
        skipped.push(item);
        continue;
      }
      const row = (await pool.query<DlqRow>("SELECT * FROM dlq_entries WHERE id = $1", [item.dlqId])).rows[0]!;
      // Sequential awaits: within a shipment the next send starts only after the previous one is acked,
      // so the partition receives them in plan (seq) order.
      const [meta] = await producer.send({
        topic,
        messages: [{ key: row.shipment_id ?? row.original_key, value: row.original_value, headers: { "x-replay-id": replayId, "x-dlq-entry-id": String(item.dlqId) } }],
      });
      await pool.query("UPDATE dlq_entries SET status = 'replayed', replay_id = $2, resolved_at = now() WHERE id = $1 AND status = 'open'", [item.dlqId, replayId]);
      published.push({ dlqId: item.dlqId, eventId: item.eventId, seq: item.seq, partition: meta?.partition ?? -1, offset: meta?.baseOffset ?? "-1" });
    }
    await pool.query("UPDATE replay_audit SET status = 'done', finished_at = now(), counts = $2 WHERE replay_id = $1", [replayId, { published: published.length, skipped: skipped.length }]);
    return { replayId, published, skipped };
  } finally {
    await lock.query("SELECT pg_advisory_unlock(73200)").catch(() => {});
    lock.release();
  }
}
// @learn:end replay-execute

/** For entries a replay cannot fix (bad payloads, real data errors): close them, with a reason, on the record. */
export async function discardEntries(pool: Pool, dlqIds: number[], actor: string, note: string): Promise<{ replayId: string; discarded: number }> {
  if (!note.trim()) throw new ReplayRefused("note_required", "say why you are discarding");
  const replayId = `dsc-${randomUUID().slice(0, 8)}`;
  const r = await pool.query("UPDATE dlq_entries SET status = 'discarded', replay_id = $2, resolved_at = now() WHERE id = ANY($1) AND status = 'open'", [dlqIds, replayId]);
  await pool.query(
    `INSERT INTO replay_audit (replay_id, kind, actor, filter, plan_hash, status, counts, note, finished_at)
     VALUES ($1, 'discard', $2, $3, '-', 'done', $4, $5, now())`,
    [replayId, actor, { dlqIds }, { discarded: r.rowCount ?? 0 }, note],
  );
  return { replayId, discarded: r.rowCount ?? 0 };
}

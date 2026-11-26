import { timingSafeEqual } from "node:crypto";
import Fastify, { type FastifyInstance } from "fastify";
import type { Producer } from "kafkajs";
import type { Pool } from "../db/pool.ts";
import { registry } from "../obs/metrics.ts";
import { registerLearn } from "./learn.ts";
import type { PartitionLag } from "../obs/lag.ts";
import { discardEntries, executeReplay, planReplay, recordDryRun, ReplayRefused, type ReplayFilter } from "../replay/replay.ts";

export interface AdminDeps {
  pool: Pool;
  producer: Producer;
  eventsTopic: string;
  adminToken: string;
  downstreamUrl: string;
  lag: () => { at: string | null; partitions: PartitionLag[] };
  learnMode?: boolean;
}

function tokenOk(header: string | undefined, expected: string): boolean {
  const got = Buffer.from(header?.replace(/^Bearer\s+/i, "") ?? "");
  const want = Buffer.from(expected);
  return got.length === want.length && timingSafeEqual(got, want);
}

/** Port 8900: health, metrics, and the operator API (DLQ, dry-run, replay, discard). /admin/* needs the bearer token. */
export function buildAdmin(deps: AdminDeps): FastifyInstance {
  const app = Fastify({ logger: false });
  app.addHook("onRequest", async (req, reply) => {
    if (req.url.startsWith("/admin/") && !tokenOk(req.headers.authorization, deps.adminToken)) {
      return reply.code(401).send({ error: "unauthorized" });
    }
  });
  app.setErrorHandler((err, _req, reply) => {
    if (err instanceof ReplayRefused) return reply.code(409).send({ error: err.code, message: err.message });
    return reply.code(500).send({ error: "internal", message: (err as Error).message });
  });

  app.get("/healthz", async () => ({ ok: true }));
  if (deps.learnMode) registerLearn(app, deps);
  app.get("/metrics", async (_req, reply) => reply.header("content-type", registry.contentType).send(await registry.metrics()));
  app.get("/admin/lag", async () => deps.lag());

  // Fault switches live on the fake dependency; proxied here so the ops page talks to one origin.
  app.get("/admin/faults", async () => (await fetch(`${deps.downstreamUrl}/faults`)).json());
  app.put<{ Body: Record<string, unknown> }>("/admin/faults", async (req) =>
    (await fetch(`${deps.downstreamUrl}/faults`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(req.body ?? {}) })).json());
  app.post("/admin/faults/reset", async () => (await fetch(`${deps.downstreamUrl}/faults/reset`, { method: "POST" })).json());

  app.get<{ Querystring: { status?: string; shipmentId?: string } }>("/admin/dlq", async (req) => {
    const r = await deps.pool.query(
      `SELECT id, event_id, shipment_id, seq, reason, detail, source_partition, source_offset, dlq_partition, dlq_offset, status, created_at, resolved_at, replay_id
       FROM dlq_entries WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR shipment_id = $2) ORDER BY id DESC LIMIT 200`,
      [req.query.status ?? null, req.query.shipmentId ?? null],
    );
    return { entries: r.rows };
  });

  app.post<{ Body: ReplayFilter & { actor?: string } }>("/admin/replay/plan", async (req) => {
    const { actor = "ops-ui", ...filter } = req.body ?? {};
    const plan = await planReplay(deps.pool, filter);
    const dryRunId = await recordDryRun(deps.pool, actor, filter, plan);
    return { dryRunId, plan };
  });

  app.post<{ Body: { dryRunId?: string; actor?: string } }>("/admin/replay/execute", async (req, reply) => {
    if (!req.body?.dryRunId) return reply.code(400).send({ error: "dry_run_required", message: "run POST /admin/replay/plan first and pass its dryRunId" });
    return executeReplay(deps.pool, deps.producer, deps.eventsTopic, req.body.dryRunId, req.body.actor ?? "ops-ui");
  });

  app.post<{ Body: { dlqIds?: number[]; note?: string; actor?: string } }>("/admin/dlq/discard", async (req) => {
    return discardEntries(deps.pool, req.body?.dlqIds ?? [], req.body?.actor ?? "ops-ui", req.body?.note ?? "");
  });

  app.get("/admin/replay/audit", async () => {
    const r = await deps.pool.query("SELECT replay_id, kind, actor, filter, plan_hash, based_on, status, counts, note, created_at, finished_at FROM replay_audit ORDER BY created_at DESC LIMIT 50");
    return { audit: r.rows };
  });
  return app;
}

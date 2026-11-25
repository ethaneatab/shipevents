import Fastify, { type FastifyInstance } from "fastify";

/**
 * A fake ETA / address-normalisation dependency with switchable faults, so the processor's
 * retry and DLQ paths can be exercised on purpose.
 */
export interface FaultConfig {
  mode: "ok" | "error" | "timeout" | "slow" | "flaky";
  /** for "slow": added latency; for "timeout": how long to hang */
  latencyMs: number;
  /** for "flaky": share of calls that fail with 503 */
  errorRate: number;
  /** one-shot failures, consumed first; optionally only for one shipment */
  failNext: { shipmentId?: string; count: number; status: number }[];
}

const HOURS_TO_GO: Record<string, number> = { CREATED: 72, PICKED_UP: 60, IN_TRANSIT: 30, OUT_FOR_DELIVERY: 4, EXCEPTION: 96 };

export function defaultFaults(): FaultConfig {
  return { mode: "ok", latencyMs: 0, errorRate: 0, failNext: [] };
}

export function buildDownstream(): { app: FastifyInstance; faults: FaultConfig; stats: Record<string, number> } {
  const app = Fastify({ logger: false });
  const faults = defaultFaults();
  const stats: Record<string, number> = {};
  const count = (k: string) => (stats[k] = (stats[k] ?? 0) + 1);

  app.get("/healthz", async () => ({ ok: true }));
  app.get("/faults", async () => faults);
  app.put<{ Body: Partial<FaultConfig> }>("/faults", async (req) => {
    const b = req.body ?? {};
    if (b.mode) faults.mode = b.mode;
    if (typeof b.latencyMs === "number") faults.latencyMs = b.latencyMs;
    if (typeof b.errorRate === "number") faults.errorRate = b.errorRate;
    if (Array.isArray(b.failNext)) faults.failNext = b.failNext;
    return faults;
  });
  app.post<{ Body: { count?: number; status?: number; shipmentId?: string } }>("/faults/fail-next", async (req) => {
    const entry: FaultConfig["failNext"][number] = { count: req.body?.count ?? 1, status: req.body?.status ?? 503 };
    if (req.body?.shipmentId) entry.shipmentId = req.body.shipmentId;
    faults.failNext.push(entry);
    return faults;
  });
  app.post("/faults/reset", async () => Object.assign(faults, defaultFaults()));
  app.get("/stats", async () => stats);

  app.post<{ Body: { shipmentId?: string; status?: string; location?: string } }>("/v1/eta", async (req, reply) => {
    const { shipmentId, status } = req.body ?? {};
    if (!shipmentId || !status) {
      count("400");
      return reply.code(400).send({ error: "shipmentId and status are required" });
    }
    const idx = faults.failNext.findIndex((f) => f.count > 0 && (!f.shipmentId || f.shipmentId === shipmentId));
    if (idx >= 0) {
      const f = faults.failNext[idx]!;
      f.count -= 1;
      if (f.count <= 0) faults.failNext.splice(idx, 1);
      count(String(f.status));
      return reply.code(f.status).send({ error: "injected failure" });
    }
    if (faults.mode === "error" || (faults.mode === "flaky" && Math.random() < faults.errorRate)) {
      count("503");
      return reply.code(503).send({ error: "eta service unavailable" });
    }
    if (faults.mode === "timeout") await new Promise((r) => setTimeout(r, faults.latencyMs || 5000));
    if (faults.mode === "slow") await new Promise((r) => setTimeout(r, faults.latencyMs));
    count("200");
    const hours = HOURS_TO_GO[status] ?? 24;
    return { etaAt: new Date(Date.now() + hours * 3600_000).toISOString(), source: `eta-model-v3/${status.toLowerCase()}` };
  });
  return { app, faults, stats };
}

import Fastify, { type FastifyInstance } from "fastify";
import type { Pool } from "../db/pool.ts";

/** Customer-facing read API. It only reads the tracking record; it never touches Kafka. */
export function buildApi(pool: Pool): FastifyInstance {
  const app = Fastify({ logger: false });
  app.get("/healthz", async () => ({ ok: true }));
  // @learn:api-shipment
  app.get<{ Params: { id: string } }>("/v1/shipments/:id", async (req, reply) => {
    const s = await pool.query("SELECT * FROM shipments WHERE shipment_id = $1", [req.params.id]);
    if (s.rowCount === 0) return reply.code(404).send({ error: "not_found" });
    const h = await pool.query("SELECT * FROM shipment_history WHERE shipment_id = $1 ORDER BY seq, id", [req.params.id]);
    return { shipment: s.rows[0], history: h.rows };
  });
  // @learn:end api-shipment

  // @learn:latency-breakdown
  // "My update showed up late": for each event, where did the time go? Queue wait vs. downstream vs. the rest.
  app.get<{ Params: { id: string } }>("/v1/shipments/:id/latency", async (req) => {
    const r = await pool.query(
      `SELECT event_id, seq, event_type, outcome, kafka_partition, kafka_offset, trace_id, attempts, downstream_ms,
              round(extract(epoch FROM (processed_at - kafka_ts)) * 1000)::int AS end_to_end_ms
       FROM shipment_history WHERE shipment_id = $1 AND kafka_ts IS NOT NULL ORDER BY id`,
      [req.params.id],
    );
    return {
      shipmentId: req.params.id,
      events: r.rows.map((x) => ({
        ...x,
        queue_and_db_ms: Math.max(0, (x.end_to_end_ms ?? 0) - (x.downstream_ms ?? 0)),
        slowest: (x.downstream_ms ?? 0) > (x.end_to_end_ms ?? 0) / 2 ? "downstream" : "queue_or_db",
      })),
    };
  });
  // @learn:end latency-breakdown
  return app;
}

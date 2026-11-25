import Fastify, { type FastifyInstance } from "fastify";
import type { Pool } from "../db/pool.ts";

/** Customer-facing read API. It only reads the tracking record; it never touches Kafka. */
export function buildApi(pool: Pool): FastifyInstance {
  const app = Fastify({ logger: false });
  app.get("/healthz", async () => ({ ok: true }));
  app.get<{ Params: { id: string } }>("/v1/shipments/:id", async (req, reply) => {
    const s = await pool.query("SELECT * FROM shipments WHERE shipment_id = $1", [req.params.id]);
    if (s.rowCount === 0) return reply.code(404).send({ error: "not_found" });
    const h = await pool.query("SELECT * FROM shipment_history WHERE shipment_id = $1 ORDER BY seq, id", [req.params.id]);
    return { shipment: s.rows[0], history: h.rows };
  });
  return app;
}

import http from "node:http";
import { config } from "../config.ts";
import { createPool } from "../db/pool.ts";
import { migrate } from "../db/migrate.ts";
import { log } from "../log.ts";
import { startProcessor } from "./consumer.ts";

const pool = createPool();
await migrate(pool);
const proc = await startProcessor({ pool, log }, { brokers: config.kafkaBrokers, topic: config.eventsTopic, dlqTopic: config.dlqTopic, groupId: config.consumerGroup });

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(404).end();
});
server.listen(config.processorPort, "127.0.0.1", () => log.info({ port: config.processorPort }, "processor listening"));

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    server.close();
    await proc.stop();
    await pool.end();
    process.exit(0);
  });
}

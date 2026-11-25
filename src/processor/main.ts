import { config } from "../config.ts";
import { createPool } from "../db/pool.ts";
import { migrate } from "../db/migrate.ts";
import { log } from "../log.ts";
import { buildAdmin } from "./admin.ts";
import { startProcessor } from "./consumer.ts";
import { policyFromEnv } from "./retry.ts";

const pool = createPool();
await migrate(pool);
const proc = await startProcessor(
  { pool, log },
  { brokers: config.kafkaBrokers, topic: config.eventsTopic, dlqTopic: config.dlqTopic, groupId: config.consumerGroup, downstreamUrl: config.downstreamUrl, retry: policyFromEnv() },
);
const admin = buildAdmin({ pool, producer: proc.producer, eventsTopic: config.eventsTopic, adminToken: config.adminToken });
const address = await admin.listen({ port: config.processorPort, host: "127.0.0.1" });
log.info({ port: config.processorPort, address }, "processor listening");

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    await admin.close();
    await proc.stop();
    await pool.end();
    process.exit(0);
  });
}

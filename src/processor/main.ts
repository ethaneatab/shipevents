import { config } from "../config.ts";
import { createPool } from "../db/pool.ts";
import { migrate } from "../db/migrate.ts";
import { createKafka } from "../kafka/client.ts";
import { log } from "../log.ts";
import { computeLag, type PartitionLag } from "../obs/lag.ts";
import { consumerLag, dlqOpen } from "../obs/metrics.ts";
import { buildAdmin } from "./admin.ts";
import { startProcessor } from "./consumer.ts";
import { policyFromEnv } from "./retry.ts";

const pool = createPool();
await migrate(pool);
const proc = await startProcessor(
  { pool, log },
  { brokers: config.kafkaBrokers, topic: config.eventsTopic, dlqTopic: config.dlqTopic, groupId: config.consumerGroup, downstreamUrl: config.downstreamUrl, retry: policyFromEnv() },
);

// Background job: queue depth and open DLQ entries every 2s, for /metrics and the ops page.
const kafkaAdmin = createKafka("shipevents-lag").admin();
await kafkaAdmin.connect();
let lagSnapshot: { at: string | null; partitions: PartitionLag[] } = { at: null, partitions: [] };
const poll = setInterval(async () => {
  try {
    const partitions = await computeLag(kafkaAdmin, config.eventsTopic, config.consumerGroup);
    for (const p of partitions) consumerLag.set({ partition: String(p.partition) }, p.lag);
    lagSnapshot = { at: new Date().toISOString(), partitions };
    const open = await pool.query<{ reason: string; n: number }>("SELECT reason, count(*)::int AS n FROM dlq_entries WHERE status = 'open' GROUP BY reason");
    dlqOpen.reset();
    for (const r of open.rows) dlqOpen.set({ reason: r.reason }, r.n);
  } catch (e) {
    log.warn({ err: (e as Error).message }, "lag poll failed");
  }
}, 2000);

const admin = buildAdmin({ pool, producer: proc.producer, eventsTopic: config.eventsTopic, adminToken: config.adminToken, downstreamUrl: config.downstreamUrl, lag: () => lagSnapshot, learnMode: process.env.LEARN_MODE === "1" });
const address = await admin.listen({ port: config.processorPort, host: process.env.PROCESSOR_HOST ?? "127.0.0.1" });
log.info({ port: config.processorPort, address }, "processor listening");

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    clearInterval(poll);
    await admin.close();
    await kafkaAdmin.disconnect();
    await proc.stop();
    await pool.end();
    process.exit(0);
  });
}

// All runtime configuration comes from the environment. Defaults are local-dev only.
import repo from "../repo.config.json" with { type: "json" };

export const DEV_ADMIN_TOKEN = "dev-admin-token-change-me";

function env(name: string, fallback: string): string {
  const v = process.env[name];
  return v === undefined || v === "" ? fallback : v;
}

function int(name: string, fallback: number): number {
  const n = Number.parseInt(env(name, String(fallback)), 10);
  return Number.isFinite(n) ? n : fallback;
}

export const config = {
  repoUrl: env("REPO_URL", repo.repoUrl),
  kafkaBrokers: env("KAFKA_BROKERS", "localhost:19210").split(","),
  databaseUrl: env("DATABASE_URL", "postgres://shipevents:shipevents-dev@localhost:54310/shipevents"),
  eventsTopic: env("EVENTS_TOPIC", "shipments.events"),
  dlqTopic: env("DLQ_TOPIC", "shipments.dlq"),
  consumerGroup: env("CONSUMER_GROUP", "shipevents-processor"),
  partitions: int("TOPIC_PARTITIONS", 3),
  processorPort: int("PROCESSOR_PORT", 8900),
  apiPort: int("API_PORT", 8901),
  downstreamPort: int("DOWNSTREAM_PORT", 8902),
  downstreamUrl: env("DOWNSTREAM_URL", "http://localhost:8902"),
  logLevel: env("LOG_LEVEL", "info"),
  /** Bearer token for /admin/*. The default is a dev placeholder; production refuses to start with it. */
  adminToken: env("ADMIN_TOKEN", DEV_ADMIN_TOKEN),
};

export type Config = typeof config;

if (process.env.NODE_ENV === "production" && config.adminToken === DEV_ADMIN_TOKEN) {
  throw new Error("ADMIN_TOKEN must be set in production");
}

import { randomUUID } from "node:crypto";

export const TEST_DB_URL = process.env.TEST_DATABASE_URL ?? "postgres://shipevents:shipevents-dev@localhost:54310/shipevents_test";
export const TEST_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:19210").split(",");

/** Every test file gets its own topics and consumer group, so files can run in parallel. */
export function uniqueNames(label: string): { topic: string; dlqTopic: string; group: string } {
  const id = `${label}-${randomUUID().slice(0, 8)}`;
  return { topic: `test.${id}.events`, dlqTopic: `test.${id}.dlq`, group: `test-${id}` };
}

export async function waitFor<T>(fn: () => Promise<T | undefined | null | false>, timeoutMs = 20_000, everyMs = 100): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (e) {
      last = e;
    }
    await new Promise((r) => setTimeout(r, everyMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms${last ? `: ${String(last)}` : ""}`);
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

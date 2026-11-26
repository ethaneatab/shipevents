import type { Consumer, IHeaders, Producer } from "kafkajs";
import { createKafka, createProducer, ensureTopics } from "../kafka/client.ts";
import type { Pool } from "../db/pool.ts";
import type { Logger } from "../log.ts";
import { handleMessage, type Deps, type Outcome, type RawMessage } from "./handle.ts";
import { crashPoint, sabotaged } from "../lab/sabotage.ts";
import type { RetryPolicy } from "./retry.ts";
import { eventsTotal, failuresTotal, handleDuration, processingLag } from "../obs/metrics.ts";

export interface ProcessorOptions {
  brokers: string[];
  topic: string;
  dlqTopic: string;
  groupId: string;
  downstreamUrl: string;
  retry: RetryPolicy;
}

export interface RunningProcessor {
  consumer: Consumer;
  producer: Producer;
  stop(): Promise<void>;
}

export function headersToStrings(h: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(h ?? {})) {
    if (v === undefined) continue;
    out[k] = Array.isArray(v) ? v.map(String).join(",") : String(v);
  }
  return out;
}

export async function startProcessor(base: { pool: Pool; log: Logger }, opts: ProcessorOptions): Promise<RunningProcessor> {
  const kafka = createKafka("shipevents-processor", opts.brokers);
  await ensureTopics(kafka, [opts.topic, opts.dlqTopic]);
  const producer = createProducer(kafka);
  await producer.connect();
  const deps: Deps = { ...base, dlq: { producer, topic: opts.dlqTopic }, downstreamUrl: opts.downstreamUrl, retry: opts.retry };
  const sessionTimeout = Number(process.env.KAFKA_SESSION_TIMEOUT_MS ?? 10000);
  const consumer = kafka.consumer({ groupId: opts.groupId, sessionTimeout, heartbeatInterval: 1000 });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message, partition }) => {
      const raw: RawMessage = {
        topic, partition, offset: message.offset, timestamp: message.timestamp,
        key: message.key ? message.key.toString() : null, value: message.value, headers: headersToStrings(message.headers),
      };
      const commit = () => consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      const started = performance.now();
      const outcome = sabotaged("commit_before_write") ? await commitThenProcessLegacy(deps, raw, commit) : await processThenCommit(deps, raw, commit);
      eventsTotal.inc({ outcome });
      handleDuration.observe({ outcome }, (performance.now() - started) / 1000);
      if (outcome === "applied" || outcome === "stale") processingLag.observe(Math.max(0, Date.now() - Number(message.timestamp)) / 1000);
      if (outcome === "rejected") failuresTotal.inc({ reason: "illegal_transition" });
    },
  });
  return {
    consumer,
    producer,
    async stop() {
      await consumer.disconnect();
      await producer.disconnect();
    },
  };
}

// @learn:commit-order
/** At-least-once: the database write is durable before Kafka hears "done". A crash in between means a redelivery, which dedupe absorbs. */
export async function processThenCommit(deps: Deps, raw: RawMessage, commit: () => Promise<void>): Promise<Outcome> {
  const outcome = await handleMessage(deps, raw);
  crashPoint(peekEventId(raw.value));
  await commit();
  return outcome;
}
// @learn:end commit-order

// @learn:commit-legacy
/** LAB_SABOTAGE=commit_before_write. At-most-once: a crash after the commit loses the event for good. */
export async function commitThenProcessLegacy(deps: Deps, raw: RawMessage, commit: () => Promise<void>): Promise<Outcome> {
  await commit();
  crashPoint(peekEventId(raw.value));
  return handleMessage(deps, raw);
}
// @learn:end commit-legacy

function peekEventId(value: Buffer | null): string | undefined {
  try {
    const v = JSON.parse(String(value)) as { eventId?: unknown };
    return typeof v.eventId === "string" ? v.eventId : undefined;
  } catch {
    return undefined;
  }
}

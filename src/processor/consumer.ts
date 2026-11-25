import type { Consumer, IHeaders, Producer } from "kafkajs";
import { createKafka, createProducer, ensureTopics } from "../kafka/client.ts";
import type { Pool } from "../db/pool.ts";
import type { Logger } from "../log.ts";
import { handleMessage, type RawMessage } from "./handle.ts";

export interface ProcessorOptions {
  brokers: string[];
  topic: string;
  dlqTopic: string;
  groupId: string;
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
  const deps = { ...base, dlq: { producer, topic: opts.dlqTopic } };
  const consumer = kafka.consumer({ groupId: opts.groupId, sessionTimeout: 10000, heartbeatInterval: 1000 });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, message, partition }) => {
      const raw: RawMessage = {
        topic, partition, offset: message.offset, timestamp: message.timestamp,
        key: message.key ? message.key.toString() : null, value: message.value, headers: headersToStrings(message.headers),
      };
      await handleMessage(deps, raw);
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

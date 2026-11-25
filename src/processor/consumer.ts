import type { Consumer } from "kafkajs";
import { createKafka, ensureTopics } from "../kafka/client.ts";
import { handleMessage, type Deps } from "./handle.ts";

export interface ProcessorOptions {
  brokers: string[];
  topic: string;
  groupId: string;
}

export interface RunningProcessor {
  consumer: Consumer;
  stop(): Promise<void>;
}

export async function startProcessor(deps: Deps, opts: ProcessorOptions): Promise<RunningProcessor> {
  const kafka = createKafka("shipevents-processor", opts.brokers);
  await ensureTopics(kafka, [opts.topic]);
  const consumer = kafka.consumer({ groupId: opts.groupId, sessionTimeout: 10000, heartbeatInterval: 1000 });
  await consumer.connect();
  await consumer.subscribe({ topic: opts.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      await handleMessage(deps, { value: message.value, partition, offset: message.offset, timestamp: message.timestamp });
    },
  });
  return {
    consumer,
    async stop() {
      await consumer.disconnect();
    },
  };
}

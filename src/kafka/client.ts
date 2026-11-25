import kafkajs from "kafkajs";
import { config } from "../config.ts";

const { Kafka, logLevel, Partitioners } = kafkajs;
export type { Consumer, Producer, Admin, KafkaMessage, EachMessagePayload } from "kafkajs";

export function createKafka(clientId: string, brokers = config.kafkaBrokers): kafkajs.Kafka {
  return new Kafka({ clientId, brokers, logLevel: logLevel.WARN, retry: { retries: 8, initialRetryTime: 200 } });
}

/** Keyed by shipmentId with the Java-compatible murmur2 partitioner, so one shipment = one partition = one order. */
export function createProducer(kafka: kafkajs.Kafka): kafkajs.Producer {
  return kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, allowAutoTopicCreation: false });
}

export async function ensureTopics(kafka: kafkajs.Kafka, topics: string[], numPartitions = config.partitions): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter((t) => !existing.has(t));
    if (missing.length > 0) {
      await admin.createTopics({ waitForLeaders: true, topics: missing.map((topic) => ({ topic, numPartitions, replicationFactor: 1 })) });
    }
  } finally {
    await admin.disconnect();
  }
}

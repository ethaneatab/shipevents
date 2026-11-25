import type { Producer } from "kafkajs";
import type { Pool } from "../db/pool.ts";

export interface DlqSink {
  producer: Producer;
  topic: string;
}

export interface DlqRecord {
  reason: string;
  detail: Record<string, unknown>;
  shipmentId?: string | undefined;
  eventId?: string | undefined;
  seq?: number | undefined;
  source: { topic: string; partition: number; offset: string; key: string | null; value: string; headers: Record<string, string> };
}

// @learn:dlq-send
/**
 * Publish to the DLQ topic first (durable, other teams can subscribe), then index it in Postgres.
 * If we crash in between, the source offset is not committed, the record is redelivered, and the
 * unique (topic, partition, offset) key keeps the index at one row. The topic may hold a duplicate;
 * replay dedupes by eventId, so that is harmless.
 */
export async function sendToDlq(pool: Pool, sink: DlqSink, rec: DlqRecord): Promise<{ partition: number; offset: string }> {
  const body = { ...rec, deadLetteredAt: new Date().toISOString() };
  const [meta] = await sink.producer.send({
    topic: sink.topic,
    messages: [{ key: rec.shipmentId ?? rec.source.key ?? "unknown", value: JSON.stringify(body), headers: { "x-dlq-reason": rec.reason } }],
  });
  const partition = meta?.partition ?? -1;
  const offset = meta?.baseOffset ?? "-1";
  await pool.query(
    `INSERT INTO dlq_entries (event_id, shipment_id, seq, reason, detail, original_key, original_value,
       source_topic, source_partition, source_offset, dlq_partition, dlq_offset)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
     ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`,
    [rec.eventId ?? null, rec.shipmentId ?? null, rec.seq ?? null, rec.reason, rec.detail, rec.source.key, rec.source.value,
     rec.source.topic, rec.source.partition, rec.source.offset, partition, offset],
  );
  return { partition, offset };
}
// @learn:end dlq-send

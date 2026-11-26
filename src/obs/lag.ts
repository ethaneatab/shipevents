import type { Admin } from "kafkajs";

export interface PartitionLag {
  partition: number;
  high: number;
  committed: number;
  lag: number;
}

// @learn:consumer-lag
/** Queue depth per partition = log end offset - committed offset of our group. Lag is what a late customer update looks like from Kafka's side. */
export async function computeLag(admin: Admin, topic: string, groupId: string): Promise<PartitionLag[]> {
  const [ends, committed] = await Promise.all([admin.fetchTopicOffsets(topic), admin.fetchOffsets({ groupId, topics: [topic] })]);
  const byPartition = new Map((committed[0]?.partitions ?? []).map((p) => [p.partition, Number(p.offset)]));
  return ends
    .map((e) => {
      const high = Number(e.high);
      const low = Number(e.low);
      const c = byPartition.get(e.partition) ?? -1;
      const from = c < 0 ? low : c; // -1 = nothing committed yet
      return { partition: e.partition, high, committed: c, lag: Math.max(0, high - from) };
    })
    .sort((a, b) => a.partition - b.partition);
}
// @learn:end consumer-lag

import type { Producer } from "kafkajs";
import type { ShipmentEvent } from "../events/types.ts";

export interface ProducedRecord {
  eventId: string;
  partition: number;
  offset: string;
}

/** Sends events one by one, keyed by shipmentId, and reports where each landed. */
export async function sendEvents(producer: Producer, topic: string, events: (ShipmentEvent | { raw: string; shipmentId: string })[], headers: Record<string, string> = {}): Promise<ProducedRecord[]> {
  const out: ProducedRecord[] = [];
  for (const e of events) {
    const value = "raw" in e ? e.raw : JSON.stringify(e);
    // @learn:produce-key
    // key = shipmentId: the default murmur2 partitioner sends one shipment to one partition, which keeps its order.
    const [meta] = await producer.send({ topic, messages: [{ key: e.shipmentId, value, headers }] });
    // @learn:end produce-key
    out.push({ eventId: "raw" in e ? "(raw)" : e.eventId, partition: meta?.partition ?? -1, offset: meta?.baseOffset ?? "-1" });
  }
  return out;
}

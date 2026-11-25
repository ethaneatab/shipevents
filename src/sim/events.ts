import { randomUUID } from "node:crypto";
import type { EventType, ShipmentEvent } from "../events/types.ts";

const HAPPY_PATH: { type: EventType; location: string }[] = [
  { type: "SHIPMENT_CREATED", location: "Oakland, CA" },
  { type: "PICKED_UP", location: "Oakland, CA" },
  { type: "HUB_SCAN", location: "Sacramento hub" },
  { type: "HUB_SCAN", location: "Reno hub" },
  { type: "OUT_FOR_DELIVERY", location: "Salt Lake City, UT" },
  { type: "DELIVERED", location: "Salt Lake City, UT" },
];

export function newShipmentId(prefix = "SHP"): string {
  return `${prefix}-${randomUUID().slice(0, 8).toUpperCase()}`;
}

export function makeEvent(shipmentId: string, seq: number, type: EventType, extra: Partial<ShipmentEvent> = {}): ShipmentEvent {
  const base = Date.parse("2026-11-20T08:00:00Z");
  return {
    eventId: randomUUID(),
    schemaVersion: 1,
    shipmentId,
    seq,
    type,
    occurredAt: new Date(base + seq * 37 * 60_000).toISOString(),
    traceId: randomUUID().replaceAll("-", ""),
    ...extra,
  };
}

/** A full journey: created, picked up, two hub scans, out for delivery, delivered. seq = 1..6. */
export function journey(shipmentId: string, steps = HAPPY_PATH.length): ShipmentEvent[] {
  return HAPPY_PATH.slice(0, steps).map((s, i) => makeEvent(shipmentId, i + 1, s.type, { location: s.location }));
}

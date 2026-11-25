export const EVENT_TYPES = [
  "SHIPMENT_CREATED",
  "PICKED_UP",
  "HUB_SCAN",
  "OUT_FOR_DELIVERY",
  "DELIVERED",
  "DELIVERY_EXCEPTION",
  "RETURNED_TO_SENDER",
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export const STATUSES = ["CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION", "RETURNED"] as const;
export type Status = (typeof STATUSES)[number];

/** Which status an event moves a shipment to. */
export const TARGET_STATUS: Record<EventType, Status> = {
  SHIPMENT_CREATED: "CREATED",
  PICKED_UP: "PICKED_UP",
  HUB_SCAN: "IN_TRANSIT",
  OUT_FOR_DELIVERY: "OUT_FOR_DELIVERY",
  DELIVERED: "DELIVERED",
  DELIVERY_EXCEPTION: "EXCEPTION",
  RETURNED_TO_SENDER: "RETURNED",
};

export interface ShipmentEvent {
  eventId: string;
  schemaVersion: number;
  shipmentId: string;
  /** Per-shipment sequence number assigned by the carrier feed. Higher = newer. */
  seq: number;
  type: EventType;
  occurredAt: string;
  location?: string | undefined;
  traceId?: string | undefined;
}

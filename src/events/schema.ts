import { z } from "zod";
import { EVENT_TYPES, type ShipmentEvent } from "./types.ts";

// Wire format v1: flat `location` string.
const V1 = z.object({
  eventId: z.uuid(),
  schemaVersion: z.literal(1),
  shipmentId: z.string().regex(/^[A-Z0-9][A-Z0-9-]{2,39}$/, "shipmentId must be 3-40 chars of A-Z, 0-9, -"),
  seq: z.number().int().positive(),
  type: z.enum(EVENT_TYPES),
  occurredAt: z.iso.datetime({ offset: true }),
  location: z.string().min(1).max(120).optional(),
  traceId: z.string().regex(/^[0-9a-f]{16,32}$/).optional(),
});

// Wire format v2: the carrier feed moved location into a facility object.
const V2 = V1.omit({ schemaVersion: true, location: true }).extend({
  schemaVersion: z.literal(2),
  facility: z.object({ code: z.string().min(2).max(16), city: z.string().min(1).max(80) }).optional(),
});

export const SUPPORTED_VERSIONS = [1, 2] as const;

export type DecodeFailure = "invalid_json" | "unsupported_version" | "schema_invalid" | "key_mismatch";
export type Decoded = { ok: true; event: ShipmentEvent } | { ok: false; reason: DecodeFailure; issues: string[]; shipmentId?: string; eventId?: string };

// @learn:schema-decode
/**
 * Bytes -> validated internal event, or a reason it can never be processed.
 * Nothing here throws: a poison message must become a DLQ entry, not a crash loop.
 */
export function decodeEvent(value: Buffer | string | null, key: string | null): Decoded {
  let body: unknown;
  try {
    body = JSON.parse(String(value ?? ""));
  } catch (e) {
    return { ok: false, reason: "invalid_json", issues: [(e as Error).message] };
  }
  const hint = peekIds(body);
  const version = typeof body === "object" && body !== null ? (body as { schemaVersion?: unknown }).schemaVersion : undefined;
  if (!SUPPORTED_VERSIONS.includes(version as 1 | 2)) {
    return { ok: false, reason: "unsupported_version", issues: [`schemaVersion ${JSON.stringify(version)} not in [${SUPPORTED_VERSIONS.join(", ")}]`], ...hint };
  }
  const parsed = version === 1 ? V1.safeParse(body) : V2.safeParse(body);
  if (!parsed.success) {
    return { ok: false, reason: "schema_invalid", issues: parsed.error.issues.map((i) => `${i.path.join(".") || "(root)"}: ${i.message}`), ...hint };
  }
  const d = parsed.data;
  // The key decides the partition, and the partition decides the order. A mismatched key breaks ordering silently.
  if (key !== null && key !== d.shipmentId) {
    return { ok: false, reason: "key_mismatch", issues: [`kafka key ${key} != shipmentId ${d.shipmentId}`], ...hint };
  }
  const location = d.schemaVersion === 2 ? (d.facility ? `${d.facility.city} (${d.facility.code})` : undefined) : d.location;
  return {
    ok: true,
    event: { eventId: d.eventId, schemaVersion: d.schemaVersion, shipmentId: d.shipmentId, seq: d.seq, type: d.type, occurredAt: d.occurredAt, location, traceId: d.traceId },
  };
}
// @learn:end schema-decode

function peekIds(body: unknown): { shipmentId?: string; eventId?: string } {
  if (typeof body !== "object" || body === null) return {};
  const b = body as Record<string, unknown>;
  const out: { shipmentId?: string; eventId?: string } = {};
  if (typeof b.shipmentId === "string") out.shipmentId = b.shipmentId.slice(0, 64);
  if (typeof b.eventId === "string") out.eventId = b.eventId.slice(0, 64);
  return out;
}

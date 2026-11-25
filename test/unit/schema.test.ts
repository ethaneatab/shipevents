import { describe, expect, it } from "vitest";
import { decodeEvent } from "../../src/events/schema.ts";
import { makeEvent } from "../../src/sim/events.ts";

const good = makeEvent("SHP-UNIT0001", 2, "PICKED_UP", { location: "Oakland, CA" });

describe("decodeEvent", () => {
  it("accepts a valid v1 event", () => {
    const d = decodeEvent(JSON.stringify(good), good.shipmentId);
    expect(d.ok).toBe(true);
    if (d.ok) expect(d.event).toMatchObject({ shipmentId: "SHP-UNIT0001", seq: 2, type: "PICKED_UP", location: "Oakland, CA" });
  });

  it("maps a v2 facility into the same internal shape", () => {
    const { location: _drop, ...rest } = good;
    const v2 = { ...rest, schemaVersion: 2, facility: { code: "OAK1", city: "Oakland" } };
    const d = decodeEvent(JSON.stringify(v2), good.shipmentId);
    expect(d.ok && d.event.location).toBe("Oakland (OAK1)");
  });

  it("rejects bytes that are not JSON, without throwing", () => {
    const d = decodeEvent(Buffer.from("{not json"), null);
    expect(d).toMatchObject({ ok: false, reason: "invalid_json" });
  });

  it("rejects an unknown schema version and still reports the ids it could see", () => {
    const d = decodeEvent(JSON.stringify({ ...good, schemaVersion: 9 }), good.shipmentId);
    expect(d).toMatchObject({ ok: false, reason: "unsupported_version", shipmentId: "SHP-UNIT0001", eventId: good.eventId });
  });

  it("lists every schema problem with its path", () => {
    const d = decodeEvent(JSON.stringify({ ...good, seq: -1, type: "TELEPORTED", occurredAt: "yesterday" }), good.shipmentId);
    expect(d.ok).toBe(false);
    if (!d.ok) {
      expect(d.reason).toBe("schema_invalid");
      expect(d.issues.map((i) => i.split(":")[0]).sort()).toEqual(["occurredAt", "seq", "type"]);
    }
  });

  it("rejects a message whose Kafka key is not its shipmentId (it would break ordering)", () => {
    const d = decodeEvent(JSON.stringify(good), "SHP-OTHER");
    expect(d).toMatchObject({ ok: false, reason: "key_mismatch" });
  });
});

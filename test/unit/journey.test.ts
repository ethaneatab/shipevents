import { describe, expect, it } from "vitest";
import { journey } from "../../src/sim/events.ts";

describe("simulator journey", () => {
  it("numbers events 1..n and keeps time moving forward", () => {
    const evs = journey("SHP-TEST");
    expect(evs.map((e) => e.seq)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(evs.map((e) => e.type)).toEqual(["SHIPMENT_CREATED", "PICKED_UP", "HUB_SCAN", "HUB_SCAN", "OUT_FOR_DELIVERY", "DELIVERED"]);
    const times = evs.map((e) => Date.parse(e.occurredAt));
    expect([...times].sort((a, b) => a - b)).toEqual(times);
    expect(new Set(evs.map((e) => e.eventId)).size).toBe(6);
  });
});

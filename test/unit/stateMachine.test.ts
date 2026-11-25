import { describe, expect, it } from "vitest";
import { ALLOWED, decide } from "../../src/domain/stateMachine.ts";
import { STATUSES } from "../../src/events/types.ts";

describe("decide", () => {
  it("starts a new shipment at any entry status", () => {
    expect(decide(null, { seq: 1, type: "SHIPMENT_CREATED" })).toEqual({ kind: "apply", from: null, to: "CREATED" });
    expect(decide(null, { seq: 2, type: "PICKED_UP" })).toEqual({ kind: "apply", from: null, to: "PICKED_UP" });
  });

  it("refuses to start a shipment as RETURNED", () => {
    expect(decide(null, { seq: 1, type: "RETURNED_TO_SENDER" }).kind).toBe("reject");
  });

  it("moves forward, including skipped scans", () => {
    expect(decide({ status: "PICKED_UP", lastSeq: 2 }, { seq: 5, type: "OUT_FOR_DELIVERY" })).toEqual({ kind: "apply", from: "PICKED_UP", to: "OUT_FOR_DELIVERY" });
    expect(decide({ status: "IN_TRANSIT", lastSeq: 3 }, { seq: 4, type: "HUB_SCAN" }).kind).toBe("apply");
  });

  it("an older seq is stale even when the move itself would be legal", () => {
    const d = decide({ status: "OUT_FOR_DELIVERY", lastSeq: 5 }, { seq: 3, type: "HUB_SCAN" });
    expect(d).toMatchObject({ kind: "stale", from: "OUT_FOR_DELIVERY", to: "IN_TRANSIT" });
  });

  it("the same seq twice is stale (two different events claiming one slot)", () => {
    expect(decide({ status: "PICKED_UP", lastSeq: 2 }, { seq: 2, type: "PICKED_UP" }).kind).toBe("stale");
  });

  it("a newer seq that moves backwards is rejected, not applied", () => {
    expect(decide({ status: "DELIVERED", lastSeq: 6 }, { seq: 7, type: "HUB_SCAN" })).toMatchObject({ kind: "reject", reason: "illegal transition DELIVERED -> IN_TRANSIT" });
    expect(decide({ status: "OUT_FOR_DELIVERY", lastSeq: 5 }, { seq: 6, type: "PICKED_UP" }).kind).toBe("reject");
  });

  it("RETURNED is only reachable from EXCEPTION", () => {
    for (const s of STATUSES) {
      const d = decide({ status: s, lastSeq: 1 }, { seq: 2, type: "RETURNED_TO_SENDER" });
      expect([s, d.kind]).toEqual([s, s === "EXCEPTION" ? "apply" : "reject"]);
    }
  });

  it("terminal statuses have no exits", () => {
    expect(ALLOWED.DELIVERED).toEqual([]);
    expect(ALLOWED.RETURNED).toEqual([]);
  });
});

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("journey");
  await h.startProc();
});
afterAll(async () => h?.close());

describe("happy path", () => {
  it("a full journey ends DELIVERED with six history rows", async () => {
    const id = newShipmentId("T");
    const recs = await h.send(journey(id));
    expect(new Set(recs.map((r) => r.partition)).size).toBe(1); // keyed by shipmentId -> one partition
    const row = await h.waitStatus(id, "DELIVERED");
    expect(row.last_seq).toBe(6);
    expect(await h.history(id)).toHaveLength(6);
  });
});

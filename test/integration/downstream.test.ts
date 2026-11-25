import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { waitFor } from "../helpers/env.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("downstream");
  await h.startProc();
});
afterAll(async () => h?.close());
beforeEach(() => h.downstream.reset());

const dlqFor = (eventId: string) => waitFor(async () => (await h.pool.query("SELECT * FROM dlq_entries WHERE event_id = $1", [eventId])).rows[0]);

describe("unavailable dependencies", () => {
  it("two 503s then success: applied on the third attempt, attempts recorded", async () => {
    const id = newShipmentId("U");
    const [c, p] = journey(id, 2);
    h.downstream.faults.failNext.push({ shipmentId: id, count: 2, status: 503 });
    await h.send([c!]);
    await h.waitStatus(id, "CREATED");
    h.downstream.faults.failNext.push({ shipmentId: id, count: 2, status: 503 });
    await h.send([p!]);
    const s = await h.waitStatus(id, "PICKED_UP");
    expect(s.eta_source).toBe("eta-model-v3/picked_up");
    const row = (await h.history(id)).find((r) => r.event_id === p!.eventId)!;
    expect(row.attempts).toBe(3);
  });

  it("down for good: bounded at four attempts, dead-lettered with every attempt, partition keeps moving", async () => {
    const id = newShipmentId("U");
    const other = newShipmentId("U");
    const [c] = journey(id, 1);
    h.downstream.faults.mode = "error";
    await h.send([c!]);
    const dlq = await dlqFor(c!.eventId);
    expect(dlq.reason).toBe("downstream_exhausted");
    expect(dlq.detail.attempts.map((a: { error: string }) => a.error)).toEqual(["eta 503: {\"error\":\"eta service unavailable\"}", expect.any(String), expect.any(String), expect.any(String)]);
    expect(h.downstream.stats["503"]).toBe(4);
    // not marked processed: a later replay must be able to apply it
    expect((await h.pool.query("SELECT 1 FROM processed_events WHERE event_id = $1", [c!.eventId])).rowCount).toBe(0);
    h.downstream.faults.mode = "ok";
    const [c2] = journey(other, 1);
    await h.send([c2!]);
    await h.waitStatus(other, "CREATED");
  });

  it("a hung dependency is cut off by the timeout and dead-lettered", async () => {
    const id = newShipmentId("U");
    const [c] = journey(id, 1);
    Object.assign(h.downstream.faults, { mode: "timeout", latencyMs: 2000 });
    await h.send([c!]);
    const dlq = await dlqFor(c!.eventId);
    expect(dlq.detail.lastError).toBe("timeout after 300ms");
    expect(dlq.detail.attempts).toHaveLength(4);
  });

  it("a 400 is permanent: one attempt, reason downstream_rejected", async () => {
    const id = newShipmentId("U");
    const [c] = journey(id, 1);
    h.downstream.faults.failNext.push({ shipmentId: id, count: 1, status: 400 });
    await h.send([c!]);
    const dlq = await dlqFor(c!.eventId);
    expect(dlq.reason).toBe("downstream_rejected");
    expect(dlq.detail.attempts).toHaveLength(1);
  });
});

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, makeEvent, newShipmentId } from "../../src/sim/events.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { waitFor } from "../helpers/env.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("ordering");
  await h.startProc();
});
afterAll(async () => h?.close());

const waitHistory = (id: string, n: number) => waitFor(async () => {
  const rows = await h.history(id);
  return rows.length >= n ? rows : undefined;
});

describe("out-of-order events", () => {
  it("late hub scans arriving after OUT_FOR_DELIVERY go to history but do not move the status back", async () => {
    const id = newShipmentId("O");
    const [c, p, s1, s2, ofd] = journey(id, 5);
    await h.send([c!, p!, ofd!, s1!, s2!]);
    const rows = await waitHistory(id, 5);
    const s = (await h.shipment(id))!;
    expect({ status: s.status, lastSeq: s.last_seq }).toEqual({ status: "OUT_FOR_DELIVERY", lastSeq: 5 });
    expect(rows.map((r) => [r.seq, r.outcome])).toEqual([[1, "applied"], [2, "applied"], [5, "applied"], [3, "stale"], [4, "stale"]]);
    expect(rows[3]!.status_from).toBe("OUT_FOR_DELIVERY");
    expect(rows[3]!.detail.reason).toBe("seq 3 <= last applied seq 5");
  });

  it("a shipment whose CREATED event is late starts at PICKED_UP and keeps CREATED as stale history", async () => {
    const id = newShipmentId("O");
    const [c, p] = journey(id, 2);
    await h.send([p!, c!]);
    await waitHistory(id, 2);
    expect((await h.shipment(id))!.status).toBe("PICKED_UP");
    expect((await h.history(id)).map((r) => r.outcome)).toEqual(["applied", "stale"]);
  });
});

describe("illegal state transitions", () => {
  it("a newer scan after DELIVERED is rejected, recorded, and dead-lettered for investigation", async () => {
    const id = newShipmentId("I");
    const evs = journey(id);
    const bogus = makeEvent(id, 7, "HUB_SCAN", { location: "Reno hub" });
    await h.send([...evs, bogus]);
    const rows = await waitHistory(id, 7);
    expect((await h.shipment(id))!.status).toBe("DELIVERED");
    expect(rows[6]).toMatchObject({ outcome: "rejected", status_from: "DELIVERED", status_to: "IN_TRANSIT" });
    const dlq = await waitFor(async () => (await h.pool.query("SELECT * FROM dlq_entries WHERE event_id = $1", [bogus.eventId])).rows[0]);
    expect(dlq).toMatchObject({ reason: "illegal_transition", shipment_id: id, seq: 7, status: "open" });
    expect(dlq.detail).toMatchObject({ from: "DELIVERED", to: "IN_TRANSIT" });
  });

  it("RETURNED without an EXCEPTION first is rejected", async () => {
    const id = newShipmentId("I");
    const [c, p, s] = journey(id, 3);
    await h.send([c!, p!, s!, makeEvent(id, 4, "RETURNED_TO_SENDER")]);
    await waitHistory(id, 4);
    expect((await h.shipment(id))!.status).toBe("IN_TRANSIT");
  });

  it("EXCEPTION then RETURNED is legal", async () => {
    const id = newShipmentId("I");
    const [c, p] = journey(id, 2);
    await h.send([c!, p!, makeEvent(id, 3, "DELIVERY_EXCEPTION"), makeEvent(id, 4, "RETURNED_TO_SENDER")]);
    await h.waitStatus(id, "RETURNED");
  });
});

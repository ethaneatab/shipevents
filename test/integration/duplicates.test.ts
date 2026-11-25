import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { sleep } from "../helpers/env.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("dupes");
  await h.startProc();
});
afterAll(async () => h?.close());

describe("duplicate delivery", () => {
  it("the same event delivered three times changes the record once and appears once in history", async () => {
    const id = newShipmentId("D");
    const [created, picked, scan] = journey(id, 3);
    // at-least-once upstream: PICKED_UP arrives three times, byte for byte
    await h.send([created!, picked!, picked!, picked!, scan!]);
    await h.waitStatus(id, "IN_TRANSIT");
    await sleep(300); // give any (wrong) extra writes time to land

    const rows = await h.history(id);
    const perEvent = rows.reduce<Record<string, number>>((m, r) => ((m[r.event_id] = (m[r.event_id] ?? 0) + 1), m), {});
    expect(perEvent[picked!.eventId]).toBe(1);
    expect(rows).toHaveLength(3);
    expect((await h.shipment(id))!.version).toBe(3);
    const ledger = await h.pool.query("SELECT count(*)::int AS n FROM processed_events WHERE shipment_id = $1", [id]);
    expect(ledger.rows[0].n).toBe(3);
  });
});

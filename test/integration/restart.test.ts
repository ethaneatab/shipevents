import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import type { ShipmentEvent } from "../../src/events/types.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { spawnProcessor, type Child } from "../helpers/child.ts";
import { waitFor } from "../helpers/env.ts";

let h: Harness;
const children: Child[] = [];
beforeAll(async () => {
  h = await createHarness("restart");
});
afterAll(async () => {
  for (const c of children) c.kill("SIGKILL");
  await h?.close();
});

async function eventCounts(ids: string[]): Promise<Map<string, number>> {
  const r = await h.pool.query("SELECT event_id, count(*)::int AS n FROM shipment_history WHERE event_id = ANY($1) GROUP BY event_id", [ids]);
  return new Map(r.rows.map((x) => [x.event_id as string, x.n as number]));
}

describe("worker restarts", () => {
  it("killed between the database write and the offset commit: nothing lost, nothing doubled", async () => {
    const id = newShipmentId("R");
    const evs = journey(id);
    const victim = evs[3]!; // the second HUB_SCAN
    const first = spawnProcessor(h.names, h.downstream.url, { LAB_CRASH_ON_EVENT: victim.eventId });
    children.push(first);
    await h.send(evs.slice(0, 4));
    const exit = await first.exited;
    expect(exit.signal).toBe("SIGKILL");
    expect(first.output()).toContain("LAB_CRASH_ON_EVENT hit");

    const second = spawnProcessor(h.names, h.downstream.url);
    children.push(second);
    await h.send(evs.slice(4));
    await h.waitStatus(id, "DELIVERED", 40_000);

    const counts = await eventCounts(evs.map((e) => e.eventId));
    expect(evs.map((e) => counts.get(e.eventId) ?? 0)).toEqual([1, 1, 1, 1, 1, 1]);
    second.kill();
    await second.exited;
  });

  it("SIGKILL in the middle of a burst of 40 journeys (240 events): every event lands exactly once", async () => {
    const all: ShipmentEvent[] = [];
    const ids = Array.from({ length: 40 }, () => newShipmentId("B"));
    for (const id of ids) all.push(...journey(id));
    const first = spawnProcessor(h.names, h.downstream.url);
    children.push(first);
    const sending = h.send(all);
    // kill once roughly a third of the burst is in the database
    await waitFor(async () => {
      const r = await h.pool.query("SELECT count(*)::int AS n FROM shipment_history WHERE shipment_id = ANY($1)", [ids]);
      return r.rows[0].n >= 80;
    }, 40_000, 20);
    first.kill("SIGKILL");
    await first.exited;
    await sending;

    const second = spawnProcessor(h.names, h.downstream.url);
    children.push(second);
    await waitFor(async () => {
      const r = await h.pool.query("SELECT count(DISTINCT event_id)::int AS n FROM shipment_history WHERE shipment_id = ANY($1)", [ids]);
      return r.rows[0].n === all.length;
    }, 60_000, 200);

    const counts = await eventCounts(all.map((e) => e.eventId));
    const doubled = [...counts.values()].filter((n) => n !== 1).length;
    expect({ distinct: counts.size, doubled }).toEqual({ distinct: 240, doubled: 0 });
    const delivered = await h.pool.query("SELECT count(*)::int AS n FROM shipments WHERE shipment_id = ANY($1) AND status = 'DELIVERED'", [ids]);
    expect(delivered.rows[0].n).toBe(40);
    second.kill();
    await second.exited;
  }, 120_000);
});

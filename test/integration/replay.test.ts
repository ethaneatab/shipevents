import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { journey, makeEvent, newShipmentId } from "../../src/sim/events.ts";
import { executeReplay, planReplay, recordDryRun, ReplayRefused } from "../../src/replay/replay.ts";
import { buildAdmin } from "../../src/processor/admin.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { sleep, waitFor } from "../helpers/env.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("replay");
  await h.startProc();
});
afterAll(async () => h?.close());
beforeEach(() => h.downstream.reset());

const openEntries = (id: string, n: number) => waitFor(async () => {
  const r = await h.pool.query("SELECT * FROM dlq_entries WHERE shipment_id = $1 AND status = 'open' ORDER BY id", [id]);
  return r.rows.length >= n ? r.rows : undefined;
});

async function dryRun(filter: Parameters<typeof planReplay>[1]) {
  const plan = await planReplay(h.pool, filter);
  return { plan, id: await recordDryRun(h.pool, "test", filter, plan) };
}

describe("safe replay", () => {
  it("dependency down, then back: dry-run, replay in seq order, applied once, audited", async () => {
    const id = newShipmentId("P");
    const [c, p, s] = journey(id, 3);
    h.downstream.faults.mode = "error";
    // they also arrive out of order while the dependency is down
    await h.send([c!, s!, p!]);
    await openEntries(id, 3);
    h.downstream.faults.mode = "ok";

    const { plan, id: dry } = await dryRun({ shipmentIds: [id] });
    expect(plan.items.map((i) => [i.seq, i.verdict, i.from, i.to])).toEqual([
      [1, "apply", null, "CREATED"],
      [2, "apply", "CREATED", "PICKED_UP"],
      [3, "apply", "PICKED_UP", "IN_TRANSIT"],
    ]);
    expect(await h.shipment(id)).toBeUndefined(); // a dry-run writes nothing

    const res = await executeReplay(h.pool, h.producer, h.names.topic, dry, "test");
    expect(res.published.map((x) => x.seq)).toEqual([1, 2, 3]);
    await h.waitStatus(id, "IN_TRANSIT");
    const hist = await h.history(id);
    expect(hist.map((r) => [r.seq, r.outcome])).toEqual([[1, "applied"], [2, "applied"], [3, "applied"]]);
    expect(new Set(hist.map((r) => r.detail.replayId))).toEqual(new Set([res.replayId]));

    const audit = (await h.pool.query("SELECT * FROM replay_audit WHERE replay_id = $1", [res.replayId])).rows[0];
    expect(audit).toMatchObject({ kind: "execute", actor: "test", based_on: dry, status: "done", counts: { published: 3, skipped: 0 } });
    const left = await h.pool.query("SELECT status, count(*)::int AS n FROM dlq_entries WHERE shipment_id = $1 GROUP BY status", [id]);
    expect(left.rows).toEqual([{ status: "replayed", n: 3 }]);
  });

  it("replaying the same events twice changes nothing the second time (dedupe still applies)", async () => {
    const id = newShipmentId("P");
    const [c] = journey(id, 1);
    h.downstream.faults.mode = "error";
    await h.send([c!]);
    const [entry] = await openEntries(id, 1);
    h.downstream.faults.mode = "ok";
    const { id: dry } = await dryRun({ shipmentIds: [id] });
    await executeReplay(h.pool, h.producer, h.names.topic, dry, "test");
    await h.waitStatus(id, "CREATED");
    // an operator re-publishes the same DLQ payload by hand
    await h.send([{ raw: entry.original_value, shipmentId: id }], { "x-replay-id": "manual" });
    await sleep(800);
    expect(await h.history(id)).toHaveLength(1);
    expect((await h.shipment(id))!.version).toBe(1);
  });

  it("execute refuses without a matching, fresh dry-run", async () => {
    const id = newShipmentId("P");
    const [c, p] = journey(id, 2);
    h.downstream.faults.mode = "error";
    await h.send([c!]);
    await openEntries(id, 1);
    const { id: dry } = await dryRun({ shipmentIds: [id] });
    await h.send([p!]); // a new DLQ entry appears after the review
    await openEntries(id, 2);
    h.downstream.faults.mode = "ok";
    await expect(executeReplay(h.pool, h.producer, h.names.topic, dry, "test")).rejects.toMatchObject({ code: "plan_changed" });
    await expect(executeReplay(h.pool, h.producer, h.names.topic, "dry-nope", "test")).rejects.toBeInstanceOf(ReplayRefused);
    expect(await h.shipment(id)).toBeUndefined();
  });

  it("dry-run marks what a replay cannot fix, and execute skips those", async () => {
    const id = newShipmentId("P");
    const evs = journey(id);
    await h.send(evs);
    await h.waitStatus(id, "DELIVERED");
    const illegal = makeEvent(id, 7, "HUB_SCAN");
    const bad = { ...makeEvent(id, 8, "HUB_SCAN"), occurredAt: "not a date" };
    await h.send([illegal, { raw: JSON.stringify(bad), shipmentId: id }]);
    await openEntries(id, 2);
    const { plan, id: dry } = await dryRun({ shipmentIds: [id] });
    expect(plan.items.map((i) => [i.reason, i.verdict])).toEqual([
      ["illegal_transition", "skip_already_processed"],
      ["schema_invalid", "skip_invalid"],
    ]);
    const res = await executeReplay(h.pool, h.producer, h.names.topic, dry, "test");
    expect(res.published).toHaveLength(0);
    expect(res.skipped).toHaveLength(2);
  });

  it("a replayed event that is older than the current status becomes stale history, never a regression", async () => {
    const id = newShipmentId("P");
    const [c, p, s] = journey(id, 3);
    await h.send([c!]);
    await h.waitStatus(id, "CREATED");
    h.downstream.faults.mode = "error";
    await h.send([p!]);
    await openEntries(id, 1);
    h.downstream.faults.mode = "ok";
    await h.send([s!]);
    await h.waitStatus(id, "IN_TRANSIT");
    const { plan, id: dry } = await dryRun({ shipmentIds: [id] });
    expect(plan.items.map((i) => i.verdict)).toEqual(["stale"]);
    await executeReplay(h.pool, h.producer, h.names.topic, dry, "test");
    await waitFor(async () => (await h.history(id)).length === 3);
    expect((await h.shipment(id))!.status).toBe("IN_TRANSIT");
    expect((await h.history(id)).map((r) => r.outcome)).toEqual(["applied", "applied", "stale"]);
  });
});

describe("operator API auth", () => {
  it("admin endpoints need the bearer token", async () => {
    const app = buildAdmin({ pool: h.pool, producer: h.producer, eventsTopic: h.names.topic, adminToken: "test-token-123" });
    expect((await app.inject({ method: "GET", url: "/admin/dlq" })).statusCode).toBe(401);
    expect((await app.inject({ method: "GET", url: "/admin/dlq", headers: { authorization: "Bearer wrong-token-xx" } })).statusCode).toBe(401);
    expect((await app.inject({ method: "GET", url: "/admin/dlq", headers: { authorization: "Bearer test-token-123" } })).statusCode).toBe(200);
    expect((await app.inject({ method: "GET", url: "/healthz" })).statusCode).toBe(200);
    const noDry = await app.inject({ method: "POST", url: "/admin/replay/execute", headers: { authorization: "Bearer test-token-123" }, payload: {} });
    expect(noDry.json()).toMatchObject({ error: "dry_run_required" });
    await app.close();
  });
});

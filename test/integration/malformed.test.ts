import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, makeEvent, newShipmentId } from "../../src/sim/events.ts";
import { createHarness, readTopic, type Harness } from "../helpers/harness.ts";
import { waitFor } from "../helpers/env.ts";

let h: Harness;
beforeAll(async () => {
  h = await createHarness("malformed");
  await h.startProc();
});
afterAll(async () => h?.close());

describe("malformed payloads", () => {
  it("dead-letters a poison message with its reason, and keeps consuming the same partition", async () => {
    const id = newShipmentId("M");
    const bad = { ...makeEvent(id, 2, "PICKED_UP"), seq: "two" };
    const [created] = journey(id, 1);
    const recs = await h.send([created!, { raw: JSON.stringify(bad), shipmentId: id }, { raw: "{oops", shipmentId: id }, makeEvent(id, 3, "PICKED_UP")]);
    expect(new Set(recs.map((r) => r.partition)).size).toBe(1);

    // the good event *after* the poison ones still gets applied
    await h.waitStatus(id, "PICKED_UP");

    const rows = await waitFor(async () => {
      const r = await h.pool.query("SELECT reason, detail, source_offset FROM dlq_entries WHERE source_topic = $1 ORDER BY source_offset", [h.names.topic]);
      return r.rows.length >= 2 ? r.rows : undefined;
    });
    expect(rows.map((r) => r.reason)).toEqual(["schema_invalid", "invalid_json"]);
    expect(rows[0].detail.issues[0]).toMatch(/^seq:/);

    const dlq = await readTopic(h.names.dlqTopic, 2);
    expect(dlq.map((m) => m.headers["x-dlq-reason"])).toEqual(["schema_invalid", "invalid_json"]);
    // full context travels with the DLQ message: original bytes and where they came from
    expect(dlq[1]!.value.source).toMatchObject({ topic: h.names.topic, partition: recs[2]!.partition, offset: recs[2]!.offset, value: "{oops" });
  });
});

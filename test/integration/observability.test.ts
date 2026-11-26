import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { journey, newShipmentId } from "../../src/sim/events.ts";
import { createKafka } from "../../src/kafka/client.ts";
import { computeLag } from "../../src/obs/lag.ts";
import { registry } from "../../src/obs/metrics.ts";
import { buildApi } from "../../src/api/server.ts";
import { createHarness, type Harness } from "../helpers/harness.ts";
import { TEST_BROKERS, waitFor } from "../helpers/env.ts";
import type { Admin } from "kafkajs";

let h: Harness;
let admin: Admin;
beforeAll(async () => {
  h = await createHarness("obs");
  await h.startProc();
  admin = createKafka("test-lag", TEST_BROKERS).admin();
  await admin.connect();
});
afterAll(async () => {
  await admin?.disconnect();
  await h?.close();
});

describe("tracing a delayed update", () => {
  it("a slow dependency shows up as partition lag, then as downstream time in the latency breakdown", async () => {
    const id = newShipmentId("L");
    const evs = journey(id, 5);
    await h.send(evs.slice(0, 1));
    await h.waitStatus(id, "CREATED");
    Object.assign(h.downstream.faults, { mode: "slow", latencyMs: 250 });
    const recs = await h.send(evs.slice(1));
    const partition = recs[0]!.partition;

    // while the handler waits on the ETA service, messages pile up on this shipment's partition
    const seen = await waitFor(async () => {
      const lag = await computeLag(admin, h.names.topic, h.names.group);
      return lag.find((p) => p.partition === partition && p.lag >= 2);
    }, 10_000, 50);
    expect(seen.lag).toBeGreaterThanOrEqual(2);

    await h.waitStatus(id, "OUT_FOR_DELIVERY");
    h.downstream.reset();
    await waitFor(async () => (await computeLag(admin, h.names.topic, h.names.group)).every((p) => p.lag === 0), 10_000);

    const api = buildApi(h.pool);
    const res = (await api.inject({ url: `/v1/shipments/${id}/latency` })).json();
    const first = res.events[1]; // the first event that hit the slow dependency
    const last = res.events.at(-1);
    expect(first.downstream_ms).toBeGreaterThanOrEqual(250);
    expect(first.slowest).toBe("downstream");
    // the last one paid the same downstream cost, but mostly waited in the partition behind the others
    expect(last.downstream_ms).toBeGreaterThanOrEqual(250);
    expect(last.slowest).toBe("queue_or_db");
    expect(last.queue_and_db_ms).toBeGreaterThan(2 * 250);
    await api.close();
  });

  it("/metrics exposes throughput, failures by reason and processing lag", async () => {
    const text = await registry.metrics();
    expect(text).toMatch(/shipevents_events_total\{outcome="applied"\} \d+/);
    expect(text).toContain("shipevents_processing_lag_seconds_bucket");
    expect(text).toContain("shipevents_downstream_duration_seconds_count");
    expect(text).toContain("# TYPE shipevents_consumer_lag_messages gauge");
  });
});

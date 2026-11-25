// Operator CLI for the safe replay procedure (docs/replay.md).
//   node src/replay/cli.ts list
//   node src/replay/cli.ts plan    [--shipment ID]... [--reason R]... [--id N]... [--actor NAME]
//   node src/replay/cli.ts execute --dry-run-id dry-xxxx --actor NAME
//   node src/replay/cli.ts discard --id N [--id N] --note "why" --actor NAME
import { parseArgs } from "node:util";
import { config } from "../config.ts";
import { createPool } from "../db/pool.ts";
import { migrate } from "../db/migrate.ts";
import { createKafka, createProducer } from "../kafka/client.ts";
import { discardEntries, executeReplay, planReplay, recordDryRun, ReplayRefused } from "./replay.ts";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    shipment: { type: "string", multiple: true },
    reason: { type: "string", multiple: true },
    id: { type: "string", multiple: true },
    "dry-run-id": { type: "string" },
    actor: { type: "string", default: process.env.USER ?? "cli" },
    note: { type: "string", default: "" },
  },
});

const pool = createPool();
await migrate(pool);
const cmd = positionals[0];
try {
  if (cmd === "list") {
    const r = await pool.query("SELECT id, shipment_id, seq, reason, status, created_at FROM dlq_entries WHERE status = 'open' ORDER BY shipment_id, seq, id");
    console.table(r.rows);
  } else if (cmd === "plan") {
    const filter = { shipmentIds: values.shipment, reasons: values.reason, dlqIds: values.id?.map(Number) };
    const plan = await planReplay(pool, filter);
    const dryRunId = await recordDryRun(pool, values.actor, filter, plan);
    console.table(plan.items.map((i) => ({ dlqId: i.dlqId, shipment: i.shipmentId, seq: i.seq, reason: i.reason, verdict: i.verdict, move: `${i.from ?? "-"} -> ${i.to ?? "-"}`, note: i.note })));
    console.log(`dry-run ${dryRunId} (plan ${plan.hash}). To execute: node src/replay/cli.ts execute --dry-run-id ${dryRunId}`);
  } else if (cmd === "execute") {
    const producer = createProducer(createKafka("shipevents-replay"));
    await producer.connect();
    try {
      const r = await executeReplay(pool, producer, config.eventsTopic, values["dry-run-id"] ?? "", values.actor);
      console.log(JSON.stringify(r, null, 2));
    } finally {
      await producer.disconnect();
    }
  } else if (cmd === "discard") {
    console.log(await discardEntries(pool, (values.id ?? []).map(Number), values.actor, values.note));
  } else {
    console.error("usage: list | plan | execute --dry-run-id ID | discard --id N --note TEXT");
    process.exitCode = 2;
  }
} catch (e) {
  if (e instanceof ReplayRefused) {
    console.error(`refused (${e.code}): ${e.message}`);
    process.exitCode = 3;
  } else throw e;
} finally {
  await pool.end();
}

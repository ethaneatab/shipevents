// Event simulator. Usage: node src/sim/cli.ts <scenario> [--shipment ID] [--count N]
import { parseArgs } from "node:util";
import { config } from "../config.ts";
import { createKafka, createProducer, ensureTopics } from "../kafka/client.ts";
import { journey, makeEvent, newShipmentId } from "./events.ts";
import { sendEvents } from "./produce.ts";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: { shipment: { type: "string" }, count: { type: "string", default: "1" } },
});
const scenario = positionals[0] ?? "journey";

const kafka = createKafka("shipevents-sim");
await ensureTopics(kafka, [config.eventsTopic]);
const producer = createProducer(kafka);
await producer.connect();

const count = Number(values.count);
for (let i = 0; i < count; i++) {
  const id = values.shipment ?? newShipmentId();
  switch (scenario) {
    case "journey": {
      const recs = await sendEvents(producer, config.eventsTopic, journey(id));
      console.log(JSON.stringify({ scenario, shipmentId: id, records: recs }));
      break;
    }
    case "duplicate": {
      // at-least-once upstream: PICKED_UP delivered three times, byte for byte
      const [created, picked] = journey(id, 2);
      const recs = await sendEvents(producer, config.eventsTopic, [created!, picked!, picked!, picked!]);
      console.log(JSON.stringify({ scenario, shipmentId: id, duplicatedEventId: picked!.eventId, records: recs }));
      break;
    }
    case "out-of-order": {
      // the carrier's two hub scans arrive after OUT_FOR_DELIVERY
      const [c, p, s1, s2, ofd] = journey(id, 5);
      const recs = await sendEvents(producer, config.eventsTopic, [c!, p!, ofd!, s1!, s2!]);
      console.log(JSON.stringify({ scenario, shipmentId: id, sentSeqs: [1, 2, 5, 3, 4], records: recs }));
      break;
    }
    case "illegal": {
      // a scan with a newer seq after DELIVERED: not stale, but not a legal move either
      const evs = [...journey(id), makeEvent(id, 7, "HUB_SCAN", { location: "Reno hub" })];
      const recs = await sendEvents(producer, config.eventsTopic, evs);
      console.log(JSON.stringify({ scenario, shipmentId: id, records: recs.slice(-1) }));
      break;
    }
    case "malformed": {
      // seq should be a positive integer; the carrier sent a string
      const bad = { ...makeEvent(id, 2, "PICKED_UP"), seq: "two" };
      const recs = await sendEvents(producer, config.eventsTopic, [{ raw: JSON.stringify(bad), shipmentId: id }]);
      console.log(JSON.stringify({ scenario, shipmentId: id, records: recs }));
      break;
    }
    default:
      console.error(`unknown scenario ${scenario}`);
      process.exit(2);
  }
}
await producer.disconnect();

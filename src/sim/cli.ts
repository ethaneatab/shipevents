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

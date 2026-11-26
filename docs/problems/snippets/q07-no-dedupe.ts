// Before (step 1): every delivery writes, no event-id ledger.
// @learn:q07-no-dedupe-before
const client = await deps.pool.connect();
await client.query("BEGIN");
await client.query(`INSERT INTO shipments ... ON CONFLICT (shipment_id) DO UPDATE SET status = $2, version = shipments.version + 1`, [...]);
await client.query(`INSERT INTO shipment_history (shipment_id, event_id, ...) VALUES (...)`, [...]);
await client.query("COMMIT");
// the same PICKED_UP delivered three times = three history rows, version + 3
// @learn:end q07-no-dedupe-before

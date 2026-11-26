// Before (step 1): trust the bytes.
// @learn:q08-parse-before
export async function handleMessage(deps: Deps, msg: RawMessage): Promise<Outcome> {
  const event = JSON.parse(String(msg.value)) as ShipmentEvent;
  const to = TARGET_STATUS[event.type];
  // "{oops" throws here; kafkajs retries the same offset forever
  // and every later event on this partition waits behind it
}
// @learn:end q08-parse-before

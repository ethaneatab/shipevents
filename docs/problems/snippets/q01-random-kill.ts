// Before: the only restart test was a random SIGKILL during a burst.
// With LAB_SABOTAGE=no_dedupe it stayed green, because a random kill almost never
// lands in the few milliseconds between "database committed" and "offset committed".
// @learn:q01-random-kill-before
const first = spawnProcessor(h.names, h.downstream.url);
const sending = h.send(all);
await waitFor(async () => (await countHistory(ids)) >= 80, 40_000, 20);
first.kill("SIGKILL");
await sending;
const second = spawnProcessor(h.names, h.downstream.url);
await waitFor(async () => (await countDistinct(ids)) === all.length, 60_000, 200);
expect(doubled).toBe(0);
// @learn:end q01-random-kill-before

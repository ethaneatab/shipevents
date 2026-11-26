// Before: the first version of the observability test, written from an assumption.
// @learn:q03-assert-before
const res = (await api.inject({ url: `/v1/shipments/${id}/latency` })).json();
const last = res.events.at(-1);
expect(last.downstream_ms).toBeGreaterThanOrEqual(250);
expect(last.slowest).toBe("downstream");
// → expected 'queue_or_db' to be 'downstream'
// @learn:end q03-assert-before

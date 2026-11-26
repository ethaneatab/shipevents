import client from "prom-client";

// One registry for the processor. Names follow Prometheus conventions: _total for counters, base units (seconds).
export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry, prefix: "shipevents_node_" });

export const eventsTotal = new client.Counter({
  name: "shipevents_events_total",
  help: "Events handled by the processor, by outcome (applied, stale, rejected, duplicate, dlq)",
  labelNames: ["outcome"] as const,
  registers: [registry],
});

export const failuresTotal = new client.Counter({
  name: "shipevents_failures_total",
  help: "Events that did not apply, by reason (schema_invalid, downstream_exhausted, illegal_transition, ...)",
  labelNames: ["reason"] as const,
  registers: [registry],
});

export const processingLag = new client.Histogram({
  name: "shipevents_processing_lag_seconds",
  help: "Kafka append time to the moment the tracking record was committed",
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
  registers: [registry],
});

export const handleDuration = new client.Histogram({
  name: "shipevents_handle_duration_seconds",
  help: "Time spent inside the handler for one message, by outcome",
  labelNames: ["outcome"] as const,
  buckets: [0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const downstreamRequests = new client.Counter({
  name: "shipevents_downstream_requests_total",
  help: "ETA service attempts by result (ok, error)",
  labelNames: ["result"] as const,
  registers: [registry],
});

export const downstreamDuration = new client.Histogram({
  name: "shipevents_downstream_duration_seconds",
  help: "Duration of single ETA attempts",
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  registers: [registry],
});

export const retriesTotal = new client.Counter({
  name: "shipevents_retries_total",
  help: "Downstream attempts that were followed by another attempt",
  registers: [registry],
});

export const consumerLag = new client.Gauge({
  name: "shipevents_consumer_lag_messages",
  help: "Queue depth: high watermark minus committed offset, per partition",
  labelNames: ["partition"] as const,
  registers: [registry],
});

export const dlqOpen = new client.Gauge({
  name: "shipevents_dlq_open_entries",
  help: "DLQ entries still waiting for replay or discard, by reason",
  labelNames: ["reason"] as const,
  registers: [registry],
});

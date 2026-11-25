-- Dedupe ledger. A row here means "this event id has been fully handled".
-- It is inserted in the same transaction as the tracking-record update, so the two cannot disagree.
CREATE TABLE processed_events (
  event_id        text PRIMARY KEY,
  shipment_id     text NOT NULL,
  outcome         text NOT NULL,
  kafka_partition integer,
  kafka_offset    bigint,
  processed_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX processed_events_shipment ON processed_events (shipment_id);

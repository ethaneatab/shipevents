-- Index of everything we sent to the DLQ topic. The topic is the durable copy;
-- this table is what operators list, dry-run and replay from.
CREATE TABLE dlq_entries (
  id               bigserial PRIMARY KEY,
  event_id         text,
  shipment_id      text,
  seq              integer,
  reason           text NOT NULL,
  detail           jsonb NOT NULL DEFAULT '{}'::jsonb,
  original_key     text,
  original_value   text NOT NULL,
  source_topic     text NOT NULL,
  source_partition integer NOT NULL,
  source_offset    bigint NOT NULL,
  dlq_partition    integer,
  dlq_offset       bigint,
  status           text NOT NULL DEFAULT 'open',
  created_at       timestamptz NOT NULL DEFAULT now(),
  resolved_at      timestamptz,
  replay_id        text,
  -- The same Kafka record redelivered must not create a second entry.
  UNIQUE (source_topic, source_partition, source_offset)
);
CREATE INDEX dlq_entries_open ON dlq_entries (status, shipment_id, seq);

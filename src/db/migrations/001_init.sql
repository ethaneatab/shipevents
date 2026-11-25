-- The tracking record customers read, one row per shipment.
CREATE TABLE shipments (
  shipment_id   text PRIMARY KEY,
  status        text NOT NULL,
  last_event_id text NOT NULL,
  last_seq      integer NOT NULL,
  last_location text,
  version       integer NOT NULL DEFAULT 1,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Append-only history: every event we looked at, applied or not.
CREATE TABLE shipment_history (
  id            bigserial PRIMARY KEY,
  shipment_id   text NOT NULL,
  event_id      text NOT NULL,
  seq           integer NOT NULL,
  event_type    text NOT NULL,
  status_from   text,
  status_to     text,
  outcome       text NOT NULL,
  occurred_at   timestamptz NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now(),
  kafka_partition integer,
  kafka_offset  bigint,
  detail        jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX shipment_history_shipment ON shipment_history (shipment_id, seq, id);

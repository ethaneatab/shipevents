ALTER TABLE shipments ADD COLUMN eta_at timestamptz;
ALTER TABLE shipments ADD COLUMN eta_source text;
ALTER TABLE shipments ADD COLUMN last_trace_id text;
ALTER TABLE shipment_history ADD COLUMN trace_id text;
ALTER TABLE shipment_history ADD COLUMN attempts integer;
ALTER TABLE shipment_history ADD COLUMN downstream_ms integer;
-- When Kafka accepted the record; processed_at - kafka_ts is the end-to-end processing lag.
ALTER TABLE shipment_history ADD COLUMN kafka_ts timestamptz;

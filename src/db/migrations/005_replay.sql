-- Every dry-run, replay and discard leaves a row here: who, what filter, the exact plan, and the result.
CREATE TABLE replay_audit (
  replay_id   text PRIMARY KEY,
  kind        text NOT NULL CHECK (kind IN ('dry_run', 'execute', 'discard')),
  actor       text NOT NULL,
  filter      jsonb NOT NULL DEFAULT '{}'::jsonb,
  plan        jsonb NOT NULL DEFAULT '[]'::jsonb,
  plan_hash   text NOT NULL,
  based_on    text REFERENCES replay_audit (replay_id),
  status      text NOT NULL,
  counts      jsonb NOT NULL DEFAULT '{}'::jsonb,
  note        text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

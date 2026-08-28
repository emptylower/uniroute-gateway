-- 213_wallet_hold_outcome.sql (Phase 3.4b, redesign §10.6)
CREATE TABLE IF NOT EXISTS wallet_hold_outcome (
  authorization_id    TEXT PRIMARY KEY,
  platform_user_id    TEXT NOT NULL,
  lease_id            TEXT NOT NULL,
  held_units          BIGINT NOT NULL,
  class               TEXT NOT NULL,
  armed_at            TIMESTAMPTZ NOT NULL,
  classified_at       TIMESTAMPTZ NOT NULL,
  resolution          TEXT NULL,
  resolved_at         TIMESTAMPTZ NULL,
  settlement_event_id TEXT NULL
);
CREATE INDEX IF NOT EXISTS wallet_hold_outcome_resolution_idx ON wallet_hold_outcome (resolution, classified_at);
COMMENT ON TABLE wallet_hold_outcome IS 'Phase 3.4b: the durable outcome of an indeterminate or abandoned hold; resolution NULL = open, settled | abandoned | expired';

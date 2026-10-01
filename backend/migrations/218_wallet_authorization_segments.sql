-- One billable write may reserve several existing leases without transferring
-- principal. Commit the complete plan before the all-or-nothing Redis arm.
CREATE TABLE IF NOT EXISTS wallet_authorization_segment (
    parent_authorization_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    authorization_id TEXT NOT NULL UNIQUE,
    platform_user_id TEXT NOT NULL,
    billing_snapshot_id TEXT NOT NULL REFERENCES wallet_billing_snapshot(id) ON DELETE RESTRICT,
    lease_id TEXT NOT NULL,
    held_units BIGINT NOT NULL CHECK (held_units > 0),
    lease_basis JSONB NOT NULL,
    event_id TEXT UNIQUE,
    actual_units BIGINT NOT NULL DEFAULT 0 CHECK (actual_units >= 0 AND actual_units <= held_units),
    pin_state TEXT NOT NULL DEFAULT 'none' CHECK (pin_state IN ('none','active','finished')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_authorization_id, ordinal),
    CHECK ((ordinal = 0 AND authorization_id = parent_authorization_id) OR ordinal > 0)
);
CREATE INDEX IF NOT EXISTS idx_wallet_authorization_segment_lease
    ON wallet_authorization_segment (platform_user_id, lease_id);

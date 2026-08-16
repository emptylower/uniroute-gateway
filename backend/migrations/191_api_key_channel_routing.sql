-- Add per-key channel routing preferences and user defaults.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS routing_mode VARCHAR(20) NOT NULL DEFAULT 'legacy_group';

ALTER TABLE api_keys
    DROP CONSTRAINT IF EXISTS chk_api_keys_routing_mode;

ALTER TABLE api_keys
    ADD CONSTRAINT chk_api_keys_routing_mode
    CHECK (routing_mode IN ('legacy_group', 'channels'));

CREATE TABLE IF NOT EXISTS api_key_channels (
    api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    channel_id BIGINT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (api_key_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_api_key_channels_channel_id
    ON api_key_channels (channel_id);

CREATE TABLE IF NOT EXISTS user_default_channels (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id BIGINT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_user_default_channels_channel_id
    ON user_default_channels (channel_id);

COMMENT ON COLUMN api_keys.routing_mode IS 'legacy_group uses group_id; channels expands api_key_channels';
COMMENT ON TABLE api_key_channels IS 'User-selected channels enabled for one API key';
COMMENT ON TABLE user_default_channels IS 'Channels copied to newly-created API keys';

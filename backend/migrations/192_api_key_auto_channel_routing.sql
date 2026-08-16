-- Allow system-managed keys to consider every currently available channel.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

ALTER TABLE api_keys
    DROP CONSTRAINT IF EXISTS chk_api_keys_routing_mode;

ALTER TABLE api_keys
    ADD CONSTRAINT chk_api_keys_routing_mode
    CHECK (routing_mode IN ('legacy_group', 'channels', 'auto_channels'));

COMMENT ON COLUMN api_keys.routing_mode IS
    'legacy_group uses group_id; channels expands api_key_channels; auto_channels resolves all available channels per request';

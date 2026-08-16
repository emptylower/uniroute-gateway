-- Persist per-user routing-group opt-outs for automatic channel routing.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

CREATE TABLE IF NOT EXISTS user_disabled_routing_groups (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_user_disabled_routing_groups_group_id
    ON user_disabled_routing_groups (group_id);

COMMENT ON TABLE user_disabled_routing_groups IS
    'Routing groups explicitly disabled by a user; absence means enabled';

-- A channel choice belongs to one user and exact model, not a shared group.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

CREATE TABLE IF NOT EXISTS user_model_channel_preferences (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    model_id VARCHAR(200) NOT NULL CHECK (
        model_id = LOWER(BTRIM(model_id)) AND
        model_id ~ '^[a-z0-9][a-z0-9._:/-]*$'
    ),
    channel VARCHAR(20) NOT NULL CHECK (channel IN ('official', 'cloud-vendor')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, model_id)
);

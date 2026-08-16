-- ShipAny-projected users are owned by the platform identity bridge and use
-- `platform` as their signup source. Keep the database constraint aligned with
-- the Ent validator before the bridge creates its first projected user.
ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_signup_source_check;

ALTER TABLE users
    ADD CONSTRAINT users_signup_source_check
    CHECK (signup_source IN (
        'email',
        'linuxdo',
        'wechat',
        'oidc',
        'github',
        'google',
        'dingtalk',
        'platform'
    ));

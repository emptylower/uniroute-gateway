-- Public task identity is scoped to both the account and calling API key.
ALTER TABLE gateway_media_task DROP CONSTRAINT IF EXISTS gateway_media_task_user_id_idempotency_key_key;
CREATE UNIQUE INDEX IF NOT EXISTS gateway_media_task_key_idempotency_unique
 ON gateway_media_task(user_id,api_key_id,idempotency_key);
CREATE INDEX IF NOT EXISTS gateway_media_task_key_history
 ON gateway_media_task(user_id,api_key_id,created_at DESC,id DESC);
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS public_api BOOLEAN NOT NULL DEFAULT false;

-- The same durable protection applies to HTTP/WS/Live attempts, so a lease
-- sweep cannot return an unknown provider write's funds.
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'llm' CHECK (kind IN ('llm','live','media'));
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'prepared' CHECK (state IN ('prepared','held','indeterminate','settling','released','finished'));
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS authorization_token TEXT;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_wallet_authorization_segment_pending ON wallet_authorization_segment (updated_at) WHERE state <> 'finished';

ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS settlement_payload JSONB;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS remainder_payload JSONB;

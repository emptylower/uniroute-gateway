-- Billing V2 currency snapshots and currency-specific group multipliers.
-- Additive only: legacy rate_multiplier remains the fallback for both currencies.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS billing_currency VARCHAR(3) NOT NULL DEFAULT 'CNY';

ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS rate_multiplier_cny DECIMAL(10,4),
    ADD COLUMN IF NOT EXISTS rate_multiplier_usd DECIMAL(10,4);

ALTER TABLE redeem_codes
    ADD COLUMN IF NOT EXISTS currency VARCHAR(3) NOT NULL DEFAULT 'CNY';

ALTER TABLE promo_codes
    ADD COLUMN IF NOT EXISTS currency VARCHAR(3) NOT NULL DEFAULT 'CNY';

ALTER TABLE payment_orders
    ADD COLUMN IF NOT EXISTS settlement_currency VARCHAR(3);

ALTER TABLE batch_image_jobs
    ADD COLUMN IF NOT EXISTS exchange_rate DECIMAL(20,10) NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS exchange_rate_source VARCHAR(64) NOT NULL DEFAULT 'legacy_batch_image',
    ADD COLUMN IF NOT EXISTS exchange_rate_as_of TIMESTAMPTZ;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS source_currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    ADD COLUMN IF NOT EXISTS settlement_currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    ADD COLUMN IF NOT EXISTS exchange_rate DECIMAL(20,10) NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS exchange_rate_source VARCHAR(64) NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS exchange_rate_as_of TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_cost DECIMAL(20,10) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS base_cost DECIMAL(20,10) NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_users_billing_currency') THEN
        ALTER TABLE users ADD CONSTRAINT chk_users_billing_currency
            CHECK (billing_currency IN ('CNY', 'USD')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_usage_logs_source_currency') THEN
        ALTER TABLE usage_logs ADD CONSTRAINT chk_usage_logs_source_currency
            CHECK (source_currency IN ('CNY', 'USD')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_usage_logs_settlement_currency') THEN
        ALTER TABLE usage_logs ADD CONSTRAINT chk_usage_logs_settlement_currency
            CHECK (settlement_currency IN ('CNY', 'USD')) NOT VALID;
    END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_redeem_codes_currency') THEN
		ALTER TABLE redeem_codes ADD CONSTRAINT chk_redeem_codes_currency
			CHECK (currency IN ('CNY', 'USD')) NOT VALID;
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_promo_codes_currency') THEN
		ALTER TABLE promo_codes ADD CONSTRAINT chk_promo_codes_currency
			CHECK (currency IN ('CNY', 'USD')) NOT VALID;
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payment_orders_settlement_currency') THEN
		ALTER TABLE payment_orders ADD CONSTRAINT chk_payment_orders_settlement_currency
			CHECK (settlement_currency IS NULL OR settlement_currency IN ('CNY', 'USD')) NOT VALID;
	END IF;
END $$;

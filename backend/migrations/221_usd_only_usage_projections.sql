-- Additive read projections. Original amounts and frozen rates remain audit facts.
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS actual_cost_usd DECIMAL(20,10) NOT NULL DEFAULT 0;
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS base_cost_usd DECIMAL(20,10) NOT NULL DEFAULT 0;
ALTER TABLE usage_logs ALTER COLUMN settlement_currency SET DEFAULT 'USD';
ALTER TABLE users ALTER COLUMN billing_currency SET DEFAULT 'USD';
ALTER TABLE redeem_codes ALTER COLUMN currency SET DEFAULT 'USD';
ALTER TABLE promo_codes ALTER COLUMN currency SET DEFAULT 'USD';

CREATE OR REPLACE FUNCTION usage_logs_project_usd() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.exchange_rate_source = 'legacy' THEN
        -- Legacy rows require reviewed evidence before the explicit backfill.
        -- Existing USD-only writers omit snapshots in a few non-billable paths.
        IF TG_OP = 'INSERT' THEN
            NEW.settlement_currency := 'USD';
            NEW.exchange_rate := 1;
            NEW.exchange_rate_source := 'usd-e8-v1';
            NEW.base_cost := NEW.total_cost;
            NEW.actual_cost_usd := NEW.actual_cost;
            NEW.base_cost_usd := NEW.total_cost;
        END IF;
    ELSIF NEW.settlement_currency = 'USD' THEN
        NEW.actual_cost_usd := NEW.actual_cost;
        NEW.base_cost_usd := NEW.base_cost;
    ELSIF NEW.settlement_currency = 'CNY' AND NEW.exchange_rate > 0 THEN
        NEW.actual_cost_usd := NEW.actual_cost / NEW.exchange_rate;
        NEW.base_cost_usd := NEW.base_cost / NEW.exchange_rate;
    ELSE
        RAISE EXCEPTION 'Usage row % has no auditable USD conversion', NEW.id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS usage_logs_project_usd ON usage_logs;
CREATE TRIGGER usage_logs_project_usd BEFORE INSERT OR UPDATE OF actual_cost, base_cost, exchange_rate, settlement_currency
 ON usage_logs FOR EACH ROW EXECUTE FUNCTION usage_logs_project_usd();

CREATE TABLE IF NOT EXISTS usd_only_migration_state (
    name TEXT PRIMARY KEY, completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS usd_only_migration_review (
    entity TEXT NOT NULL, entity_id TEXT NOT NULL, reason TEXT NOT NULL,
    original_amount NUMERIC, reviewed_at TIMESTAMPTZ,
    PRIMARY KEY(entity, entity_id)
);
CREATE TABLE IF NOT EXISTS usd_only_native_balance_export (
    user_id BIGINT PRIMARY KEY, platform_user_id TEXT NOT NULL,
    source_currency TEXT NOT NULL CHECK(source_currency IN ('CNY','USD')),
    original_balance NUMERIC NOT NULL CHECK(original_balance > 0),
    amount_units BIGINT NOT NULL CHECK(amount_units > 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    exported_at TIMESTAMPTZ NOT NULL DEFAULT now(), retired_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS usd_only_native_balance_receipt (
    user_id BIGINT PRIMARY KEY REFERENCES usd_only_native_balance_export(user_id),
    platform_user_id TEXT NOT NULL, amount_units BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE, console_grant_id TEXT NOT NULL UNIQUE,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

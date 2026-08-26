-- Durable outbox for canonical wallet settlement events (Phase 2).
-- At-least-once delivery: ObserveSettlement inserts synchronously; a polling
-- dispatcher claims rows (status -> 'in_flight' with a claimed_by ownership
-- token), delivers them to the control plane, and resolves the row. A
-- crashed dispatcher's rows are reclaimed via the claimed_at staleness
-- threshold. event_id is the idempotency identity; payload_hash detects a
-- re-priced duplicate under the same event_id (reject, never double-apply).
CREATE TABLE IF NOT EXISTS wallet_settlement_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    platform_user_id TEXT NOT NULL,
    lease_id TEXT, -- nullable: a post-hoc settlement event (ObserveSettlement) does not resolve a lease until the dispatcher delivers it
    gateway_request_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_units BIGINT NOT NULL,
    local_balance_after_units BIGINT, -- nullable: mirrors CanonicalWalletSettlementEvent.LocalBalanceAfterUnits (*int64)
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending | in_flight | delivered | dead_letter
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ, -- when this row was last transitioned to 'in_flight'; NULL once back in 'pending'/'delivered'/'dead_letter'
    claimed_by TEXT, -- opaque per-dispatcher-instance claim token; every resolve is conditioned on it so a stale claimant cannot resolve a row it no longer owns
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_pending
    ON wallet_settlement_outbox (next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_stale_in_flight
    ON wallet_settlement_outbox (claimed_at)
    WHERE status = 'in_flight';

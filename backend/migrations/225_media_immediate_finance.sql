-- Provider execution and the v5 financial boundary have independent lifetimes.
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_policy_version TEXT NOT NULL DEFAULT 'wallet-immediate-v5';
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_state TEXT NOT NULL DEFAULT 'held' CHECK (financial_state IN ('held','unknown_pending','zero_pending','released_unknown','released_zero','fee_pending','charged'));
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_runtime_deadline TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS query_episode_first_failed_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS query_uncertainty_deadline TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS write_owner TEXT;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS write_started_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS write_ended_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_terminal_proof TEXT CHECK (financial_terminal_proof IS NULL OR financial_terminal_proof ~ '^[0-9a-f]{64}$');
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_terminal_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS financial_released_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS fee_pending_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS fee_evidence JSONB;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS fee_plan_at TIMESTAMPTZ;
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS next_fee_recovery_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE gateway_media_task ADD COLUMN IF NOT EXISTS next_financial_recovery_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE gateway_media_task DROP CONSTRAINT IF EXISTS gateway_media_task_check;
ALTER TABLE gateway_media_task ADD CONSTRAINT gateway_media_task_actual_units_check CHECK(actual_units IS NULL OR actual_units>=0);
ALTER TABLE gateway_media_task DROP CONSTRAINT IF EXISTS gateway_media_task_check4;
ALTER TABLE gateway_media_task ADD CONSTRAINT media_failed_financial_state_check CHECK(status<>'failed' OR (pin_state='finished' AND (actual_units=0 OR financial_state='released_unknown')));
WITH anchors AS (
 SELECT t.id,COALESCE(min(s.first_write_at),t.created_at) anchor
 FROM gateway_media_task t LEFT JOIN wallet_authorization_segment s ON s.parent_authorization_id=t.authorization_id
 WHERE t.status NOT IN ('completed','failed') AND (t.provider_task_id IS NOT NULL OR t.status IN ('submitting','indeterminate')) GROUP BY t.id
)
UPDATE gateway_media_task t SET accepted_at=a.anchor,financial_runtime_deadline=a.anchor+CASE WHEN t.media_type='image' THEN interval '30 minutes' ELSE interval '2 hours' END,
 write_started_at=a.anchor,write_ended_at=CASE WHEN t.provider_task_id IS NOT NULL THEN t.updated_at ELSE NULL END,
 financial_state=CASE WHEN t.status='settling' THEN 'fee_pending' WHEN t.status='releasing' THEN 'zero_pending' ELSE 'held' END,
 fee_pending_at=CASE WHEN t.status='settling' THEN t.updated_at ELSE NULL END,
 fee_evidence=CASE WHEN t.status='settling' THEN jsonb_build_object('source','kie_authoritative_get','provider_task_id',t.provider_task_id,'snapshot_id',t.billing_snapshot_id,'quoted_units',t.quoted_units,'result',t.result) ELSE NULL END
 FROM anchors a WHERE t.id=a.id;
UPDATE gateway_media_task SET financial_state=CASE WHEN status='completed' THEN 'charged' ELSE 'released_zero' END WHERE status IN ('completed','failed');
CREATE INDEX IF NOT EXISTS idx_media_fee_recovery ON gateway_media_task(next_fee_recovery_at,id) WHERE financial_state='fee_pending';
CREATE INDEX IF NOT EXISTS idx_media_financial_recovery ON gateway_media_task(next_financial_recovery_at,id) WHERE financial_state IN ('held','unknown_pending','zero_pending');
CREATE OR REPLACE FUNCTION guard_media_financial_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.id,NEW.authorization_id,NEW.platform_user_id,NEW.billing_snapshot_id,NEW.quoted_units,NEW.settlement_event_id,NEW.financial_policy_version)
 IS DISTINCT FROM ROW(OLD.id,OLD.authorization_id,OLD.platform_user_id,OLD.billing_snapshot_id,OLD.quoted_units,OLD.settlement_event_id,OLD.financial_policy_version)
 OR OLD.accepted_at IS NOT NULL AND ROW(NEW.accepted_at,NEW.financial_runtime_deadline) IS DISTINCT FROM ROW(OLD.accepted_at,OLD.financial_runtime_deadline)
 OR OLD.write_started_at IS NOT NULL AND ROW(NEW.write_started_at,NEW.write_owner,NEW.authorization_token) IS DISTINCT FROM ROW(OLD.write_started_at,OLD.write_owner,OLD.authorization_token)
 OR OLD.write_ended_at IS NOT NULL AND NEW.write_ended_at IS DISTINCT FROM OLD.write_ended_at
 OR OLD.provider_task_id IS NOT NULL AND NEW.provider_task_id IS DISTINCT FROM OLD.provider_task_id
 OR OLD.financial_terminal_proof IS NOT NULL AND ROW(NEW.financial_terminal_proof,NEW.financial_terminal_at) IS DISTINCT FROM ROW(OLD.financial_terminal_proof,OLD.financial_terminal_at)
 OR OLD.financial_released_at IS NOT NULL AND NEW.financial_released_at IS DISTINCT FROM OLD.financial_released_at
 OR OLD.fee_evidence IS NOT NULL AND ROW(NEW.fee_evidence,NEW.fee_pending_at,NEW.actual_units) IS DISTINCT FROM ROW(OLD.fee_evidence,OLD.fee_pending_at,OLD.actual_units)
 OR OLD.financial_state IN ('released_unknown','released_zero','charged') AND NEW.financial_state='held'
 OR OLD.financial_state='released_zero' AND NEW.financial_state<>'released_zero'
 OR OLD.financial_state='charged' AND NEW.financial_state<>'charged'
 THEN RAISE EXCEPTION 'media financial identity and first evidence are immutable'; END IF;
 IF NEW.query_episode_first_failed_at IS NOT NULL AND (NEW.query_uncertainty_deadline IS DISTINCT FROM NEW.query_episode_first_failed_at+interval '15 minutes')
 OR NEW.financial_runtime_deadline IS NOT NULL AND (NEW.financial_runtime_deadline IS DISTINCT FROM NEW.accepted_at+CASE WHEN NEW.media_type='image' THEN interval '30 minutes' ELSE interval '2 hours' END)
 THEN RAISE EXCEPTION 'media financial deadline policy conflict'; END IF;
 IF OLD.query_episode_first_failed_at IS NOT NULL AND NEW.query_episode_first_failed_at IS NOT NULL AND ROW(NEW.query_episode_first_failed_at,NEW.query_uncertainty_deadline) IS DISTINCT FROM ROW(OLD.query_episode_first_failed_at,OLD.query_uncertainty_deadline)
 THEN RAISE EXCEPTION 'media query episode cannot slide'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS media_financial_identity_guard ON gateway_media_task;
CREATE TRIGGER media_financial_identity_guard BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION guard_media_financial_identity();

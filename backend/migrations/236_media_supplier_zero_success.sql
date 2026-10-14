-- Only a new, frozen supplier-priced task with explicit zero supplier usage
-- and completed signed financial release may finish successfully at zero.
-- Keep the existing positive settlement guard and every other constraint.
ALTER TABLE gateway_media_task DROP CONSTRAINT gateway_media_task_check3;
ALTER TABLE gateway_media_task ADD CONSTRAINT gateway_media_task_check3 CHECK (
 status NOT IN ('settling','completed') OR (actual_units IS NOT NULL AND actual_units > 0)
 OR COALESCE(
  status='completed' AND actual_units=0 AND financial_state='released_zero'
  AND pin_state='finished' AND financial_released_at IS NOT NULL
  AND financial_terminal_proof IS NOT NULL AND financial_terminal_at IS NOT NULL
  AND financial_policy_version='wallet-immediate-v5' AND fee_pending_at IS NOT NULL
  AND fee_evidence->>'source'='kie_authoritative_get'
  AND fee_evidence->>'provider_task_id'=provider_task_id
  AND fee_evidence->>'snapshot_id'=billing_snapshot_id
  AND fee_evidence->>'quoted_units'=quoted_units::text
  AND fee_evidence->>'actual_units'='0'
  AND fee_evidence->'supplier_pricing'->>'policy_version'='kie-cost-plus-25-20261014'
  AND fee_evidence->'supplier_pricing'->>'credit_usd'='0.005'
  AND fee_evidence->'supplier_pricing'->>'markup_bps'='2500'
  AND fee_evidence->'supplier_pricing'->>'budget_units'=quoted_units::text
  AND length(fee_evidence->'supplier_pricing'->>'source_version')>0
  AND result->>'status'='success' AND result->>'provider_task_id'=provider_task_id
  AND result->>'provider_model'=model
  AND result->>'credits_consumed' ~ '^0+(\.0+)?$'
  AND fee_evidence->'result'=result,
  false
 )
);

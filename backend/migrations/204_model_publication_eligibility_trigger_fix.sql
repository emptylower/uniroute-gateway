-- Phase 5 fix: scope the eligibility exact-unique trigger to identity changes only.
-- Root cause: 203 created trg_model_publication_eligibility_exact_unique firing on
-- INSERT as well as identity updates, but BEFORE triggers run before ON CONFLICT
-- arbitration, making the repo's INSERT ... ON CONFLICT DO UPDATE upsert
-- unreachable for existing identities. Insert-time duplicates remain guarded by
-- uq_model_publication_eligibility_identity; ON CONFLICT DO UPDATE now works.
-- Additive only; never edits 203.

DROP TRIGGER IF EXISTS trg_model_publication_eligibility_exact_unique ON model_publication_eligibility;

CREATE TRIGGER trg_model_publication_eligibility_exact_unique
    BEFORE UPDATE OF account_id, canonical_model_id, channel_id ON model_publication_eligibility
    FOR EACH ROW EXECUTE FUNCTION enforce_model_publication_eligibility_exact_unique();

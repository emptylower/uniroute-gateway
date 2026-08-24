-- Fix: resolved_commit must fit content-digest versions ("sha256:<64 hex>" = 71
-- chars). Git SHAs fit in VARCHAR(64); content-addressed digests did not, which
-- made FinishSyncRun fail and left sync runs stuck in 'running' forever.
ALTER TABLE model_catalog_sync_runs
    ALTER COLUMN resolved_commit TYPE VARCHAR(128);

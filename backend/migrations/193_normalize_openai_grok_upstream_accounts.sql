-- OpenAI-compatible custom Base URL accounts use the standard API-key transport.
-- The deprecated upstream alias is not understood by model sync, connection tests,
-- or the data plane, so normalize rows accepted by older control clients.
UPDATE accounts
SET type = 'apikey', updated_at = CURRENT_TIMESTAMP
WHERE type = 'upstream'
  AND platform IN ('openai', 'grok')
  AND deleted_at IS NULL;

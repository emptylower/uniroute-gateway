-- A live provider owner and its first observed success survive failed PG handoffs.
-- Evidence lives on the already registered durable wallet volume, independently
-- of the LLM reader journal and runtime feature flags.
ALTER TABLE gateway_media_task ADD COLUMN media_journal_volume TEXT;
ALTER TABLE gateway_media_task ADD COLUMN media_journal_owner TEXT;
ALTER TABLE gateway_media_task ADD COLUMN media_journal_pending BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE gateway_media_task ADD COLUMN next_journal_recovery_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE gateway_media_task ADD COLUMN write_proven_not_sent BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE gateway_media_task ADD COLUMN write_proven_zero BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE gateway_media_task ADD CONSTRAINT media_durable_journal_identity CHECK (
 (media_journal_volume IS NULL AND media_journal_owner IS NULL AND NOT media_journal_pending)
 OR (media_journal_volume IS NOT NULL AND media_journal_owner IS NOT NULL AND length(media_journal_volume) BETWEEN 1 AND 128 AND length(media_journal_owner) BETWEEN 1 AND 128)
);
CREATE INDEX idx_media_durable_journal_recovery ON gateway_media_task(next_journal_recovery_at,id) WHERE media_journal_pending;
CREATE FUNCTION guard_media_durable_journal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.media_journal_volume IS NOT NULL AND NEW.media_journal_volume IS DISTINCT FROM OLD.media_journal_volume
 OR OLD.write_proven_not_sent AND NOT NEW.write_proven_not_sent
 OR OLD.write_proven_zero AND NOT NEW.write_proven_zero
 THEN RAISE EXCEPTION 'media durable journal identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER media_durable_journal_guard BEFORE UPDATE ON gateway_media_task FOR EACH ROW EXECUTE FUNCTION guard_media_durable_journal();

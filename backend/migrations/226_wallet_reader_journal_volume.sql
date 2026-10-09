-- One immutable journal-volume identity per Gateway database. Container IDs,
-- controller URLs and runtime flags cannot implicitly adopt a replacement
-- volume. Existing pre-journal rows do not provide a volume identity.
CREATE TABLE wallet_reader_journal_volume (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    volume_id TEXT NOT NULL CHECK (length(volume_id) BETWEEN 1 AND 128),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION wallet_reader_journal_volume_immutable() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet reader journal volume identity is immutable';
END;
$$;

CREATE TRIGGER wallet_reader_journal_volume_immutable
BEFORE UPDATE OR DELETE ON wallet_reader_journal_volume
FOR EACH ROW EXECUTE FUNCTION wallet_reader_journal_volume_immutable();

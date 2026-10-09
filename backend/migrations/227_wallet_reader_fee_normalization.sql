ALTER TABLE wallet_authorization_segment ADD COLUMN reader_fee_normalization JSONB;
ALTER TABLE wallet_authorization_segment ADD CONSTRAINT wallet_reader_fee_normalization_shape
CHECK (reader_fee_normalization IS NULL OR (jsonb_typeof(reader_fee_normalization)='object' AND reader_fee_normalization->>'version'='1'));

CREATE FUNCTION wallet_reader_fee_normalization_immutable() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.reader_fee_normalization IS NOT NULL AND NEW.reader_fee_normalization IS DISTINCT FROM OLD.reader_fee_normalization THEN
        RAISE EXCEPTION 'wallet reader fee normalization is immutable';
    END IF;
    IF OLD.authorization_token IS NOT NULL AND OLD.reader_fee_normalization IS NULL AND NEW.reader_fee_normalization IS NOT NULL
       AND OLD.reader_journal_host IS NULL THEN
        RAISE EXCEPTION 'historical reader without durable normalization cannot adopt fee defaults';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallet_reader_fee_normalization_immutable
BEFORE UPDATE ON wallet_authorization_segment
FOR EACH ROW EXECUTE FUNCTION wallet_reader_fee_normalization_immutable();

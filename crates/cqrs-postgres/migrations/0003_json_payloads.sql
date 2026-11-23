-- Payloads and metadata as `json`: stored as written and returned as is.
-- They are never queried inside (tags have their own table), so `jsonb`'s
-- parse on every write and re-serialization on every read bought nothing.
-- Guarded: runs once, on a schema still holding `jsonb`.
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'cqrs_events' AND column_name = 'payload') = 'jsonb' THEN
        ALTER TABLE cqrs_events
            ALTER COLUMN payload TYPE json USING payload::json,
            ALTER COLUMN metadata TYPE json USING metadata::json;
    END IF;
END $$;

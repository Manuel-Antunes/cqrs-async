-- Named logs: several event types (one enum per aggregate) can share one
-- database, each store reading and appending only its own log. Rows that
-- exist when this runs belong to the log called 'default'. Guarded: runs
-- once, on a schema whose events have no `log` yet.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'cqrs_events' AND column_name = 'log'
    ) THEN
        ALTER TABLE cqrs_events ADD COLUMN log text NOT NULL DEFAULT 'default';
        ALTER TABLE cqrs_events ALTER COLUMN log DROP DEFAULT;
        ALTER TABLE cqrs_events DROP CONSTRAINT cqrs_events_stream_type_stream_id_sequence_key;
        ALTER TABLE cqrs_events ADD CONSTRAINT cqrs_events_log_stream_sequence_key
            UNIQUE (log, stream_type, stream_id, sequence);
        CREATE INDEX cqrs_events_log_position ON cqrs_events (log, position);

        ALTER TABLE cqrs_event_tags ADD COLUMN log text NOT NULL DEFAULT 'default';
        ALTER TABLE cqrs_event_tags ALTER COLUMN log DROP DEFAULT;
        ALTER TABLE cqrs_event_tags DROP CONSTRAINT cqrs_event_tags_pkey;
        ALTER TABLE cqrs_event_tags ADD PRIMARY KEY (log, tag, position);
    END IF;
END $$;

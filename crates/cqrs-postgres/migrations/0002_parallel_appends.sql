-- Parallel appends (Axon's JDBC design): positions come from a sequence, so
-- appends no longer wait for each other on `cqrs_head`; they commit out of
-- order and readers track the gaps (`cqrs::Cursor`). `cqrs_head` and
-- `cqrs_consistency` are no longer written.

-- CACHE 1: values are handed out in the order they are asked for, across
-- sessions, which is what makes a gap's horizon sound (see `cqrs_positions`).
DO $$
BEGIN
    IF to_regclass('cqrs_position') IS NULL THEN
        CREATE SEQUENCE cqrs_position CACHE 1;
        PERFORM setval('cqrs_position', coalesce((SELECT max(position) FROM cqrs_events), 0) + 1, false);
    END IF;
END $$;

-- Advisory locks on keys derived from the domain (a tag, an event type, a
-- stream), always taken in key order so they never deadlock. Writers take
-- them shared, so appends run in parallel; reading the events matching a
-- criterion and checking it (DCB) take that criterion's keys exclusive, so no
-- matching append is half-way while they run.
CREATE OR REPLACE FUNCTION cqrs_lock(exclusive bigint[], shared bigint[]) RETURNS void
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    key bigint;
BEGIN
    FOR key IN SELECT DISTINCT k FROM unnest(exclusive || shared) AS k ORDER BY 1 LOOP
        IF key = ANY(exclusive) THEN
            PERFORM pg_advisory_xact_lock(key);
        ELSE
            PERFORM pg_advisory_xact_lock_shared(key);
        END IF;
    END LOOP;
END $$;

-- `n` positions for one append, in order, after its locks. The transaction
-- takes its id before its positions: whoever holds a position a reader saw
-- missing started before that read, so once every transaction older than
-- the read finished, the gap can no longer be filled.
CREATE OR REPLACE FUNCTION cqrs_positions(n integer, shared bigint[]) RETURNS SETOF bigint
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM cqrs_lock('{}', shared);
    PERFORM pg_current_xact_id();
    RETURN QUERY SELECT nextval('cqrs_position') FROM generate_series(1, n);
END $$;

-- A token is a cursor: its position and the gaps below it, each with the
-- horizon after which nobody can fill it.
ALTER TABLE cqrs_tokens ADD COLUMN IF NOT EXISTS gaps bigint[] NOT NULL DEFAULT '{}';
ALTER TABLE cqrs_tokens ADD COLUMN IF NOT EXISTS gap_horizons bigint[] NOT NULL DEFAULT '{}';

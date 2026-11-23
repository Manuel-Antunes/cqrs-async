-- The cqrs tables, in their final shape. Idempotent: `cqrs_postgres::migrate`
-- runs it on every start, and `sqlx migrate run --source <this dir>` works
-- too. The query macros check every statement of the crate against this
-- schema.

-- One row per event, in one of several named logs (one event enum per log, so
-- several aggregates' event types can share a database). `position` is
-- global: it comes from `cqrs_position`, so the positions of one log are not
-- contiguous when others append in between. Payloads and metadata are `json`:
-- stored as written and returned as is; they are never queried inside (tags
-- have their own table).
CREATE TABLE IF NOT EXISTS cqrs_events (
    log           text        NOT NULL,
    position      bigint      PRIMARY KEY,
    stream_type   text        NOT NULL,
    stream_id     text        NOT NULL,
    sequence      bigint      NOT NULL,
    event_type    text        NOT NULL,
    event_version integer     NOT NULL,
    payload       json        NOT NULL,
    metadata      json        NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (log, stream_type, stream_id, sequence)
);
CREATE INDEX IF NOT EXISTS cqrs_events_log_position ON cqrs_events (log, position);

-- Tag -> events, in position order (Axon's `tags` table): what criteria are
-- read through.
CREATE TABLE IF NOT EXISTS cqrs_event_tags (
    log      text   NOT NULL,
    tag      text   NOT NULL,
    position bigint NOT NULL,
    PRIMARY KEY (log, tag, position)
);

CREATE TABLE IF NOT EXISTS cqrs_snapshots (
    stream_type text    NOT NULL,
    stream_id   text    NOT NULL,
    revision    integer NOT NULL,
    version     bigint  NOT NULL,
    state       jsonb   NOT NULL,
    PRIMARY KEY (stream_type, stream_id)
);

-- A token is a cursor: its position and the gaps below it, each with the
-- horizon after which nobody can fill it.
CREATE TABLE IF NOT EXISTS cqrs_tokens (
    processor     text        NOT NULL,
    segment       integer     NOT NULL,
    position      bigint      NOT NULL DEFAULT 0,
    owner         text,
    claimed_until timestamptz,
    replay_until  bigint      NOT NULL DEFAULT 0,
    gaps          bigint[]    NOT NULL DEFAULT '{}',
    gap_horizons  bigint[]    NOT NULL DEFAULT '{}',
    PRIMARY KEY (processor, segment)
);

-- Parked events (the dead-letter queue): which event of which sequence
-- (aggregate stream) a processor gave up on, and why. The event itself is
-- read from the log by position, so nothing is copied.
CREATE TABLE IF NOT EXISTS cqrs_dead_letters (
    processor text        NOT NULL,
    sequence  text        NOT NULL,
    position  bigint      NOT NULL,
    cause     text        NOT NULL,
    parked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (processor, sequence, position)
);

-- Parallel appends (Axon's JDBC design): positions come from a sequence, so
-- appends never wait for each other on a shared row; they commit out of
-- order and readers track the gaps (`cqrs::Cursor`).
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
-- stream, within a log), always taken in key order so they never deadlock.
-- Writers take them shared, so appends run in parallel; reading the events
-- matching a criterion and checking it (DCB) take that criterion's keys
-- exclusive, so no matching append is half-way while they run.
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

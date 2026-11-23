-- The cqrs tables. Idempotent: `cqrs_postgres::migrate` runs it on every
-- start, and `sqlx migrate run --source <this dir>` works too. The query
-- macros check every statement of the crate against this schema.

CREATE TABLE IF NOT EXISTS cqrs_events (
    position      bigint      PRIMARY KEY,
    stream_type   text        NOT NULL,
    stream_id     text        NOT NULL,
    sequence      bigint      NOT NULL,
    event_type    text        NOT NULL,
    event_version integer     NOT NULL,
    payload       jsonb       NOT NULL,
    metadata      jsonb       NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream_type, stream_id, sequence)
);

-- Tag -> events, in position order (Axon's `tags` table): what criteria are
-- read through.
CREATE TABLE IF NOT EXISTS cqrs_event_tags (
    tag      text   NOT NULL,
    position bigint NOT NULL,
    PRIMARY KEY (tag, position)
);

-- The last position each tag was written at (Axon's `consistency_tags`,
-- keyed by the tag itself rather than a hash: exact, and one row per tag,
-- not per event). A conditional append checks it first.
CREATE TABLE IF NOT EXISTS cqrs_consistency (
    tag      text   PRIMARY KEY,
    position bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS cqrs_head (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    position  bigint  NOT NULL
);
INSERT INTO cqrs_head (singleton, position) VALUES (true, 0) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS cqrs_snapshots (
    stream_type text    NOT NULL,
    stream_id   text    NOT NULL,
    revision    integer NOT NULL,
    version     bigint  NOT NULL,
    state       jsonb   NOT NULL,
    PRIMARY KEY (stream_type, stream_id)
);

CREATE TABLE IF NOT EXISTS cqrs_tokens (
    processor     text        NOT NULL,
    segment       integer     NOT NULL,
    position      bigint      NOT NULL DEFAULT 0,
    owner         text,
    claimed_until timestamptz,
    replay_until  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (processor, segment)
);
ALTER TABLE cqrs_tokens ADD COLUMN IF NOT EXISTS replay_until bigint NOT NULL DEFAULT 0;

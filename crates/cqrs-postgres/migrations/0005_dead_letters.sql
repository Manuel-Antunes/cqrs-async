-- Parked events (the dead-letter queue): which event of which sequence
-- (aggregate stream) a processor gave up on, and why. The event itself is
-- read from the log by position, so nothing is copied. Idempotent.
CREATE TABLE IF NOT EXISTS cqrs_dead_letters (
    processor text        NOT NULL,
    sequence  text        NOT NULL,
    position  bigint      NOT NULL,
    cause     text        NOT NULL,
    parked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (processor, sequence, position)
);

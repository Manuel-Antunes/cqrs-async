//! Fan-in over PostgreSQL: several logs read as one, in the global order
//! their shared sequence gives them.

use std::{fmt, marker::PhantomData, sync::Arc};

use cqrs::{
    event::EventEnvelope,
    store::{Cursor, Horizon, LogPage, ReadLog, StoreError},
};
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::{
    Upcaster, backend,
    event_store::{EventRow, PostgresEventStore, cursor_at, to_i64},
    unsigned,
};

/// Several logs of one database read as one ([`ReadLog`] of `U`), in
/// position order: each event decoded by its own log's store (with its
/// upcasters), then converted into `U` (an enum with a variant per log's
/// event type, `#[event(fan_in)]`). What a projection over those logs runs
/// on; [`Postgres::fan_in`](crate::Postgres::fan_in) makes its events.
///
/// Each read takes, per log, the next events in the `(log, position)`
/// index, then merges them: as cheap per page as reading one log. Gaps are
/// tracked as for one log (a [`Cursor`]), over the positions of all of
/// them.
pub struct PostgresFanIn<U, L> {
    /// The logs, a tuple of stores.
    logs: L,
    /// Their names, as the reads bind them.
    names: Arc<[String]>,
    /// The connections.
    pool: PgPool,
    /// The event they are read as.
    event: PhantomData<fn() -> U>,
}

impl<U, L: PostgresLogs<U>> PostgresFanIn<U, L> {
    /// Reads `logs` (a tuple of 2 to 8 stores of one database) as one.
    pub fn new(logs: L) -> Self {
        Self {
            names: logs.names().into(),
            pool: logs.pool().clone(),
            logs,
            event: PhantomData,
        }
    }
}

impl<U, L> PostgresFanIn<U, L> {
    /// The logs.
    pub fn logs(&self) -> &L {
        &self.logs
    }
}

impl<U, L: Clone> Clone for PostgresFanIn<U, L> {
    fn clone(&self) -> Self {
        Self {
            logs: self.logs.clone(),
            names: Arc::clone(&self.names),
            pool: self.pool.clone(),
            event: PhantomData,
        }
    }
}

impl<U, L> fmt::Debug for PostgresFanIn<U, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresFanIn")
            .field("logs", &self.names)
            .finish_non_exhaustive()
    }
}

/// A tuple of 2 to 8 [`PostgresEventStore`]s whose event types convert into
/// `U`: what a [`PostgresFanIn`] reads.
pub trait PostgresLogs<U>: sealed::Sealed + Send + Sync {
    /// The logs' names.
    #[doc(hidden)]
    fn names(&self) -> Vec<String>;

    /// The pool of the first store (they share a database).
    #[doc(hidden)]
    fn pool(&self) -> &PgPool;

    /// The event of `row`, read from the log `log`.
    #[doc(hidden)]
    #[allow(private_interfaces)]
    fn decode(&self, log: &str, row: EventRow) -> Result<EventEnvelope<U>, StoreError>;
}

/// The seal of [`PostgresLogs`]: only tuples of stores.
mod sealed {
    /// Implemented for tuples of `PostgresEventStore`s.
    pub trait Sealed {}
}

/// `event`, its payload converted into `U`.
fn convert<E, U: From<E>>(event: EventEnvelope<E>) -> EventEnvelope<U> {
    EventEnvelope {
        position: event.position,
        stream: event.stream,
        sequence: event.sequence,
        event_type: event.event_type,
        event_version: event.event_version,
        recorded_at: event.recorded_at,
        metadata: event.metadata,
        payload: U::from(event.payload),
    }
}

/// Implements [`PostgresLogs`] for a tuple of stores.
macro_rules! postgres_logs {
    ($(($event:ident, $upcaster:ident)),+) => {
        impl<$($event, $upcaster),+> sealed::Sealed for ($(PostgresEventStore<$event, $upcaster>,)+) {}

        #[allow(non_snake_case)]
        impl<Union, $($event, $upcaster),+> PostgresLogs<Union> for ($(PostgresEventStore<$event, $upcaster>,)+)
        where
            $(Union: From<$event>, $event: DeserializeOwned, $upcaster: Upcaster,)+
        {
            fn names(&self) -> Vec<String> {
                let ($($event,)+) = self;
                vec![$($event.log().to_owned()),+]
            }

            fn pool(&self) -> &PgPool {
                self.0.pool()
            }

            #[allow(private_interfaces)]
            fn decode(&self, log: &str, row: EventRow) -> Result<EventEnvelope<Union>, StoreError> {
                let ($($event,)+) = self;
                $(
                    if $event.log() == log {
                        return $event.decode_row(row).map(convert);
                    }
                )+
                Err(StoreError::backend(format!("an event of the log `{log}`, which this fan-in does not read")))
            }
        }
    };
}

postgres_logs!((A, UA), (B, UB));
postgres_logs!((A, UA), (B, UB), (C, UC));
postgres_logs!((A, UA), (B, UB), (C, UC), (D, UD));
postgres_logs!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE));
postgres_logs!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF));
postgres_logs!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF), (G, UG));
postgres_logs!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF), (G, UG), (H, UH));

impl<U, L> ReadLog<U> for PostgresFanIn<U, L>
where
    U: Send + Sync,
    L: PostgresLogs<U>,
{
    async fn read_all(&self, after: u64, limit: usize) -> Result<Vec<EventEnvelope<U>>, StoreError> {
        let rows = sqlx::query!(
            r#"
            SELECT e.log AS "log!", e.position AS "position!", e.stream_type AS "stream_type!",
                   e.stream_id AS "stream_id!", e.sequence AS "sequence!", e.event_type AS "event_type!",
                   e.event_version AS "event_version!", e.payload AS "payload!", e.metadata AS "metadata!",
                   e.recorded_us AS "recorded_us!"
            FROM unnest($1::text[]) AS l(name)
            CROSS JOIN LATERAL (
                SELECT log, position, stream_type, stream_id, sequence, event_type, event_version,
                       payload::text AS payload, metadata::text AS metadata,
                       (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                FROM cqrs_events WHERE log = l.name AND position > $2 ORDER BY position LIMIT $3
            ) AS e
            ORDER BY e.position LIMIT $3"#,
            &self.names[..],
            to_i64(after),
            i64::try_from(limit).unwrap_or(i64::MAX),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter()
            .map(|row| {
                let event = EventRow {
                    position: row.position,
                    stream_type: row.stream_type,
                    stream_id: row.stream_id,
                    sequence: row.sequence,
                    event_type: row.event_type,
                    event_version: row.event_version,
                    payload: row.payload,
                    metadata: row.metadata,
                    recorded_us: row.recorded_us,
                };
                self.logs.decode(&row.log, event)
            })
            .collect()
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        let head = sqlx::query_scalar!(
            r#"
            SELECT coalesce(max(h.head), 0) AS "head!"
            FROM unnest($1::text[]) AS l(name)
            CROSS JOIN LATERAL (SELECT max(position) AS head FROM cqrs_events WHERE log = l.name) AS h"#,
            &self.names[..],
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        unsigned(head)
    }

    /// As [`PostgresEventStore::read`](ReadLog::read), over all the logs:
    /// the cursor's gaps committed since in any of them, then what follows
    /// it in each, merged, with the snapshot they were read in; the
    /// positions skipped that no log at all committed are the only ones
    /// that may still be filled.
    async fn read(&self, cursor: &Cursor, limit: usize) -> Result<LogPage<U>, StoreError> {
        let gaps: Vec<i64> = cursor.gaps().iter().map(|gap| to_i64(gap.position)).collect();
        let rows = sqlx::query!(
            r#"
            WITH snapshot AS (
                SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS xmin,
                       pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS xmax
            ),
            page AS (
                (SELECT log, position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata,
                        (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                 FROM cqrs_events WHERE log = ANY($4) AND position = ANY($2))
                UNION ALL
                (SELECT e.log, e.position, e.stream_type, e.stream_id, e.sequence, e.event_type, e.event_version,
                        e.payload, e.metadata, e.recorded_us
                 FROM unnest($4::text[]) AS l(name)
                 CROSS JOIN LATERAL (
                     SELECT log, position, stream_type, stream_id, sequence, event_type, event_version, payload,
                            metadata, (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                     FROM cqrs_events WHERE log = l.name AND position > $1 ORDER BY position LIMIT $3
                 ) AS e)
                ORDER BY position LIMIT $3
            ),
            missing AS (
                SELECT array(
                    SELECT p FROM (
                        SELECT generate_series($1::bigint + 1, (SELECT coalesce(max(position), $1) FROM page)) AS p
                        EXCEPT
                        SELECT position FROM page
                    ) AS absent
                    WHERE NOT EXISTS (SELECT 1 FROM cqrs_events WHERE position = p)
                    ORDER BY p
                ) AS positions
            )
            SELECT s.xmin AS "xmin!", s.xmax AS "xmax!", m.positions AS "missing!",
                   e.log AS "log?", e.position AS "position?", e.stream_type AS "stream_type?",
                   e.stream_id AS "stream_id?", e.sequence AS "sequence?", e.event_type AS "event_type?",
                   e.event_version AS "event_version?", e.payload::text AS "payload?", e.metadata::text AS "metadata?",
                   e.recorded_us AS "recorded_us?"
            FROM snapshot s CROSS JOIN missing m LEFT JOIN page e ON true"#,
            to_i64(cursor.position()),
            &gaps,
            i64::try_from(limit).unwrap_or(i64::MAX),
            &self.names[..],
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let horizon = match rows.first() {
            Some(row) => Horizon {
                finished_below: unsigned(row.xmin)?,
                started_below: unsigned(row.xmax)?,
            },
            None => Horizon::SETTLED,
        };
        let missing = match rows.first() {
            Some(row) => row
                .missing
                .iter()
                .map(|&position| unsigned(position))
                .collect::<Result<_, _>>()?,
            None => Vec::new(),
        };
        let events = rows
            .into_iter()
            .filter_map(|row| {
                let event = EventRow {
                    position: row.position?,
                    stream_type: row.stream_type?,
                    stream_id: row.stream_id?,
                    sequence: row.sequence?,
                    event_type: row.event_type?,
                    event_version: row.event_version?,
                    payload: row.payload?,
                    metadata: row.metadata?,
                    recorded_us: row.recorded_us?,
                };
                Some((row.log?, event))
            })
            .map(|(log, event)| self.logs.decode(&log, event))
            .collect::<Result<_, _>>()?;
        Ok(LogPage {
            events,
            horizon,
            missing: Some(missing),
        })
    }

    async fn cursor_at(&self, position: u64) -> Result<Cursor, StoreError> {
        cursor_at(&self.pool, position).await
    }
}

/// The events of the logs a fan-in reads: a tuple of 2 to 8 references to
/// [`PgEvents`](crate::PgEvents), for [`Postgres::fan_in`](crate::Postgres::fan_in).
#[cfg(feature = "tokio")]
pub trait PgFanInSources: sources_sealed::Sealed {
    /// The stores, as [`PostgresFanIn`] reads them.
    type Logs;

    /// A clone of each store.
    #[doc(hidden)]
    fn logs(&self) -> Self::Logs;

    /// Has `events` follow each source.
    #[doc(hidden)]
    fn followed_by<Union, Store, Tokens, Transactions>(
        &self,
        events: &cqrs::processing::Events<Union, Store, Tokens, Transactions>,
    );
}

/// The seal of [`PgFanInSources`].
#[cfg(feature = "tokio")]
mod sources_sealed {
    /// Implemented for tuples of references to `PgEvents`.
    pub trait Sealed {}
}

/// Implements [`PgFanInSources`] for a tuple of references to events.
#[cfg(feature = "tokio")]
macro_rules! pg_fan_in_sources {
    ($(($event:ident, $upcaster:ident)),+) => {
        impl<$($event, $upcaster),+> sources_sealed::Sealed for ($(&crate::PgEvents<$event, $upcaster>,)+) {}

        #[allow(non_snake_case)]
        impl<$($event, $upcaster),+> PgFanInSources for ($(&crate::PgEvents<$event, $upcaster>,)+) {
            type Logs = ($(PostgresEventStore<$event, $upcaster>,)+);

            fn logs(&self) -> Self::Logs {
                let ($($event,)+) = self;
                ($($event.store().clone(),)+)
            }

            fn followed_by<Union, Store, Tokens, Transactions>(
                &self,
                events: &cqrs::processing::Events<Union, Store, Tokens, Transactions>,
            ) {
                let ($($event,)+) = self;
                $(events.follow($event);)+
            }
        }
    };
}

#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC), (D, UD));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF), (G, UG));
#[cfg(feature = "tokio")]
pg_fan_in_sources!((A, UA), (B, UB), (C, UC), (D, UD), (E, UE), (F, UF), (G, UG), (H, UH));

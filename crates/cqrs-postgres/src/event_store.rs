use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cqrs::{
    event::{AppendCondition, DomainEvent, EventCriteria, EventEnvelope, Metadata, StreamId},
    store::{
        AppendEvents, Cursor, ExpectedVersion, Gap, Horizon, LoadStream, LogPage, ReadLog, Recorded, SourceEvents,
        StoreError, TaggedEvent,
    },
};
use futures_util::TryStreamExt;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Connection, PgConnection, PgExecutor, PgPool, Postgres, Transaction};

use crate::{StoredEvent, Upcaster, backend, unsigned};

/// A named event log in PostgreSQL, events stored as JSON (`serde`), read
/// through the [`Upcaster`]s `U` (none by default).
///
/// Every store names its log, and reads and appends nothing else, so several
/// event types (one enum per aggregate, as the guide recommends) can share
/// one database: `PostgresEventStore::<PostEvent>::new(pool, "posts")` and
/// `PostgresEventStore::<UserEvent>::new(pool, "users")`. Positions come
/// from one sequence for the whole database, so a log's positions are not
/// contiguous when others append in between.
///
/// Cloning shares the pool: give a clone to the repository, the processors
/// and the subscriptions (`Events::new(store)` does).
pub struct PostgresEventStore<E, U = ()> {
    /// The connection pool.
    pool: PgPool,
    /// The log this store reads and appends: its events and their tags.
    log: Arc<str>,
    /// Rewrites old payloads before decoding.
    upcaster: Arc<U>,
    /// The event type this store reads and writes.
    event: PhantomData<fn() -> E>,
}

impl<E, U> ::std::fmt::Debug for PostgresEventStore<E, U> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("PostgresEventStore").finish_non_exhaustive()
    }
}

impl<E> PostgresEventStore<E> {
    /// A store of the log `log` over `pool`, whose schema
    /// [`migrate`](crate::migrate) created. Two stores with the same log name
    /// share their events: give each event type its own name.
    pub fn new(pool: PgPool, log: &str) -> Self {
        Self {
            pool,
            log: log.into(),
            upcaster: Arc::new(()),
            event: PhantomData,
        }
    }
}

impl<E, U> PostgresEventStore<E, U> {
    /// Runs `upcasters` (one [`Upcaster`], or a tuple of them, oldest step
    /// The log it reads and appends to.
    pub fn log(&self) -> &str {
        &self.log
    }

    /// first) on every event read, instead of the current ones.
    pub fn with_upcasters<U2: Upcaster>(self, upcasters: U2) -> PostgresEventStore<E, U2> {
        PostgresEventStore {
            pool: self.pool,
            log: self.log,
            upcaster: Arc::new(upcasters),
            event: PhantomData,
        }
    }
}

impl<E, U> Clone for PostgresEventStore<E, U> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            log: Arc::clone(&self.log),
            upcaster: Arc::clone(&self.upcaster),
            event: PhantomData,
        }
    }
}

/// A row as every read selects it.
struct EventRow {
    position: i64,
    stream_type: String,
    stream_id: String,
    sequence: i64,
    event_type: String,
    event_version: i32,
    /// As stored (`json`): decoded straight into the event type.
    payload: String,
    /// As stored (`json`).
    metadata: String,
    recorded_us: i64,
}

/// `sqlx::query_as!` reading [`EventRow`]s from `cqrs_events` filtered by
/// `$where` (string literals, joined with `+`): every statement is checked
/// against the schema at compile time, and `query!` takes literals only, so
/// the shared column list is spliced in here rather than with `concat!`.
macro_rules! select_events {
    ($log:expr, $($where:literal)+, $($arg:expr),* $(,)?) => {
        sqlx::query_as!(
            EventRow,
            "SELECT position, stream_type, stream_id, sequence, event_type, event_version, \
             payload::text AS \"payload!\", metadata::text AS \"metadata!\", \
             (extract(epoch FROM recorded_at) * 1000000)::bigint AS \"recorded_us!\" FROM cqrs_events WHERE log = $1 AND "
            $(+ $where)+,
            $log,
            $($arg),*
        )
    };
}

impl<E, U> LoadStream<E> for PostgresEventStore<E, U>
where
    E: DeserializeOwned + Send + Sync,
    U: Upcaster,
{
    async fn load(&self, stream: &StreamId, after: u64) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let rows = select_events!(
            &*self.log,
            "stream_type = $2 AND stream_id = $3 AND sequence > $4 ORDER BY sequence",
            stream.aggregate_type(),
            stream.id(),
            to_i64(after),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter().map(|row| envelope(row, &*self.upcaster)).collect()
    }

    /// Reads only what rebuilding an aggregate uses (sequence, type,
    /// version, payload) and decodes each row as it arrives: no metadata,
    /// no stream ids, no list of envelopes.
    async fn replay<F>(&self, stream: &StreamId, after: u64, mut apply: F) -> Result<u64, StoreError>
    where
        E: Send,
        F: FnMut(u64, E) + Send,
    {
        let mut rows = sqlx::query!(
            r#"SELECT sequence, event_type, event_version, payload::text AS "payload!" FROM cqrs_events
               WHERE log = $1 AND stream_type = $2 AND stream_id = $3 AND sequence > $4 ORDER BY sequence"#,
            &*self.log,
            stream.aggregate_type(),
            stream.id(),
            to_i64(after),
        )
        .fetch(&self.pool);
        let mut count = 0;
        while let Some(row) = rows.try_next().await.map_err(backend)? {
            let (event, _, _) = StoredEvent::decode_text(
                row.event_type,
                u32::try_from(row.event_version).map_err(backend)?,
                &row.payload,
                &*self.upcaster,
            )?;
            apply(unsigned(row.sequence)?, event);
            count += 1;
        }
        Ok(count)
    }
}

/// Appends on its own: one statement on the pool (an implicit transaction)
/// without a condition, a transaction of its own with one.
impl<E, U> AppendEvents<E> for PostgresEventStore<E, U>
where
    E: DomainEvent + Serialize,
    U: Upcaster,
{
    async fn append_tagged(
        &self,
        (): &mut (),
        stream: &StreamId,
        expected: ExpectedVersion,
        events: &[TaggedEvent<E>],
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> Result<Vec<Recorded>, StoreError> {
        let Some(condition) = condition else {
            // `Any` never conflicts: a concurrent append took the sequence
            // this one computed, so it goes again at the new end.
            let mut attempts = 0;
            let rows = loop {
                let rows = append_rows(&self.pool, &self.log, stream, expected, events, metadata).await;
                attempts += 1;
                match rows {
                    Err(StoreError::Conflict { .. }) if expected == ExpectedVersion::Any && attempts < 16 => {}
                    rows => break rows,
                }
            };
            return recorded(rows);
        };
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // An error drops `tx`: rolled back, the locks released.
        let committed =
            recorded(append_conditional(&mut tx, &self.log, stream, expected, events, metadata, condition).await)?;
        tx.commit().await.map_err(backend)?;
        Ok(committed)
    }
}

/// Appends in the caller's transaction, so its own writes (an aggregate's
/// state, an outbox row) and the events commit together. Nothing is visible
/// before the caller commits; readers wait for its positions (they are gaps
/// until then).
///
/// A conflict aborts the transaction, as any failed statement does in
/// PostgreSQL: roll it back and decide again. A condition's locks are held
/// until it ends, so keep a transaction with a conditional append short.
impl<'c, E, U> AppendEvents<E, Transaction<'c, Postgres>> for PostgresEventStore<E, U>
where
    E: DomainEvent + Serialize,
    U: Upcaster,
{
    async fn append_tagged(
        &self,
        tx: &mut Transaction<'c, Postgres>,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: &[TaggedEvent<E>],
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> Result<Vec<Recorded>, StoreError> {
        let rows = match condition {
            Some(condition) => append_conditional(tx, &self.log, stream, expected, events, metadata, condition).await,
            // `Any` goes again at the new end after a lost race, in a
            // savepoint so the race does not abort the caller's transaction.
            None if expected == ExpectedVersion::Any => {
                let mut attempts = 0;
                loop {
                    let mut savepoint = tx.begin().await.map_err(backend)?;
                    let rows = append_rows(&mut *savepoint, &self.log, stream, expected, events, metadata).await;
                    attempts += 1;
                    match rows {
                        Err(StoreError::Conflict { .. }) if attempts < 16 => {}
                        Ok(rows) => {
                            savepoint.commit().await.map_err(backend)?;
                            break Ok(rows);
                        }
                        rows => break rows,
                    }
                }
            }
            None => append_rows(&mut **tx, &self.log, stream, expected, events, metadata).await,
        };
        recorded(rows)
    }
}

impl<E, U> SourceEvents<E> for PostgresEventStore<E, U>
where
    E: DeserializeOwned + Send + Sync,
    U: Upcaster,
{
    async fn source(&self, criteria: &EventCriteria) -> Result<(Vec<EventEnvelope<E>>, u64), StoreError> {
        // The criteria's keys exclusive first: no matching append is
        // half-way, so every matching event is committed before the reads,
        // and any later one takes a position above the head read here.
        let exclusive = criteria_keys(&self.log, criteria)?;
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query!("SELECT cqrs_lock($1, '{}')", &exclusive)
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;
        let mut rows = Vec::new();
        for criterion in &criteria.criteria {
            let (tags, types) = bind_criterion(criterion);
            let matching = if tags.is_empty() {
                // shortcut: no index on event_type, so this reads the whole
                // log; add one if tag-less criteria become common.
                select_events!(
                    &*self.log,
                    "(cardinality($2::text[]) = 0 OR event_type = ANY($2)) ORDER BY position",
                    &types
                )
                .fetch_all(&mut *tx)
                .await
            } else {
                // Positions of the events carrying every tag: walks the first
                // tag's entries (an index range, in position order) and checks
                // the others by primary key.
                select_events!(
                    &*self.log,
                    "position IN (SELECT t.position FROM cqrs_event_tags t WHERE t.log = $1 \
                     AND t.tag = ($2::text[])[1] AND (SELECT count(*) FROM cqrs_event_tags o \
                     WHERE o.log = $1 AND o.position = t.position AND o.tag = ANY($2)) = cardinality($2)) "
                    "AND (cardinality($3::text[]) = 0 OR event_type = ANY($3)) ORDER BY position",
                    &tags,
                    &types,
                )
                .fetch_all(&mut *tx)
                .await
            };
            rows.extend(matching.map_err(backend)?);
        }
        let head = head(&mut *tx, &self.log).await?;
        tx.commit().await.map_err(backend)?;

        let mut events = rows
            .into_iter()
            .map(|row| envelope(row, &*self.upcaster))
            .collect::<Result<Vec<_>, _>>()?;
        // Several criteria may match the same event.
        events.sort_unstable_by_key(|event| event.position);
        events.dedup_by_key(|event| event.position);
        Ok((events, head))
    }
}

impl<E, U> ReadLog<E> for PostgresEventStore<E, U>
where
    E: DeserializeOwned + Send + Sync,
    U: Upcaster,
{
    async fn read_all(&self, after: u64, limit: usize) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let rows = select_events!(
            &*self.log,
            "position > $2 ORDER BY position LIMIT $3",
            to_i64(after),
            i64::try_from(limit).unwrap_or(i64::MAX),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter().map(|row| envelope(row, &*self.upcaster)).collect()
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        head(&self.pool, &self.log).await
    }

    /// The cursor's gaps that were committed since, then what follows it,
    /// with the snapshot they were read in: transactions older than its
    /// `xmin` finished, and whoever holds a position missing among those
    /// read started before its `xmax` (it took a transaction id before its
    /// positions, see `cqrs_positions`).
    ///
    /// The positions skipped up to the last event read that **no log**
    /// committed in that snapshot come back as
    /// [`missing`](cqrs::store::LogPage::missing): only those can still be filled.
    /// The other logs' positions, missing from this log's rows, are not
    /// taken for gaps (risk 1.2 of the migration plan: with two logs under
    /// load, a processor's cursor carried a hundred gaps or more).
    async fn read(&self, cursor: &Cursor, limit: usize) -> Result<LogPage<E>, StoreError> {
        let gaps: Vec<i64> = cursor.gaps().iter().map(|gap| to_i64(gap.position)).collect();
        let rows = sqlx::query!(
            r#"
            WITH snapshot AS (
                SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS xmin,
                       pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS xmax
            ),
            page AS (
                (SELECT position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata,
                        (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                 FROM cqrs_events WHERE log = $4 AND position = ANY($2))
                UNION ALL
                (SELECT position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata,
                        (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                 FROM cqrs_events WHERE log = $4 AND position > $1 ORDER BY position LIMIT $3)
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
                   e.position AS "position?", e.stream_type AS "stream_type?", e.stream_id AS "stream_id?",
                   e.sequence AS "sequence?", e.event_type AS "event_type?", e.event_version AS "event_version?",
                   e.payload::text AS "payload?", e.metadata::text AS "metadata?", e.recorded_us AS "recorded_us?"
            FROM snapshot s CROSS JOIN missing m LEFT JOIN page e ON true"#,
            to_i64(cursor.position()),
            &gaps,
            i64::try_from(limit).unwrap_or(i64::MAX),
            &*self.log,
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
                Some(EventRow {
                    position: row.position?,
                    stream_type: row.stream_type?,
                    stream_id: row.stream_id?,
                    sequence: row.sequence?,
                    event_type: row.event_type?,
                    event_version: row.event_version?,
                    payload: row.payload?,
                    metadata: row.metadata?,
                    recorded_us: row.recorded_us?,
                })
            })
            .map(|row| envelope(row, &*self.upcaster))
            .collect::<Result<_, _>>()?;
        Ok(LogPage {
            events,
            horizon,
            missing: Some(missing),
        })
    }

    /// `position`, with the positions below it not committed yet (within
    /// the last `CURSOR_WINDOW` positions) as gaps, unless every transaction that
    /// could still commit them already finished.
    async fn cursor_at(&self, position: u64) -> Result<Cursor, StoreError> {
        let row = sqlx::query!(
            r#"
            SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS "xmin!",
                   pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS "xmax!",
                   array(SELECT p FROM generate_series(greatest($1::bigint - $2::bigint, 0) + 1, $1::bigint) AS p
                         WHERE NOT EXISTS (SELECT 1 FROM cqrs_events WHERE position = p)) AS "missing!""#,
            to_i64(position),
            CURSOR_WINDOW,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        let horizon = Horizon {
            finished_below: unsigned(row.xmin)?,
            started_below: unsigned(row.xmax)?,
        };
        let gaps = row
            .missing
            .into_iter()
            .map(|missing| {
                Ok(Gap {
                    position: unsigned(missing)?,
                    horizon: horizon.started_below,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let mut cursor = Cursor::new(position, gaps);
        cursor.settle(&horizon, |_| false);
        Ok(cursor)
    }
}

/// A conditional append on `conn` (in a transaction): the criteria's keys
/// (and the stream's) exclusive, in a statement of their own, so the check
/// runs on a snapshot taken after the wait and no matching append is
/// half-way while it runs; then the check, then the append. The locks are
/// held until the transaction ends.
async fn append_conditional<E: DomainEvent + Serialize>(
    conn: &mut PgConnection,
    log: &str,
    stream: &StreamId,
    expected: ExpectedVersion,
    events: &[TaggedEvent<E>],
    metadata: &Metadata,
    condition: &AppendCondition,
) -> Result<Vec<Appended>, StoreError> {
    let mut exclusive = criteria_keys(log, &condition.criteria)?;
    exclusive.push(key(log, "stream", &stream.to_string()));
    sqlx::query!("SELECT cqrs_lock($1, $2)", &exclusive, &write_keys(log, events))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    for criterion in &condition.criteria.criteria {
        let (tags, types) = bind_criterion(criterion);
        let failed = condition_failed(conn, log, &tags, &types, to_i64(condition.after))
            .await
            .map_err(backend)?;
        if failed {
            return Err(StoreError::ConditionFailed);
        }
    }
    append_rows(conn, log, stream, expected, events, metadata).await
}

/// How far below a starting position [`ReadLog::cursor_at`] looks for
/// appends still being written.
// shortcut: an append still uncommitted after this many later positions
// were taken is missed by a subscription starting "from now" (Axon's head
// token misses them all); a stored cursor never misses it.
const CURSOR_WINDOW: i64 = 10_000;

/// The highest committed position of the log `log` (0 when empty).
async fn head<'c>(executor: impl PgExecutor<'c>, log: &str) -> Result<u64, StoreError> {
    let head = sqlx::query_scalar!(
        r#"SELECT coalesce(max(position), 0) AS "head!" FROM cqrs_events WHERE log = $1"#,
        log
    )
    .fetch_one(executor)
    .await
    .map_err(backend)?;
    unsigned(head)
}

/// A row as an envelope, upcast to the current shape.
fn envelope<E: DeserializeOwned>(row: EventRow, upcaster: &impl Upcaster) -> Result<EventEnvelope<E>, StoreError> {
    let (payload, event_type, event_version) = StoredEvent::decode_text(
        row.event_type,
        u32::try_from(row.event_version).map_err(backend)?,
        &row.payload,
        upcaster,
    )?;
    Ok(EventEnvelope {
        position: unsigned(row.position)?,
        stream: StreamId::new(row.stream_type, row.stream_id),
        sequence: unsigned(row.sequence)?,
        recorded_at: from_micros(row.recorded_us),
        metadata: serde_json::from_str(&row.metadata).map_err(backend)?,
        payload,
        event_type: event_type.into(),
        event_version,
    })
}

/// What an append returns per event.
struct Appended {
    position: i64,
    sequence: i64,
    recorded_us: i64,
}

/// One append, in one statement (one round trip, an implicit transaction),
/// in parallel with any other:
///
/// 1. `current` is the stream's last sequence; `allowed` keeps it only if it
///    is the expected one (`$3`; NULL: no check).
/// 2. `positions` asks `cqrs_positions` for one position per event, only if
///    allowed. It first takes the events' keys shared (`$9`: their tags and
///    types, see [`write_keys`]), so it waits for a criterion being read or
///    checked exclusively, then a transaction id, then the positions.
/// 3. The events are inserted from the `$5` JSON array, in order, with the
///    next sequences; their tags go into `cqrs_event_tags` (`$7`/`$8`: tag
///    and 1-based event index, computed in Rust: deduplicating in SQL made
///    the planner estimate millions of rows and JIT-compile every append).
///
/// Refused check: nothing is inserted, no rows come back. A concurrent
/// append to the same stream can pass the check too: then the insert hits
/// `UNIQUE (stream_type, stream_id, sequence)` and the statement rolls back.
/// That is the same conflict, reported the same way; the positions it took
/// are gaps readers give up once its transaction is over.
///
/// Runs on `executor` (the pool, or a transaction).
async fn append_rows<'c, E: DomainEvent + Serialize>(
    executor: impl PgExecutor<'c>,
    log: &str,
    stream: &StreamId,
    expected: ExpectedVersion,
    events: &[TaggedEvent<E>],
    metadata: &Metadata,
) -> Result<Vec<Appended>, StoreError> {
    let expected_version = match expected {
        ExpectedVersion::NoStream => Some(0),
        ExpectedVersion::Exact(version) => Some(to_i64(version)),
        ExpectedVersion::Any => None,
    };
    let batch = serde_json::to_string(&Batch(events)).map_err(backend)?;
    let metadata = serde_json::to_string(metadata).map_err(backend)?;
    let keys = write_keys(log, events);
    let tags = EventTags::of(events);
    let result = sqlx::query!(
        r#"
        WITH current AS (
            SELECT coalesce(max(sequence), 0) AS version FROM cqrs_events
            WHERE log = $10 AND stream_type = $1 AND stream_id = $2
        ),
        allowed AS (
            SELECT version FROM current WHERE $3::bigint IS NULL OR $3 = version
        ),
        positions AS (
            SELECT p.position, p.n
            FROM allowed, LATERAL cqrs_positions(CASE WHEN allowed.version >= 0 THEN $4::integer END, $9::bigint[])
                WITH ORDINALITY AS p(position, n)
        ),
        tag_index AS (
            INSERT INTO cqrs_event_tags (log, tag, position)
            SELECT $10, t.tag, positions.position
            FROM unnest($7::text[], $8::bigint[]) AS t(tag, n) JOIN positions ON positions.n = t.n
        ),
        inserted AS (
            INSERT INTO cqrs_events
                (log, position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata)
            SELECT $10, positions.position, $1, $2, allowed.version + batch.n,
                   batch.item->>'type', (batch.item->>'version')::integer, batch.item->'payload', $6::text::json
            FROM allowed, positions, json_array_elements($5::text::json) WITH ORDINALITY AS batch(item, n)
            WHERE positions.n = batch.n
            RETURNING position, sequence, recorded_at
        )
        SELECT position AS "position?", sequence AS "sequence?",
               (extract(epoch FROM recorded_at) * 1000000)::bigint AS "recorded_us?", NULL::bigint AS "actual?"
        FROM inserted
        UNION ALL
        SELECT NULL, NULL, NULL, version FROM current WHERE NOT EXISTS (SELECT 1 FROM allowed)"#,
        stream.aggregate_type(),
        stream.id(),
        expected_version,
        i32::try_from(events.len()).unwrap_or(i32::MAX),
        batch,
        metadata,
        &tags.tags,
        &tags.events,
        &keys,
        log,
    )
    .fetch_all(executor)
    .await;
    let conflict = |actual: Option<i64>| StoreError::Conflict {
        stream: stream.clone(),
        expected,
        // An empty stream has no version.
        actual: actual
            .and_then(|version| u64::try_from(version).ok())
            .filter(|&version| version > 0),
    };
    match result {
        // The version check refused the append: the row says what the
        // stream is at.
        Ok(rows) if rows.first().is_some_and(|row| row.actual.is_some()) => Err(conflict(rows[0].actual)),
        Ok(rows) if rows.is_empty() => Err(conflict(None)),
        Ok(rows) => Ok(rows
            .into_iter()
            .filter_map(|row| {
                Some(Appended {
                    position: row.position?,
                    sequence: row.sequence?,
                    recorded_us: row.recorded_us?,
                })
            })
            .collect()),
        // A concurrent append to this stream won (see above): its version
        // is not known without another read, and only informative.
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23505") => Err(conflict(None)),
        Err(error) => Err(backend(error)),
    }
}

/// A batch as the append binds it (`$5`): a JSON array of
/// `{"type", "version", "payload"}`, written straight from the events, with
/// no intermediate `serde_json::Value`.
struct Batch<'a, E>(&'a [TaggedEvent<E>]);

impl<E: DomainEvent + Serialize> Serialize for Batch<'_, E> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        /// One event of the batch.
        #[derive(Serialize)]
        struct Item<'a, E> {
            #[serde(rename = "type")]
            event_type: &'a str,
            version: u32,
            payload: &'a E,
        }
        serializer.collect_seq(self.0.iter().map(|TaggedEvent { event, .. }| Item {
            event_type: event.event_type(),
            version: event.event_version(),
            payload: event,
        }))
    }
}

/// The tags of a batch (resolved by the core, distinct per event), as the
/// append binds them: one entry per (tag, event). Events are numbered from
/// 1, as `WITH ORDINALITY` numbers them.
#[derive(Debug, Default, PartialEq)]
struct EventTags {
    /// `$7`: each event's distinct tags...
    tags: Vec<String>,
    /// `$8`: ...and the event they are on.
    events: Vec<i64>,
}

impl EventTags {
    fn of<E>(batch: &[TaggedEvent<E>]) -> Self {
        let mut pairs = Self::default();
        for (index, event) in (1..).zip(batch) {
            for tag in &event.tags {
                pairs.tags.push(tag.to_string());
                pairs.events.push(index);
            }
        }
        pairs
    }
}

/// An advisory lock key for a piece of the domain of a log: FNV-1a of
/// `log:kind:value`, stable across processes. Two pieces sharing a key only
/// wait for each other more than they need to; logs never share one.
fn key(log: &str, kind: &str, value: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in log
        .bytes()
        .chain(*b":")
        .chain(kind.bytes())
        .chain(*b":")
        .chain(value.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    // The hash's bits, as the `bigint` an advisory lock takes.
    i64::from_ne_bytes(hash.to_ne_bytes())
}

/// What an append of `events` takes shared: each of their tags and types,
/// the pieces of the domain a criterion can name.
fn write_keys<E: DomainEvent>(log: &str, events: &[TaggedEvent<E>]) -> Vec<i64> {
    let mut keys: Vec<i64> = events
        .iter()
        .flat_map(|TaggedEvent { event, tags }| {
            let tags = tags.iter().map(|tag| key(log, "tag", &tag.to_string()));
            tags.chain([key(log, "type", event.event_type())])
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// What reading or checking `criteria` takes exclusive: one tag of each
/// criterion with tags (every matching append holds it shared), or each
/// type of one without (every append of those types holds it shared).
fn criteria_keys(log: &str, criteria: &EventCriteria) -> Result<Vec<i64>, StoreError> {
    criteria
        .criteria
        .iter()
        .map(|criterion| match (criterion.tags.first(), criterion.types.is_empty()) {
            (Some(tag), _) => Ok(vec![key(log, "tag", &tag.to_string())]),
            (None, false) => Ok(criterion.types.iter().map(|name| key(log, "type", name)).collect()),
            (None, true) => Err(StoreError::backend("a criterion must name a tag or an event type")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|keys| keys.concat())
}

/// A criterion as the queries bind it: its distinct tags and its types.
fn bind_criterion(criterion: &cqrs::event::Criterion) -> (Vec<String>, Vec<String>) {
    let mut tags: Vec<String> = criterion.tags.iter().map(ToString::to_string).collect();
    tags.sort_unstable();
    tags.dedup();
    (tags, criterion.types.iter().map(|&name| name.to_owned()).collect())
}

/// Whether an event of the log `$4` matching the criterion (tags `$1`, types
/// `$2`) was appended after `$3`. Runs holding the criterion's keys exclusive, so
/// every matching append is committed (and took a position above the head
/// its decision was sourced at) or has not started.
async fn condition_failed(
    tx: &mut sqlx::PgConnection,
    log: &str,
    tags: &[String],
    types: &[String],
    after: i64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar!(
        r#"SELECT CASE
             WHEN cardinality($1::text[]) = 0 THEN EXISTS (
                 SELECT 1 FROM cqrs_events WHERE log = $4 AND position > $3
                 AND (cardinality($2::text[]) = 0 OR event_type = ANY($2)))
             ELSE EXISTS (
                 SELECT 1 FROM cqrs_events WHERE log = $4 AND position IN (
                     SELECT t.position FROM cqrs_event_tags t
                     WHERE t.log = $4 AND t.tag = ($1)[1] AND t.position > $3
                     AND (SELECT count(*) FROM cqrs_event_tags o
                          WHERE o.log = $4 AND o.position = t.position AND o.tag = ANY($1))
                         = cardinality($1))
                 AND (cardinality($2) = 0 OR event_type = ANY($2)))
           END AS "failed!""#,
        tags,
        types,
        after,
        log,
    )
    .fetch_one(tx)
    .await
}

impl<E, U> PostgresEventStore<E, U> {
    /// The pool, for the stores that share this one's database.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl<E, U> PostgresEventStore<E, U>
where
    E: DeserializeOwned,
    U: Upcaster,
{
    /// The event of this log at `position`, if there is one.
    pub(crate) async fn event_at(&self, position: u64) -> Result<Option<EventEnvelope<E>>, StoreError> {
        let row = select_events!(&*self.log, "position = $2", to_i64(position))
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        row.map(|row| envelope(row, &*self.upcaster)).transpose()
    }
}

/// What the rows an append returned record, in event order.
fn recorded(rows: Result<Vec<Appended>, StoreError>) -> Result<Vec<Recorded>, StoreError> {
    let mut rows = rows?;
    rows.sort_by_key(|row| row.position);
    rows.into_iter()
        .map(|row| {
            Ok(Recorded {
                position: unsigned(row.position)?,
                sequence: unsigned(row.sequence)?,
                recorded_at: from_micros(row.recorded_us),
            })
        })
        .collect()
}

/// Positions and sequences are stored as `bigint`.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn from_micros(micros: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(micros).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use cqrs::event::Tag;

    use super::*;

    struct Tagged(Vec<Tag>);

    impl DomainEvent for Tagged {
        fn event_type(&self) -> &'static str {
            "tagged"
        }
    }

    impl cqrs::event::TagResolver for Tagged {
        fn tags(&self) -> Vec<Tag> {
            self.0.clone()
        }
    }

    #[test]
    fn a_batch_binds_each_tag_per_event() {
        let batch = [
            Tagged(vec![Tag::new("room", 1), Tag::new("room", 1), Tag::new("floor", 2)]),
            Tagged(vec![]),
            Tagged(vec![Tag::new("room", 1)]),
        ];
        let batch: Vec<_> = batch.into_iter().map(TaggedEvent::new).collect();
        assert_eq!(
            EventTags::of(&batch),
            EventTags {
                tags: vec!["floor=2".into(), "room=1".into(), "room=1".into()],
                events: vec![1, 1, 3],
            }
        );
    }

    #[test]
    fn an_append_locks_its_tags_and_types_and_a_criterion_one_tag_or_its_types() {
        let batch = [
            Tagged(vec![Tag::new("room", 1)]),
            Tagged(vec![Tag::new("room", 1), Tag::new("floor", 2)]),
        ];
        let mut expected = vec![
            key("log", "tag", "room=1"),
            key("log", "tag", "floor=2"),
            key("log", "type", "tagged"),
        ];
        expected.sort_unstable();
        let batch: Vec<_> = batch.into_iter().map(TaggedEvent::new).collect();
        assert_eq!(write_keys("log", &batch), expected);
        assert_ne!(write_keys("other", &batch), expected, "logs never share a lock key");
        let criteria = EventCriteria {
            criteria: vec![
                cqrs::event::Criterion {
                    types: vec![],
                    tags: vec![Tag::new("room", 1), Tag::new("floor", 2)],
                },
                cqrs::event::Criterion {
                    types: vec!["a", "b"],
                    tags: vec![],
                },
            ],
        };
        let keys = criteria_keys("log", &criteria).unwrap();
        assert_eq!(keys.len(), 3, "one tag, then both types");
        assert!(
            criteria_keys(
                "log",
                &EventCriteria {
                    criteria: vec![cqrs::event::Criterion::default()]
                }
            )
            .is_err()
        );
    }
}

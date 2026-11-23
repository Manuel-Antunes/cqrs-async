use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cqrs::{
    AppendCondition, Cursor, DomainEvent, EventCriteria, EventEnvelope, EventStore, ExpectedVersion, Gap, Horizon,
    Metadata, Recorded, StoreError, StreamId, TaggedEvent,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgExecutor, PgPool};

use crate::{StoredEvent, Upcaster, backend};

/// The event log in PostgreSQL, events stored as JSON (`serde`), read
/// through the [`Upcaster`]s `U` (none by default).
///
/// Cloning shares the pool: give a clone to the repository, the processors
/// and the subscriptions (`Events::new(store)` does).
pub struct PostgresEventStore<E, U = ()> {
    /// The connection pool.
    pool: PgPool,
    /// Rewrites old payloads before decoding.
    upcaster: Arc<U>,
    /// The event type this store reads and writes.
    event: PhantomData<fn() -> E>,
}

impl<E> PostgresEventStore<E> {
    /// A store over `pool`, whose schema [`migrate`](crate::migrate) created.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            upcaster: Arc::new(()),
            event: PhantomData,
        }
    }
}

impl<E, U> PostgresEventStore<E, U> {
    /// Runs `upcasters` (one [`Upcaster`], or a tuple of them, oldest step
    /// first) on every event read, instead of the current ones.
    pub fn with_upcasters<U2: Upcaster>(self, upcasters: U2) -> PostgresEventStore<E, U2> {
        PostgresEventStore {
            pool: self.pool,
            upcaster: Arc::new(upcasters),
            event: PhantomData,
        }
    }
}

impl<E, U> Clone for PostgresEventStore<E, U> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
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
    payload: serde_json::Value,
    metadata: serde_json::Value,
    recorded_us: i64,
}

/// `sqlx::query_as!` reading [`EventRow`]s from `cqrs_events` filtered by
/// `$where` (string literals, joined with `+`): every statement is checked
/// against the schema at compile time, and `query!` takes literals only, so
/// the shared column list is spliced in here rather than with `concat!`.
macro_rules! select_events {
    ($($where:literal)+, $($arg:expr),* $(,)?) => {
        sqlx::query_as!(
            EventRow,
            "SELECT position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata, \
             (extract(epoch FROM recorded_at) * 1000000)::bigint AS \"recorded_us!\" FROM cqrs_events WHERE "
            $(+ $where)+,
            $($arg),*
        )
    };
}

impl<E, U> EventStore<E> for PostgresEventStore<E, U>
where
    E: DomainEvent + Serialize + DeserializeOwned,
    U: Upcaster,
{
    async fn load(&self, stream: &StreamId, after: u64) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let rows = select_events!(
            "stream_type = $1 AND stream_id = $2 AND sequence > $3 ORDER BY sequence",
            stream.aggregate_type(),
            stream.id(),
            to_i64(after),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter().map(|row| envelope(row, &*self.upcaster)).collect()
    }

    async fn append_tagged(
        &self,
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
                let rows = append_rows(&self.pool, stream, expected, events, metadata).await;
                attempts += 1;
                match rows {
                    Err(StoreError::Conflict { .. }) if expected == ExpectedVersion::Any && attempts < 16 => {}
                    rows => break rows,
                }
            };
            return self.recorded(rows, stream, expected).await;
        };

        // The criteria's keys (and the stream's) exclusive, in a statement of
        // their own: the check then runs on a snapshot taken after the wait,
        // and no matching append is half-way while it runs.
        let mut exclusive = criteria_keys(&condition.criteria)?;
        exclusive.push(key("stream", &stream.to_string()));
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query!("SELECT cqrs_lock($1, $2)", &exclusive, &write_keys(events))
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;
        for criterion in &condition.criteria.criteria {
            let (tags, types) = bind_criterion(criterion);
            let failed = condition_failed(&mut tx, &tags, &types, to_i64(condition.after)).await.map_err(backend)?;
            if failed {
                return Err(StoreError::ConditionFailed);
            }
        }
        let rows = append_rows(&mut *tx, stream, expected, events, metadata).await;
        if let Err(StoreError::Conflict { .. }) = rows {
            drop(tx); // roll back and unlock before reading the version
            return Err(self.conflict(stream, expected).await);
        }
        let committed = self.recorded(rows, stream, expected).await?;
        tx.commit().await.map_err(backend)?;
        Ok(committed)
    }

    async fn source(&self, criteria: &EventCriteria) -> Result<(Vec<EventEnvelope<E>>, u64), StoreError> {
        // The criteria's keys exclusive first: no matching append is
        // half-way, so every matching event is committed before the reads,
        // and any later one takes a position above the head read here.
        let exclusive = criteria_keys(criteria)?;
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query!("SELECT cqrs_lock($1, '{}')", &exclusive).fetch_one(&mut *tx).await.map_err(backend)?;
        let mut rows = Vec::new();
        for criterion in &criteria.criteria {
            let (tags, types) = bind_criterion(criterion);
            let matching = if tags.is_empty() {
                // shortcut: no index on event_type, so this reads the whole
                // log; add one if tag-less criteria become common.
                select_events!("(cardinality($1::text[]) = 0 OR event_type = ANY($1)) ORDER BY position", &types)
                    .fetch_all(&mut *tx)
                    .await
            } else {
                // Positions of the events carrying every tag: walks the first
                // tag's entries (an index range, in position order) and checks
                // the others by primary key.
                select_events!(
                    "position IN (SELECT t.position FROM cqrs_event_tags t WHERE t.tag = ($1::text[])[1] \
                     AND (SELECT count(*) FROM cqrs_event_tags o WHERE o.position = t.position AND o.tag = ANY($1)) \
                     = cardinality($1)) "
                    "AND (cardinality($2::text[]) = 0 OR event_type = ANY($2)) ORDER BY position",
                    &tags,
                    &types,
                )
                .fetch_all(&mut *tx)
                .await
            };
            rows.extend(matching.map_err(backend)?);
        }
        let head = head(&mut *tx).await?;
        tx.commit().await.map_err(backend)?;

        let mut events = rows.into_iter().map(|row| envelope(row, &*self.upcaster)).collect::<Result<Vec<_>, _>>()?;
        // Several criteria may match the same event.
        events.sort_unstable_by_key(|event| event.position);
        events.dedup_by_key(|event| event.position);
        Ok((events, head))
    }

    async fn read_all(&self, after: u64, limit: usize) -> Result<Vec<EventEnvelope<E>>, StoreError> {
        let rows = select_events!(
            "position > $1 ORDER BY position LIMIT $2",
            to_i64(after),
            i64::try_from(limit).unwrap_or(i64::MAX),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter().map(|row| envelope(row, &*self.upcaster)).collect()
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        head(&self.pool).await
    }

    /// The cursor's gaps that were committed since, then what follows it,
    /// with the snapshot they were read in: transactions older than its
    /// `xmin` finished, and whoever holds a position missing among those
    /// read started before its `xmax` (it took a transaction id before its
    /// positions, see `cqrs_positions`).
    async fn read(&self, cursor: &Cursor, limit: usize) -> Result<(Vec<EventEnvelope<E>>, Horizon), StoreError> {
        let gaps: Vec<i64> = cursor.gaps().iter().map(|gap| to_i64(gap.position)).collect();
        let rows = sqlx::query!(
            r#"
            WITH snapshot AS (
                SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS xmin,
                       pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS xmax
            )
            SELECT s.xmin AS "xmin!", s.xmax AS "xmax!",
                   e.position AS "position?", e.stream_type AS "stream_type?", e.stream_id AS "stream_id?",
                   e.sequence AS "sequence?", e.event_type AS "event_type?", e.event_version AS "event_version?",
                   e.payload AS "payload?", e.metadata AS "metadata?", e.recorded_us AS "recorded_us?"
            FROM snapshot s LEFT JOIN LATERAL (
                (SELECT position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata,
                        (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                 FROM cqrs_events WHERE position = ANY($2))
                UNION ALL
                (SELECT position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata,
                        (extract(epoch FROM recorded_at) * 1000000)::bigint AS recorded_us
                 FROM cqrs_events WHERE position > $1 ORDER BY position LIMIT $3)
                ORDER BY position LIMIT $3
            ) e ON true"#,
            to_i64(cursor.position()),
            &gaps,
            i64::try_from(limit).unwrap_or(i64::MAX),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let horizon = rows.first().map_or(Horizon::SETTLED, |row| Horizon {
            finished_below: row.xmin as u64,
            started_below: row.xmax as u64,
        });
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
        Ok((events, horizon))
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
        let horizon = Horizon { finished_below: row.xmin as u64, started_below: row.xmax as u64 };
        let gaps = row.missing.into_iter().map(|missing| Gap { position: missing as u64, horizon: horizon.started_below });
        let mut cursor = Cursor::new(position, gaps);
        cursor.settle(&horizon, |_| false);
        Ok(cursor)
    }
}

/// How far below a starting position [`EventStore::cursor_at`] looks for
/// appends still being written.
// shortcut: an append still uncommitted after this many later positions
// were taken is missed by a subscription starting "from now" (Axon's head
// token misses them all); a stored cursor never misses it.
const CURSOR_WINDOW: i64 = 10_000;

/// The highest committed position (0 when empty).
async fn head<'c>(executor: impl PgExecutor<'c>) -> Result<u64, StoreError> {
    let head = sqlx::query_scalar!(r#"SELECT coalesce(max(position), 0) AS "head!" FROM cqrs_events"#)
        .fetch_one(executor)
        .await
        .map_err(backend)?;
    Ok(head as u64)
}

/// A row as an envelope, upcast to the current shape.
fn envelope<E: DeserializeOwned>(row: EventRow, upcaster: &impl Upcaster) -> Result<EventEnvelope<E>, StoreError> {
    let stored = StoredEvent { event_type: row.event_type, version: row.event_version as u32, payload: row.payload };
    let (payload, event_type, event_version) = stored.decode(upcaster)?;
    Ok(EventEnvelope {
        position: row.position as u64,
        stream: StreamId::new(row.stream_type, row.stream_id),
        sequence: row.sequence as u64,
        recorded_at: from_micros(row.recorded_us),
        metadata: serde_json::from_value(row.metadata).map_err(backend)?,
        payload,
        event_type,
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
    let batch = events
        .iter()
        .map(|TaggedEvent { event, .. }| {
            let stored = StoredEvent::encode(event)?;
            Ok(serde_json::json!({ "type": stored.event_type, "version": stored.version, "payload": stored.payload }))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let keys = write_keys(events);
    let tags = EventTags::of(events);
    let result = sqlx::query_as!(
        Appended,
        r#"
        WITH current AS (
            SELECT coalesce(max(sequence), 0) AS version FROM cqrs_events
            WHERE stream_type = $1 AND stream_id = $2
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
            INSERT INTO cqrs_event_tags (tag, position)
            SELECT t.tag, positions.position
            FROM unnest($7::text[], $8::bigint[]) AS t(tag, n) JOIN positions ON positions.n = t.n
        )
        INSERT INTO cqrs_events
            (position, stream_type, stream_id, sequence, event_type, event_version, payload, metadata)
        SELECT positions.position, $1, $2, allowed.version + batch.n,
               batch.item->>'type', (batch.item->>'version')::integer, batch.item->'payload', $6
        FROM allowed, positions, jsonb_array_elements($5) WITH ORDINALITY AS batch(item, n)
        WHERE positions.n = batch.n
        RETURNING position AS "position!", sequence AS "sequence!", (extract(epoch FROM recorded_at) * 1000000)::bigint AS "recorded_us!""#,
        stream.aggregate_type(),
        stream.id(),
        expected_version,
        i32::try_from(events.len()).unwrap_or(i32::MAX),
        serde_json::Value::Array(batch),
        serde_json::to_value(metadata).map_err(backend)?,
        &tags.tags,
        &tags.events,
        &keys,
    )
    .fetch_all(executor)
    .await;
    match result {
        // Nothing inserted: the version check refused the append.
        Ok(rows) if rows.is_empty() => Err(StoreError::Conflict { stream: stream.clone(), expected, actual: None }),
        Ok(rows) => Ok(rows),
        // A concurrent append to this stream won (see above).
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23505") => {
            Err(StoreError::Conflict { stream: stream.clone(), expected, actual: None })
        }
        Err(error) => Err(backend(error)),
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

/// An advisory lock key for a piece of the domain: FNV-1a of `kind:value`,
/// stable across processes. Two pieces sharing a key only wait for each
/// other more than they need to.
fn key(kind: &str, value: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in kind.bytes().chain(*b":").chain(value.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash as i64
}

/// What an append of `events` takes shared: each of their tags and types,
/// the pieces of the domain a criterion can name.
fn write_keys<E: DomainEvent>(events: &[TaggedEvent<E>]) -> Vec<i64> {
    let mut keys: Vec<i64> = events
        .iter()
        .flat_map(|TaggedEvent { event, tags }| {
            let tags = tags.iter().map(|tag| key("tag", &tag.to_string()));
            tags.chain([key("type", event.event_type())])
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// What reading or checking `criteria` takes exclusive: one tag of each
/// criterion with tags (every matching append holds it shared), or each
/// type of one without (every append of those types holds it shared).
fn criteria_keys(criteria: &EventCriteria) -> Result<Vec<i64>, StoreError> {
    criteria
        .criteria
        .iter()
        .map(|criterion| match (criterion.tags.first(), criterion.types.is_empty()) {
            (Some(tag), _) => Ok(vec![key("tag", &tag.to_string())]),
            (None, false) => Ok(criterion.types.iter().map(|name| key("type", name)).collect()),
            (None, true) => Err(StoreError::Backend("a criterion must name a tag or an event type".to_owned())),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|keys| keys.concat())
}

/// A criterion as the queries bind it: its distinct tags and its types.
fn bind_criterion(criterion: &cqrs::Criterion) -> (Vec<String>, Vec<String>) {
    let mut tags: Vec<String> = criterion.tags.iter().map(ToString::to_string).collect();
    tags.sort_unstable();
    tags.dedup();
    (tags, criterion.types.iter().map(|&name| name.to_owned()).collect())
}

/// Whether an event matching the criterion (tags `$1`, types `$2`) was
/// appended after `$3`. Runs holding the criterion's keys exclusive, so
/// every matching append is committed (and took a position above the head
/// its decision was sourced at) or has not started.
async fn condition_failed(
    tx: &mut sqlx::PgConnection,
    tags: &[String],
    types: &[String],
    after: i64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar!(
        r#"SELECT CASE
             WHEN cardinality($1::text[]) = 0 THEN EXISTS (
                 SELECT 1 FROM cqrs_events WHERE position > $3
                 AND (cardinality($2::text[]) = 0 OR event_type = ANY($2)))
             ELSE EXISTS (
                 SELECT 1 FROM cqrs_events WHERE position IN (
                     SELECT t.position FROM cqrs_event_tags t WHERE t.tag = ($1)[1] AND t.position > $3
                     AND (SELECT count(*) FROM cqrs_event_tags o WHERE o.position = t.position AND o.tag = ANY($1))
                         = cardinality($1))
                 AND (cardinality($2) = 0 OR event_type = ANY($2)))
           END AS "failed!""#,
        tags,
        types,
        after,
    )
    .fetch_one(tx)
    .await
}

impl<E, U> PostgresEventStore<E, U> {
    /// What the rows an append returned record, in event order, or its
    /// error (a conflict completed with the stream's actual version).
    async fn recorded(
        &self,
        rows: Result<Vec<Appended>, StoreError>,
        stream: &StreamId,
        expected: ExpectedVersion,
    ) -> Result<Vec<Recorded>, StoreError> {
        let mut rows = match rows {
            Ok(rows) => rows,
            Err(StoreError::Conflict { .. }) => return Err(self.conflict(stream, expected).await),
            Err(error) => return Err(error),
        };
        rows.sort_by_key(|row| row.position);
        Ok(rows
            .into_iter()
            .map(|row| Recorded {
                position: row.position as u64,
                sequence: row.sequence as u64,
                recorded_at: from_micros(row.recorded_us),
            })
            .collect())
    }

    /// The conflict error for a refused append, with the stream's current
    /// version (read after the fact: informative only).
    async fn conflict(&self, stream: &StreamId, expected: ExpectedVersion) -> StoreError {
        let actual = sqlx::query_scalar!(
            "SELECT max(sequence) FROM cqrs_events WHERE stream_type = $1 AND stream_id = $2",
            stream.aggregate_type(),
            stream.id(),
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(None);
        StoreError::Conflict {
            stream: stream.clone(),
            expected,
            actual: actual.map(|sequence| sequence as u64),
        }
    }
}

/// Positions and sequences are stored as `bigint`.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn from_micros(micros: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(micros.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use cqrs::Tag;

    use super::*;

    struct Tagged(Vec<Tag>);

    impl DomainEvent for Tagged {
        fn event_type(&self) -> &'static str {
            "tagged"
        }
    }

    impl cqrs::TagResolver for Tagged {
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
        assert_eq!(EventTags::of(&batch), EventTags {
            tags: vec!["floor=2".into(), "room=1".into(), "room=1".into()],
            events: vec![1, 1, 3],
        });
    }

    #[test]
    fn an_append_locks_its_tags_and_types_and_a_criterion_one_tag_or_its_types() {
        let batch = [Tagged(vec![Tag::new("room", 1)]), Tagged(vec![Tag::new("room", 1), Tag::new("floor", 2)])];
        let mut expected = vec![key("tag", "room=1"), key("tag", "floor=2"), key("type", "tagged")];
        expected.sort_unstable();
        let batch: Vec<_> = batch.into_iter().map(TaggedEvent::new).collect();
        assert_eq!(write_keys(&batch), expected);
        let criteria = EventCriteria {
            criteria: vec![
                cqrs::Criterion { types: vec![], tags: vec![Tag::new("room", 1), Tag::new("floor", 2)] },
                cqrs::Criterion { types: vec!["a", "b"], tags: vec![] },
            ],
        };
        let keys = criteria_keys(&criteria).unwrap();
        assert_eq!(keys.len(), 3, "one tag, then both types");
        assert!(criteria_keys(&EventCriteria { criteria: vec![cqrs::Criterion::default()] }).is_err());
    }
}

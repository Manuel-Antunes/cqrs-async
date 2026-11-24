//! Append-only storage of event streams: the [`EventStore`] contract, its
//! optimistic concurrency check ([`ExpectedVersion`]) and an in-memory
//! implementation.

use crate::sync::Locked;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    future::{Future, ready},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use crate::{AppendCondition, Cursor, DomainEvent, EventCriteria, EventEnvelope, Horizon, Metadata, StreamId, Tag};

/// An event with its tags, resolved once by [`AppendEvents::append_if`]:
/// what a store writes. The tags are sorted and without duplicates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaggedEvent<E> {
    /// The event.
    pub event: E,
    /// Its tags ([`TagResolver`](crate::TagResolver)), sorted, distinct.
    pub tags: Vec<Tag>,
}

impl<E: DomainEvent> TaggedEvent<E> {
    /// `event` with its resolved tags.
    pub fn new(event: E) -> Self {
        let mut tags = event.tags();
        tags.sort_unstable();
        tags.dedup();
        Self { event, tags }
    }
}

/// What a store decided for one appended event: the rest of its envelope
/// comes from the event and the append's arguments
/// ([`AppendEvents::append_if`] builds it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recorded {
    /// Its place in the whole store.
    pub position: u64,
    /// Its place in its stream.
    pub sequence: u64,
    /// When it was committed.
    pub recorded_at: SystemTime,
}

/// The version a writer believes the stream is at: the optimistic
/// concurrency check of [`AppendEvents::append`]. If the stream moved on since
/// the writer read it, the append fails instead of building on stale state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// The stream must not exist (creating an aggregate).
    NoStream,
    /// The stream's last sequence must be exactly this.
    Exact(u64),
    /// No check (only for streams that never race).
    Any,
}

/// Why an event store operation failed.
#[derive(Debug)]
pub enum StoreError {
    /// Optimistic concurrency failure: someone else appended first. The
    /// caller should reload the aggregate and retry the decision.
    Conflict {
        /// The stream that was written to.
        stream: StreamId,
        /// The version the writer expected.
        expected: ExpectedVersion,
        /// The stream's actual last sequence (`None`: it does not exist).
        actual: Option<u64>,
    },
    /// An [`AppendCondition`] failed: an event matching its criteria was
    /// appended after the decision read them. Like a conflict, the caller
    /// should read again and decide again.
    ConditionFailed,
    /// The storage itself failed (connection, I/O, serialization...): the
    /// original error, kept as the [`source`](std::error::Error::source).
    /// A message converts too: `StoreError::Backend("down".into())`.
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

impl StoreError {
    /// A [`Backend`](Self::Backend) failure caused by `error`.
    pub fn backend(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Backend(error.into())
    }

    /// Whether this is a lost race ([`Conflict`](Self::Conflict) or
    /// [`ConditionFailed`](Self::ConditionFailed)): reading again and
    /// deciding again may succeed.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. } | Self::ConditionFailed)
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict {
                stream,
                expected,
                actual,
            } => write!(
                f,
                "concurrency conflict on {stream}: expected {expected:?}, found {actual:?}"
            ),
            Self::ConditionFailed => f.write_str("an event matching the decision's criteria was appended meanwhile"),
            Self::Backend(error) => write!(f, "event store failure: {error}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(error) => Some(&**error),
            Self::Conflict { .. } | Self::ConditionFailed => None,
        }
    }
}

/// Reads one stream: what an aggregate is rebuilt from
/// ([`AggregateLoader`](crate::AggregateLoader)).
pub trait LoadStream<E>: Send + Sync {
    /// Events of `stream` with `sequence > after`, oldest first.
    fn load(
        &self,
        stream: &StreamId,
        after: u64,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send;

    /// Hands `apply` each event of `stream` with `sequence > after`, oldest
    /// first, with its sequence; returns how many there were. What
    /// rebuilding an aggregate needs, and all it needs: a store can read and
    /// decode less than [`load`](Self::load) (no metadata, no envelope, no
    /// list of them). By default, through `load`.
    fn replay<F>(
        &self,
        stream: &StreamId,
        after: u64,
        mut apply: F,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send
    where
        E: Send,
        F: FnMut(u64, E) + Send,
    {
        async move {
            let events = self.load(stream, after).await?;
            let count = events.len() as u64;
            for event in events {
                apply(event.sequence, event.payload);
            }
            Ok(count)
        }
    }
}

/// Appends to streams, atomically per stream, with optimistic concurrency
/// ([`ExpectedVersion`]) and, for stores that index tags, a condition on
/// what a decision read ([`AppendCondition`], Axon 5's DCB): what a
/// publisher needs.
///
/// `Tx` is the transaction the append joins. `()`, the default, means none:
/// the store appends on its own, in a transaction it opens and commits. A
/// database store also appends in a transaction its caller owns (a
/// PostgreSQL store implements it for `sqlx::Transaction`), so an
/// application's own writes and the events commit together; then nothing is
/// visible, nor announced, before the transaction commits (a command's
/// [`UnitOfWork`](crate::UnitOfWork) in a transaction).
///
/// Either every event is stored with consecutive sequences, or none is and
/// [`StoreError::Conflict`] / [`StoreError::ConditionFailed`] /
/// [`StoreError::Backend`] is returned.
pub trait AppendEvents<E: DomainEvent, Tx: Send = ()>: Send + Sync {
    /// Appends `events`, returning their committed envelopes.
    fn append(
        &self,
        tx: &mut Tx,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<E>,
        metadata: &Metadata,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        self.append_if(tx, stream, expected, events, metadata, None)
    }

    /// [`append`](Self::append), and only if `condition` holds: no event
    /// matching its criteria after its position
    /// ([`StoreError::ConditionFailed`] otherwise). The check and the write
    /// are atomic.
    ///
    /// The same for every store, so a store only implements
    /// [`append_tagged`](Self::append_tagged): nothing to append succeeds
    /// at once (no check, no write); otherwise each event's tags are
    /// resolved ([`TaggedEvent::new`]), the store writes them, and the
    /// envelopes are built from what it [`Recorded`].
    fn append_if(
        &self,
        tx: &mut Tx,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: Vec<E>,
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        async move {
            if events.is_empty() {
                return Ok(Vec::new());
            }
            let events: Vec<TaggedEvent<E>> = events.into_iter().map(TaggedEvent::new).collect();
            let recorded = self
                .append_tagged(tx, stream, expected, &events, metadata, condition)
                .await?;
            debug_assert_eq!(recorded.len(), events.len(), "one record per event, in order");
            Ok(events
                .into_iter()
                .zip(recorded)
                .map(|(TaggedEvent { event, .. }, recorded)| EventEnvelope {
                    position: recorded.position,
                    stream: stream.clone(),
                    sequence: recorded.sequence,
                    event_type: Cow::Borrowed(event.event_type()),
                    event_version: event.event_version(),
                    recorded_at: recorded.recorded_at,
                    metadata: metadata.clone(),
                    payload: event,
                })
                .collect())
        }
    }

    /// What a store implements for [`append_if`](Self::append_if): writes
    /// `events` (never empty) with their tags, indexed so criteria find them,
    /// atomically with the version check and `condition`; returns what it
    /// recorded for each, in order.
    fn append_tagged(
        &self,
        tx: &mut Tx,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: &[TaggedEvent<E>],
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> impl Future<Output = Result<Vec<Recorded>, StoreError>> + Send;
}

/// Reads by criteria, from any stream (Axon 5's DCB): what a decision
/// spanning several aggregates, or an aggregate built from tags, reads.
pub trait SourceEvents<E>: Send + Sync {
    /// Every event matching `criteria`, from any stream, in position order,
    /// and the head position they were read at: the `after` of the
    /// [`AppendCondition`] a decision on them appends with.
    fn source(
        &self,
        criteria: &EventCriteria,
    ) -> impl Future<Output = Result<(Vec<EventEnvelope<E>>, u64), StoreError>> + Send;
}

/// One read of the log ([`ReadLog::read`]).
#[derive(Debug)]
pub struct LogPage<E> {
    /// The events, in position order.
    pub events: Vec<EventEnvelope<E>>,
    /// What the store knew when it answered.
    pub horizon: Horizon,
    /// The positions after the cursor's, up to the last event read, that
    /// nothing was committed at when the store answered (in order): the only
    /// ones that may still be filled. `None`: every position skipped may be
    /// (the default, for a store that cannot tell). A store with several
    /// logs over one sequence tells, so the other logs' positions are never
    /// taken for gaps.
    pub missing: Option<Vec<u64>>,
}

/// Reads the whole log in order: what tracking processors and
/// subscriptions follow.
///
/// # Global order
///
/// Every event gets a [`position`](EventEnvelope::position) in the whole
/// store, increasing in the order positions are taken. A store whose
/// appends run one at a time (this crate's in-memory one) has no gaps, and a
/// position is visible only once every lower one is. One whose appends run
/// in parallel (a SQL sequence) commits them out of order: readers of the
/// global log read through a [`Cursor`] that remembers the positions skipped
/// ([`read`](Self::read)), as Axon's gap-aware tracking tokens do.
pub trait ReadLog<E>: Send + Sync {
    /// Up to `limit` events of every stream with `position > after`, in
    /// position order: the global log, read page by page.
    fn read_all(
        &self,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send;

    /// The position of the last committed event (0 when empty).
    fn last_position(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Up to `limit` events `cursor` has not seen, in position order: those
    /// filling its gaps, then those after it; with what the store knew when
    /// it answered, to track the gaps it reveals. By default (a gapless
    /// store), [`read_all`](Self::read_all) after the cursor's position.
    fn read(&self, cursor: &Cursor, limit: usize) -> impl Future<Output = Result<LogPage<E>, StoreError>> + Send {
        let read = self.read_all(cursor.position(), limit);
        async move {
            Ok(LogPage {
                events: read.await?,
                horizon: Horizon::SETTLED,
                missing: None,
            })
        }
    }

    /// A cursor that saw everything committed up to `position`: where a
    /// reader starting "from here" begins. A store committing out of order
    /// marks the positions below it still being written as gaps. By
    /// default, [`Cursor::at`].
    fn cursor_at(&self, position: u64) -> impl Future<Output = Result<Cursor, StoreError>> + Send {
        ready(Ok(Cursor::at(position)))
    }
}

/// Every capability of an event store at once: what the in-memory and the
/// PostgreSQL stores offer, and a shorter bound where all of them are used.
/// Implemented for every type that has them; components ask only for the
/// ones they use ([`LoadStream`], [`AppendEvents`], [`SourceEvents`],
/// [`ReadLog`]), so a store for another database implements what that
/// database can do.
pub trait EventStore<E: DomainEvent>: LoadStream<E> + AppendEvents<E> + SourceEvents<E> + ReadLog<E> {}

impl<E: DomainEvent, S> EventStore<E> for S where S: LoadStream<E> + AppendEvents<E> + SourceEvents<E> + ReadLog<E> {}

/// Event store for tests and examples. `E: Clone` is required here (loading
/// hands out copies), not by the [`EventStore`] contract.
///
/// A handle: cloning it shares the same events, so the publisher, the
/// repositories and the processors each hold a clone.
pub struct InMemoryEventStore<E> {
    /// Everything behind one lock, so an append checks the version and writes
    /// atomically.
    inner: Arc<Mutex<Inner<E>>>,
}

impl<E> Clone for InMemoryEventStore<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// The store's contents.
struct Inner<E> {
    /// Every committed event in position order: the event at position `p` is
    /// `log[p - 1]`, so reading the global log is a slice.
    log: Vec<EventEnvelope<E>>,
    /// Each stream's events, as indices into `log`, in sequence order.
    streams: HashMap<StreamId, Vec<usize>>,
    /// Each event's tags, computed once at append (parallel to `log`).
    tags: Vec<Vec<Tag>>,
    /// The events carrying each tag, as indices into `log`, in order: the
    /// inverted index criteria are read through.
    tagged: HashMap<Tag, Vec<usize>>,
}

impl<E> Inner<E> {
    /// Indices of the events matching `criteria`, in position order, from
    /// index `from` on. A criterion with tags walks the shortest of its
    /// tags' lists instead of the whole log.
    fn matching(&self, criteria: &EventCriteria, from: usize) -> Vec<usize> {
        let mut found: Vec<usize> = Vec::new();
        for criterion in &criteria.criteria {
            let accepts = |index: &usize| {
                let event = &self.log[*index];
                (criterion.types.is_empty() || criterion.types.contains(&&*event.event_type))
                    && criterion.tags.iter().all(|tag| self.tags[*index].contains(tag))
            };
            let shortest = criterion
                .tags
                .iter()
                .map(|tag| self.tagged.get(tag).map_or(&[][..], Vec::as_slice))
                .min_by_key(|list| list.len());
            match shortest {
                Some(list) => {
                    let start = list.partition_point(|index| *index < from);
                    found.extend(list[start..].iter().filter(|index| accepts(index)));
                }
                None => found.extend((from..self.log.len()).filter(accepts)),
            }
        }
        found.sort_unstable();
        found.dedup();
        found
    }
}

impl<E> InMemoryEventStore<E> {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                log: Vec::new(),
                streams: HashMap::new(),
                tags: Vec::new(),
                tagged: HashMap::new(),
            })),
        }
    }

    /// Locks the contents. Appends never panic while holding the lock, so a
    /// poisoned lock is a bug: panic too.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<E>> {
        self.inner.locked()
    }
}

impl<E> Default for InMemoryEventStore<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clone> InMemoryEventStore<E> {
    /// Every committed event in commit order (what a tracking processor
    /// would read).
    pub fn all(&self) -> Vec<EventEnvelope<E>> {
        self.lock().log.clone()
    }
}

impl<E: Clone + Send + Sync> LoadStream<E> for InMemoryEventStore<E> {
    fn load(
        &self,
        stream: &StreamId,
        after: u64,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        let inner = self.lock();
        let events = inner
            .streams
            .get(stream)
            .map(|indices| {
                // Sequences are 1, 2, 3... per stream, so `after` is also
                // how many to skip: a load from a snapshot reads only the tail.
                let skip = usize::try_from(after).unwrap_or(usize::MAX);
                indices
                    .iter()
                    .skip(skip)
                    .map(|&index| inner.log[index].clone())
                    .collect()
            })
            .unwrap_or_default();
        ready(Ok(events))
    }
}

impl<E: DomainEvent + Clone> AppendEvents<E> for InMemoryEventStore<E> {
    fn append_tagged(
        &self,
        (): &mut (),
        stream: &StreamId,
        expected: ExpectedVersion,
        events: &[TaggedEvent<E>],
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> impl Future<Output = Result<Vec<Recorded>, StoreError>> + Send {
        ready(self.append_now(stream, expected, events, metadata, condition))
    }
}

impl<E: Clone + Send + Sync> SourceEvents<E> for InMemoryEventStore<E> {
    fn source(
        &self,
        criteria: &EventCriteria,
    ) -> impl Future<Output = Result<(Vec<EventEnvelope<E>>, u64), StoreError>> + Send {
        let inner = self.lock();
        let matching = inner
            .matching(criteria, 0)
            .into_iter()
            .map(|index| inner.log[index].clone())
            .collect();
        ready(Ok((matching, inner.log.len() as u64)))
    }
}

impl<E: Clone + Send + Sync> ReadLog<E> for InMemoryEventStore<E> {
    fn read_all(
        &self,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        let inner = self.lock();
        let start = usize::try_from(after).map_or(inner.log.len(), |after| after.min(inner.log.len()));
        let end = start.saturating_add(limit).min(inner.log.len());
        ready(Ok(inner.log[start..end].to_vec()))
    }

    fn last_position(&self) -> impl Future<Output = Result<u64, StoreError>> + Send {
        ready(Ok(self.lock().log.len() as u64))
    }
}

impl<E: DomainEvent + Clone> InMemoryEventStore<E> {
    /// The synchronous body of [`AppendEvents::append`]: checks `expected`
    /// against the stream's last sequence, then numbers and stores the events.
    fn append_now(
        &self,
        stream: &StreamId,
        expected: ExpectedVersion,
        events: &[TaggedEvent<E>],
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> Result<Vec<Recorded>, StoreError> {
        let mut inner = self.lock();
        if let Some(condition) = condition {
            let after = usize::try_from(condition.after).unwrap_or(usize::MAX);
            if !inner.matching(&condition.criteria, after).is_empty() {
                return Err(StoreError::ConditionFailed);
            }
        }
        let Inner {
            log,
            streams,
            tags,
            tagged,
        } = &mut *inner;
        let indices = streams.entry(stream.clone()).or_default();
        let actual = indices.last().map(|&index| log[index].sequence);

        let matches = match expected {
            ExpectedVersion::NoStream => actual.is_none(),
            ExpectedVersion::Exact(version) => actual == Some(version),
            ExpectedVersion::Any => true,
        };
        if !matches {
            return Err(StoreError::Conflict {
                stream: stream.clone(),
                expected,
                actual,
            });
        }

        let recorded_at = SystemTime::now();
        let first = (actual.unwrap_or(0) + 1, log.len() as u64 + 1);
        let numbered = (first.0..).zip(first.1..).zip(events);
        let mut recorded = Vec::with_capacity(events.len());
        for (
            (sequence, position),
            TaggedEvent {
                event: payload,
                tags: event_tags,
            },
        ) in numbered
        {
            let index = log.len();
            for tag in event_tags {
                tagged.entry(tag.clone()).or_default().push(index);
            }
            tags.push(event_tags.clone());
            indices.push(index);
            log.push(EventEnvelope {
                position,
                stream: stream.clone(),
                sequence,
                event_type: Cow::Borrowed(payload.event_type()),
                event_version: payload.event_version(),
                recorded_at,
                metadata: metadata.clone(),
                payload: payload.clone(),
            });
            recorded.push(Recorded {
                position,
                sequence,
                recorded_at,
            });
        }
        Ok(recorded)
    }
}

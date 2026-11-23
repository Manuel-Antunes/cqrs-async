//! The event-sourcing engine: [`EventSourced`] replays streams into
//! aggregates, commits their new events with optimistic concurrency and
//! feeds the commits to an [`EventProcessor`].
//!
//! It is not a repository: repositories belong to the application (a port
//! with the finders it needs, as in NestJS), and their adapters load through
//! [`EventSourced::load`]. Command handlers commit through the
//! [`EventPublisher`](crate::EventPublisher) port, which it implements.

use std::{
    any::type_name,
    fmt,
    future::Future,
    marker::PhantomData,
    ops::Deref,
};

use crate::{
    AppendCondition, Aggregate, Appender, DomainEvent, EventCriteria, EventEnvelope, EventHandler, EventPublisher,
    EventStore, ExpectedVersion, HandlerError, Metadata, Pending, SnapshotStore, StoreError, StreamId,
};

/// An aggregate together with the version it was loaded at.
///
/// The version lives here, not in the aggregate: the domain never sees
/// persistence concerns. It derefs to `&A` only: the only way to change the
/// aggregate is to save events, which [`Aggregate::evolve`] applies.
pub struct Loaded<A> {
    /// The aggregate, reachable through `Deref`.
    aggregate: A,
    /// The stream's last sequence when loaded (`None`: not stored yet). It is
    /// the expected version of the next save.
    version: Option<u64>,
    /// For an aggregate built from [`Aggregate::criteria`]: what it was
    /// built from, checked by its saves instead of the version.
    condition: Option<AppendCondition>,
}

impl<A: Aggregate> Loaded<A> {
    /// A brand-new aggregate whose stream must not exist yet.
    pub fn new(aggregate: A) -> Self {
        Self {
            aggregate,
            version: None,
            condition: None,
        }
    }

    /// `id` built from the events matching its criteria (`None` if there
    /// were none), consistent through `condition`.
    pub(crate) fn sourced(id: &A::Id, events: &[EventEnvelope<A::Event>], condition: AppendCondition) -> Option<Self> {
        if events.is_empty() {
            return None;
        }
        let mut aggregate = A::initial(id);
        for event in events {
            aggregate.evolve(&event.payload);
        }
        Some(Self { aggregate, version: None, condition: Some(condition) })
    }

    /// What its next commit expects of its stream: built from criteria, its
    /// condition guards it, not a stream version (other streams hold its
    /// events too).
    pub(crate) fn expected(&self) -> ExpectedVersion {
        match (&self.condition, self.version) {
            (Some(_), _) => ExpectedVersion::Any,
            (None, None) => ExpectedVersion::NoStream,
            (None, Some(version)) => ExpectedVersion::Exact(version),
        }
    }

    /// The stream version this aggregate was loaded at (`None` when new).
    pub fn version(&self) -> Option<u64> {
        self.version
    }

    /// Unwraps the aggregate, dropping the version: it can no longer be
    /// saved.
    pub fn into_inner(self) -> A {
        self.aggregate
    }
}

impl<A> Deref for Loaded<A> {
    type Target = A;

    fn deref(&self) -> &A {
        &self.aggregate
    }
}

/// The result of a successful save: the events are committed.
pub struct Saved<A: Aggregate> {
    /// The aggregate at its new version, ready for further changes and saves.
    pub aggregate: Loaded<A>,
    /// The envelopes the store committed (empty if there was nothing to save).
    pub events: Vec<EventEnvelope<A::Event>>,
    /// What happened in the in-process (subscribing) processors. A failure
    /// here does **not** undo the commit: the events are stored and a
    /// tracking processor would retry them.
    pub processing: Result<(), ProcessingFailure>,
}

/// Runs committed events through a processing group, in-process and right
/// after the commit, in the same call as the save. Projections are then
/// up to date when the save returns; a failure is reported in
/// [`Saved::processing`] and never undoes the commit.
///
/// Implemented for `()` (no processing) and for tuples of [`EventHandler`]s up
/// to 8 elements: a fixed, typed fan-out with no `Vec<Box<dyn _>>`.
pub trait EventProcessor<E: DomainEvent>: Send + Sync {
    /// Hands `events` (one commit, in order) to every handler of the group.
    fn process(
        &self,
        events: &[EventEnvelope<E>],
    ) -> impl Future<Output = Result<(), ProcessingFailure>> + Send;
}

/// Handlers that failed. Each handler is isolated: one failing does not stop
/// the others, and a failing handler skips the rest of the batch so it never
/// sees events out of order.
#[derive(Debug)]
pub struct ProcessingFailure {
    /// One entry per failed handler (never empty).
    pub failures: Vec<HandlerFailure>,
}

/// One handler that failed while processing a commit.
#[derive(Debug)]
pub struct HandlerFailure {
    /// The handler's type name, to tell which one failed.
    pub handler: &'static str,
    /// Global position of the event it failed on: where a retry resumes.
    pub position: u64,
    /// What the handler returned.
    pub error: HandlerError,
}

impl fmt::Display for ProcessingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} event handler(s) failed", self.failures.len())
    }
}

impl<E: DomainEvent> EventProcessor<E> for () {
    async fn process(&self, _: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
        Ok(())
    }
}

/// Feeds `events` to one handler, stopping at its first error (recorded in
/// `failures`) so it never sees later events before an earlier one.
async fn run_handler<E: DomainEvent, H: EventHandler<E>>(
    handler: &H,
    events: &[EventEnvelope<E>],
    failures: &mut Vec<HandlerFailure>,
) {
    for event in events {
        // The events are committed already: each one's post-commit step
        // follows its handling, as in a processor's unit of work.
        if let Err(error) = handler.handle(event, &mut crate::ProcessingContext::new(&mut (), false)).await {
            failures.push(HandlerFailure {
                handler: type_name::<H>(),
                position: event.position,
                error,
            });
            return;
        }
        handler.committed(event).await;
    }
}

/// Implements [`EventProcessor`] for a tuple of handlers: each one runs over
/// the whole batch, independently of the others' failures.
macro_rules! tuple_processor {
    ($($handler:ident $index:tt),+) => {
        impl<E: DomainEvent, $($handler: EventHandler<E>),+> EventProcessor<E> for ($($handler,)+) {
            async fn process(&self, events: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
                let mut failures = Vec::new();
                $(run_handler(&self.$index, events, &mut failures).await;)+
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(ProcessingFailure { failures })
                }
            }
        }
    };
}

tuple_processor!(H0 0);
tuple_processor!(H0 0, H1 1);
tuple_processor!(H0 0, H1 1, H2 2);
tuple_processor!(H0 0, H1 1, H2 2, H3 3);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5, H6 6);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5, H6 6, H7 7);

/// Event sourcing for aggregates `A`: loads them by replaying their stream
/// (from a snapshot, if any) and commits their new events with optimistic
/// concurrency, then hands the commit to the processing group `P`.
/// Statically typed over the store `S`, `P` and the snapshot store `N`.
///
/// ```text
/// let posts = EventSourced::<Post, _>::new(store);              // or events.event_sourced::<Post>()
/// let post = posts.load(&id).await?;                            // in your repository's adapter
/// posts.commit(post, events).await?;                            // in a handler, as EventPublisher<Post>
/// ```
pub struct EventSourced<A, S, P = (), N = ()> {
    /// Where the event streams live.
    store: S,
    /// Who receives the events after each commit.
    processor: P,
    /// Saved states, so a load replays only the tail (`()`: none).
    snapshots: N,
    /// Take a snapshot every this many events (0: never).
    snapshot_every: u64,
    /// How many times [`update`](Self::update) re-runs a decision whose
    /// commit lost an optimistic-concurrency race.
    conflict_retries: u32,
    /// Ties the engine to one aggregate type without storing one.
    aggregate: PhantomData<fn() -> A>,
}

impl<A, S> EventSourced<A, S, ()> {
    /// Event sourcing over `store`, whose commits feed no event handler.
    pub fn new(store: S) -> Self {
        Self::with_processor(store, ())
    }
}

impl<A, S, P> EventSourced<A, S, P> {
    /// Event sourcing over `store` that hands every commit to `processor`
    /// (an [`EventProcessor`], e.g. a tuple of event handlers).
    pub fn with_processor(store: S, processor: P) -> Self {
        Self {
            store,
            processor,
            snapshots: (),
            snapshot_every: 0,
            conflict_retries: DEFAULT_CONFLICT_RETRIES,
            aggregate: PhantomData,
        }
    }
}

impl<A, S, P, N> EventSourced<A, S, P, N> {
    /// Keeps a snapshot in `snapshots` every `every` events, so loading
    /// replays at most `every - 1` events after the newest one. Worth it for
    /// long-lived aggregates; the stream stays the source of truth.
    ///
    /// # Panics
    ///
    /// If `every` is zero.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> EventSourced<A, S, P, N2> {
        assert!(every > 0, "snapshot every 0 events");
        EventSourced {
            store: self.store,
            processor: self.processor,
            snapshots,
            snapshot_every: every,
            conflict_retries: self.conflict_retries,
            aggregate: PhantomData,
        }
    }

    /// How many times [`update`](Self::update) re-runs a decision after
    /// losing a concurrency race (default 8; 0 never retries).
    #[must_use]
    pub fn with_conflict_retries(mut self, retries: u32) -> Self {
        self.conflict_retries = retries;
        self
    }

    /// The underlying event store, e.g. to read every event in order.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The processing group fed after each commit.
    pub fn processor(&self) -> &P {
        &self.processor
    }
}

/// Retries of a decision that lost a concurrency race, unless configured.
const DEFAULT_CONFLICT_RETRIES: u32 = 8;

impl<A, S, P, N> EventSourced<A, S, P, N>
where
    A: Aggregate,
    S: EventStore<A::Event>,
    P: EventProcessor<A::Event>,
    N: SnapshotStore<A>,
{
    /// Rebuilds the aggregate: from its newest snapshot if any, then the
    /// events after it. Replay calls [`Aggregate::evolve`] directly, so it
    /// never records pending events.
    pub async fn load(&self, id: &A::Id) -> Result<Option<Loaded<A>>, StoreError> {
        if let Some(criteria) = A::criteria(id) {
            let (events, head) = self.store.source(&criteria).await?;
            return Ok(Loaded::sourced(id, &events, AppendCondition { criteria, after: head }));
        }
        // A failing snapshot store only costs a full replay.
        let snapshot = self.snapshots.load(id).await.ok().flatten();
        let (mut aggregate, mut version) = snapshot.unwrap_or_else(|| (A::initial(id), 0));
        let tail = self.store.load(&StreamId::of::<A>(id), version).await?;
        if version == 0 && tail.is_empty() {
            return Ok(None);
        }
        for event in &tail {
            aggregate.evolve(&event.payload);
            version = event.sequence;
        }
        if self.snapshot_every > 0 && tail.len() as u64 >= self.snapshot_every {
            // Commits through a publisher do not snapshot (it serves every
            // aggregate of the event type): a long replay does. A failed
            // snapshot only means a longer replay next time.
            let _ = self.snapshots.save(&aggregate, version).await;
        }
        Ok(Some(Loaded {
            aggregate,
            version: Some(version),
            condition: None,
        }))
    }

    /// Creates the aggregate `id`: runs `decide` on its initial state, then
    /// saves the events it appended, expecting the stream not to exist.
    ///
    /// The decision's error (`De`, usually the domain's) and the store's
    /// both convert into the caller's error `Er`, so a handler returns the
    /// call as is: `posts.create(&id, |post, events| post.create(title, events)).await`.
    ///
    /// # Errors
    ///
    /// What `decide` returns, or the store's error, including
    /// [`StoreError::Conflict`] if `id` already exists.
    pub async fn create<T, De, Er>(
        &self,
        id: &A::Id,
        decide: impl FnOnce(&A, &mut Vec<A::Event>) -> Result<T, De>,
    ) -> Result<T, Er>
    where
        Er: From<De> + From<StoreError>,
    {
        let new = Loaded::new(A::initial(id));
        let mut events = Vec::new();
        let output = decide(&new, &mut events).map_err(Er::from)?;
        self.append(new, events).await?;
        Ok(output)
    }

    /// Changes the aggregate `id`: loads it, runs `decide` on it, and saves
    /// the events it appended, expecting the version it loaded. Commit at
    /// the end of the handler, as Axon's processing context does.
    ///
    /// If someone else saved in between, the decision is re-run on the
    /// fresh state, up to [`with_conflict_retries`](Self::with_conflict_retries)
    /// times: `decide` must have no side effects besides appending.
    ///
    /// # Errors
    ///
    /// What `decide` returns, [`NotFound`] if `id` has no events, or the
    /// store's error (a [`StoreError::Conflict`] once the retries run out),
    /// each converted into `Er`.
    pub async fn update<T, De, Er>(
        &self,
        id: &A::Id,
        decide: impl Fn(&A, &mut Vec<A::Event>) -> Result<T, De>,
    ) -> Result<T, Er>
    where
        Er: From<De> + From<StoreError> + From<NotFound<A::Id>>,
    {
        let mut retries = self.conflict_retries;
        loop {
            let loaded = self.load(id).await?.ok_or_else(|| NotFound(id.clone()))?;
            let mut events = Vec::new();
            let output = decide(&loaded, &mut events).map_err(Er::from)?;
            match self.append(loaded, events).await {
                Ok(_) => return Ok(output),
                Err(StoreError::Conflict { .. }) if retries > 0 => retries -= 1,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// [`load`](Self::load) within a command: for an aggregate built from
    /// [`criteria`](Aggregate::criteria), the commits of `events` then fail
    /// if anything it was built from changes, **even if it did not exist**
    /// (absence is a fact a decision can rely on too). The aggregates a
    /// decision reads without committing are loaded this way.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn load_in<X>(&self, id: &A::Id, events: &mut Appender<A::Event, X>) -> Result<Option<Loaded<A>>, StoreError> {
        let Some(criteria) = A::criteria(id) else {
            return self.load(id).await;
        };
        let (found, head) = self.store.source(&criteria).await?;
        let condition = AppendCondition { criteria, after: head };
        events.depend_on(condition.clone());
        Ok(Loaded::sourced(id, &found, condition))
    }

    /// Reads every event matching `criteria`, from any stream of the store,
    /// and makes the following commits of `events` conditional on them (see
    /// [`Appender::source`]).
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn source<X>(&self, criteria: EventCriteria, events: &mut Appender<A::Event, X>) -> Result<Vec<EventEnvelope<A::Event>>, StoreError> {
        events.source(&self.store, criteria).await
    }

    /// Appends `events` to `loaded`'s stream, expecting the version it was
    /// loaded at, and applies them to it: the low-level step under
    /// [`create`](Self::create) and [`update`](Self::update), for code
    /// outside command handlers (which commit through their appender).
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] if the stream moved on since `loaded` was
    /// read; then `loaded` is dropped, so stale state cannot be reused.
    pub async fn append(&self, loaded: Loaded<A>, events: Vec<A::Event>) -> Result<Saved<A>, StoreError> {
        self.append_with(loaded, events, &Metadata::new()).await
    }

    /// Like [`append`](Self::append), storing `metadata` with every event (e.g.
    /// the correlation id of the request that caused them). Then hands the
    /// committed envelopes to the processing group.
    ///
    /// # Errors
    ///
    /// As [`append`](Self::append).
    pub async fn append_with(
        &self,
        loaded: Loaded<A>,
        events: Vec<A::Event>,
        metadata: &Metadata,
    ) -> Result<Saved<A>, StoreError> {
        self.append_if(loaded, events, metadata, None).await
    }

    /// Like [`append_with`](Self::append_with), and only if `condition`
    /// holds (see [`EventStore::append_if`]).
    ///
    /// # Errors
    ///
    /// As [`append`](Self::append), or [`StoreError::ConditionFailed`].
    pub async fn append_if(
        &self,
        mut loaded: Loaded<A>,
        events: Vec<A::Event>,
        metadata: &Metadata,
        condition: Option<&AppendCondition>,
    ) -> Result<Saved<A>, StoreError> {
        if events.is_empty() {
            return Ok(Saved {
                aggregate: loaded,
                events: Vec::new(),
                processing: Ok(()),
            });
        }

        let expected = loaded.expected();
        let condition = AppendCondition::both(loaded.condition.clone(), condition.cloned());
        let stream = StreamId::of::<A>(loaded.aggregate.id());
        // On error `loaded` (already mutated) is dropped here.
        let committed = self
            .store
            .append_if(&stream, expected, events, metadata, condition.as_ref())
            .await?;

        // Committed: now the aggregate may change, through its events only.
        for event in &committed {
            loaded.aggregate.evolve(&event.payload);
        }
        if let Some(condition) = &mut loaded.condition {
            // Its own events match its criteria: the next save checks only
            // what others append after them.
            condition.after = committed.last().map_or(condition.after, |event| event.position);
            let processing = self.processor.process(&committed).await;
            return Ok(Saved { aggregate: loaded, events: committed, processing });
        }
        let before = loaded.version.unwrap_or(0);
        loaded.version = committed.last().map(|event| event.sequence);
        let after = loaded.version.unwrap_or(0);
        if self.snapshot_every > 0 && before / self.snapshot_every != after / self.snapshot_every {
            // An optimization: a failed snapshot only means a longer replay.
            let _ = self.snapshots.save(&loaded.aggregate, after).await;
        }
        let processing = self.processor.process(&committed).await;
        Ok(Saved {
            aggregate: loaded,
            events: committed,
            processing,
        })
    }
}

/// Rebuilds aggregate `id` from `store`: its stream, or the events matching
/// its [`criteria`](Aggregate::criteria). What an application's repository
/// adapter calls to find an event-sourced entity (no snapshots: see
/// [`EventSourced::load`] for that).
///
/// ```text
/// async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
///     cqrs::load(&self.store, &id).await
/// }
/// ```
///
/// # Errors
///
/// The store's.
pub async fn load<A, S>(store: &S, id: &A::Id) -> Result<Option<Loaded<A>>, StoreError>
where
    A: Aggregate,
    S: EventStore<A::Event>,
{
    if let Some(criteria) = A::criteria(id) {
        let (events, head) = store.source(&criteria).await?;
        return Ok(Loaded::sourced(id, &events, AppendCondition { criteria, after: head }));
    }
    let events = store.load(&StreamId::of::<A>(id), 0).await?;
    let Some(last) = events.last() else { return Ok(None) };
    let version = last.sequence;
    let mut aggregate = A::initial(id);
    for event in &events {
        aggregate.evolve(&event.payload);
    }
    Ok(Some(Loaded { aggregate, version: Some(version), condition: None }))
}

/// [`load`] within a command: for an aggregate built from
/// [`criteria`](Aggregate::criteria), the commits of `events` then fail if
/// anything it was built from changes, even if it did not exist.
///
/// # Errors
///
/// The store's.
pub async fn load_in<A, S, P>(store: &S, id: &A::Id, events: &mut Appender<A::Event, P>) -> Result<Option<Loaded<A>>, StoreError>
where
    A: Aggregate,
    S: EventStore<A::Event>,
{
    let Some(criteria) = A::criteria(id) else {
        return load(store, id).await;
    };
    let (found, head) = store.source(&criteria).await?;
    let condition = AppendCondition { criteria, after: head };
    events.depend_on(condition.clone());
    Ok(Loaded::sourced(id, &found, condition))
}

/// Appends `pending` to `entity`'s stream in `store`, expecting the version
/// it was loaded at, then hands the commit to `processor`: what every
/// publisher does. The last position, if anything was appended.
pub(crate) async fn publish<A, S, P>(store: &S, processor: &P, entity: Loaded<A>, pending: Pending<A::Event>) -> Result<Option<u64>, StoreError>
where
    A: Aggregate,
    S: EventStore<A::Event>,
    P: EventProcessor<A::Event>,
{
    let expected = entity.expected();
    let condition = AppendCondition::both(entity.condition, pending.condition);
    let stream = StreamId::of::<A>(entity.aggregate.id());
    let committed = store.append_if(&stream, expected, pending.events, &pending.metadata, condition.as_ref()).await?;
    // Processing failures do not undo the commit: a tracking processor
    // retries; inline ones are the caller's to watch.
    let _ = processor.process(&committed).await;
    Ok(committed.last().map(|event| event.position))
}

/// The engine publishes every aggregate of its event type: appends, then
/// feeds its processing group (inline).
impl<A0, S, P, N> EventPublisher<A0::Event> for EventSourced<A0, S, P, N>
where
    A0: Aggregate,
    S: EventStore<A0::Event>,
    P: EventProcessor<A0::Event>,
    N: Send + Sync,
{
    fn commit<A: Aggregate<Event = A0::Event>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<A0::Event>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send {
        publish(&self.store, &self.processor, entity, pending)
    }
}

/// The aggregate an [`update`](EventSourced::update) targets has
/// no events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotFound<Id>(pub Id);

impl<Id: fmt::Display> fmt::Display for NotFound<Id> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} not found", self.0)
    }
}

impl<Id: fmt::Debug + fmt::Display> std::error::Error for NotFound<Id> {}

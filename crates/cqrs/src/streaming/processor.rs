use crate::sync::Locked;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{FutureExt, StreamExt};
use tokio::{
    sync::{mpsc, watch},
    task::{AbortHandle, JoinSet},
};

use super::{
    LiveEvents, SubscriptionOptions,
    subscription::{Start, track},
};
use crate::{
    Cursor, DeadLetter, DeadLetterQueue, DomainEvent, EventEnvelope, EventHandler, HandlerError, ProcessingContext,
    ReadLog, StoreError, TokenStore, TrackingToken, TransactionManager, segment_of,
};

use crate::message::sealed::INTERNAL;

/// What moving a segment's token did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenUpdate {
    /// Nothing: the processor stores the cursor itself.
    Untouched,
    /// Moved past the event, atomically with the handler's effects.
    Stored,
    /// Another instance holds the segment now; nothing was committed and
    /// the processor stops handling it.
    ClaimLost,
}

/// How a [`TrackingProcessor`] splits, claims, reads and retries.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct TrackingOptions {
    /// How each segment reads the store and the live feed.
    pub subscription: SubscriptionOptions,
    /// Segments the log is split into, by aggregate. Fixed by whoever
    /// initializes the token store first. Default 1.
    pub segments: u32,
    /// At most this many segments per instance, so several instances share
    /// them. Default: all.
    pub max_segments: u32,
    /// How long a claim lasts unless renewed; after it, another instance may
    /// take the segment over. Default 5 s.
    pub claim_timeout: Duration,
    /// How often claims are renewed and free segments looked for. Must be
    /// well under `claim_timeout`. Default 1 s.
    pub claim_interval: Duration,
    /// Wait before the first retry of a failed event; doubles on each retry.
    pub retry_initial: Duration,
    /// Upper bound for the wait between retries.
    pub retry_max: Duration,
    /// Attempts before an event is parked in the dead-letter queue (with the
    /// later events of its aggregate). `None`: retry forever, holding the
    /// segment back. Needs [`TrackingProcessor::with_dead_letters`].
    pub dead_letter_after: Option<u32>,
    /// Events the shared reader may queue for one segment. A full queue is
    /// backpressure: the reader waits for room. Default 1024.
    pub segment_buffer: usize,
    /// How long the reader waits for room in a full queue before deciding the
    /// segment is stuck: it is then detached and reads on its own, so it
    /// never holds the others back. Default 1 s.
    pub segment_stall_timeout: Duration,
    /// Events of a segment handled in one unit of work, with one token
    /// update. A worker takes what is already queued
    /// for it, up to this many, and never waits to fill a batch, so it adds
    /// no latency. If a batch fails it is rolled back and its events are
    /// handled one at a time, with the usual retries and dead letters.
    /// Default 1.
    pub batch_size: usize,
    /// How often an instance that does not own every segment reads the
    /// others' progress from the token store, to tell when an event was
    /// handled elsewhere ([`ProcessorStatus::handled`]): the extra latency of
    /// subscriptions served by another instance. Default 50 ms.
    pub progress_poll: Duration,
}

/// Built from [`Default`], one setter per field: `TrackingOptions::default().subscription(..)`.
impl TrackingOptions {
    /// Sets [`subscription`](Self::subscription): how each segment reads the store and the live feed.
    #[must_use]
    pub fn subscription(mut self, subscription: SubscriptionOptions) -> Self {
        self.subscription = subscription;
        self
    }

    /// Sets [`segments`](Self::segments): segments the log is split into, by aggregate. Fixed by whoever
    #[must_use]
    pub fn segments(mut self, segments: u32) -> Self {
        self.segments = segments;
        self
    }

    /// Sets [`max_segments`](Self::max_segments): at most this many segments per instance, so several instances share
    #[must_use]
    pub fn max_segments(mut self, max_segments: u32) -> Self {
        self.max_segments = max_segments;
        self
    }

    /// Sets [`claim_timeout`](Self::claim_timeout): how long a claim lasts unless renewed; after it, another instance may
    #[must_use]
    pub fn claim_timeout(mut self, claim_timeout: Duration) -> Self {
        self.claim_timeout = claim_timeout;
        self
    }

    /// Sets [`claim_interval`](Self::claim_interval): how often claims are renewed and free segments looked for. Must be
    #[must_use]
    pub fn claim_interval(mut self, claim_interval: Duration) -> Self {
        self.claim_interval = claim_interval;
        self
    }

    /// Sets [`retry_initial`](Self::retry_initial): wait before the first retry of a failed event; doubles on each retry.
    #[must_use]
    pub fn retry_initial(mut self, retry_initial: Duration) -> Self {
        self.retry_initial = retry_initial;
        self
    }

    /// Sets [`retry_max`](Self::retry_max): upper bound for the wait between retries.
    #[must_use]
    pub fn retry_max(mut self, retry_max: Duration) -> Self {
        self.retry_max = retry_max;
        self
    }

    /// Sets [`dead_letter_after`](Self::dead_letter_after): attempts before an event is parked in the dead-letter queue (with the
    #[must_use]
    pub fn dead_letter_after(mut self, dead_letter_after: Option<u32>) -> Self {
        self.dead_letter_after = dead_letter_after;
        self
    }

    /// Sets [`segment_buffer`](Self::segment_buffer): events the shared reader may queue for one segment. A full queue is
    #[must_use]
    pub fn segment_buffer(mut self, segment_buffer: usize) -> Self {
        self.segment_buffer = segment_buffer;
        self
    }

    /// Sets [`segment_stall_timeout`](Self::segment_stall_timeout): how long the reader waits for room in a full queue before deciding the
    #[must_use]
    pub fn segment_stall_timeout(mut self, segment_stall_timeout: Duration) -> Self {
        self.segment_stall_timeout = segment_stall_timeout;
        self
    }

    /// Sets [`batch_size`](Self::batch_size): events of a segment handled in one unit of work, with one token
    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets [`progress_poll`](Self::progress_poll): how often an instance that does not own every segment reads the
    #[must_use]
    pub fn progress_poll(mut self, progress_poll: Duration) -> Self {
        self.progress_poll = progress_poll;
        self
    }
}

impl Default for TrackingOptions {
    fn default() -> Self {
        Self {
            subscription: SubscriptionOptions::default(),
            segments: 1,
            max_segments: u32::MAX,
            claim_timeout: Duration::from_secs(5),
            claim_interval: Duration::from_secs(1),
            retry_initial: Duration::from_millis(50),
            retry_max: Duration::from_secs(5),
            dead_letter_after: None,
            segment_buffer: 1024,
            segment_stall_timeout: Duration::from_secs(1),
            batch_size: 1,
            progress_poll: Duration::from_millis(50),
        }
    }
}

/// Runs one [`EventHandler`] over the event log, split into segments by
/// aggregate: Axon 5's pooled streaming processor.
///
/// - **Order per aggregate, parallel across them.** An aggregate's events
///   always land in the same segment (see [`segment_of`]); each segment is
///   handled by its own worker, in position order.
/// - **One read per instance.** A single reader reads the log once and hands
///   each event to its segment's queue. A segment that falls behind (its queue fills, or it was taken
///   over from another instance behind the reader) reads on its own from its
///   position, so a slow segment never blocks the others.
/// - **Shared between instances.** Segments are claimed in the
///   [`TokenStore`]; claims are renewed every `claim_interval` and expire
///   after `claim_timeout`, so the segments of an instance that dies are
///   taken over from their stored positions. Storing a position requires
///   still holding the claim.
/// - **Failures.** A failing event is retried with exponential backoff; it
///   only holds back its own segment. With a dead-letter queue, after
///   `dead_letter_after` attempts it is parked together with the later
///   events of its aggregate, and everything else keeps flowing.
/// - **One unit of work per event.** The transaction manager `M` opens it;
///   the handler, the token store `T` and the dead-letter queue `D` borrow
///   it; it commits the handler's changes with the token. A read model in
///   the same database as the tokens applies each event exactly once.
///   Outside of it (an e-mail, a call to another service), delivery is
///   at-least-once: an event handled just before a crash is handled again.
#[must_use = "a processor does nothing until it is spawned or run"]
pub struct TrackingProcessor<E, S, H, T, M = (), D = ()> {
    /// Key of the tokens, and the name in logs.
    name: String,
    /// Where the events are read from.
    store: S,
    /// Live feed, for low latency once caught up.
    live: LiveEvents<E>,
    /// Changes when another process committed to the log (from
    /// [`Events`](super::Events)): segments read the store at once.
    wake: Option<watch::Receiver<u64>>,
    /// What each event is handed to, shared by the segment workers.
    handler: Arc<H>,
    /// Positions and claims.
    tokens: Arc<T>,
    /// Opens and commits the units of work.
    transactions: Arc<M>,
    /// Parked events.
    dead_letters: Arc<D>,
    /// What to do with a failing event, when it has a queue (`None`:
    /// [`ParkAfter`](crate::ParkAfter) the options' `dead_letter_after`).
    policy: Option<Arc<dyn crate::EnqueuePolicy<E>>>,
    /// Splitting, claiming, reading and retry settings.
    options: TrackingOptions,
    /// Events up to here are a replay (read from the tokens at start).
    replay_until: AtomicU64,
    /// Where the running instance publishes its progress.
    status: watch::Sender<ProcessorStatus>,
}

impl<E, S, H, T, M, D> ::std::fmt::Debug for TrackingProcessor<E, S, H, T, M, D> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("TrackingProcessor").finish_non_exhaustive()
    }
}

/// What a running processor instance reports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProcessorStatus {
    /// Every event up to here was handled by the segments this instance
    /// owns (the lowest of their watermarks; 0 while it owns none). Events
    /// above it may be handled too: see [`covers`](Self::covers).
    pub position: u64,
    /// The error of an event being retried, while it keeps failing.
    pub failing: Option<String>,
    /// The segments this instance owns.
    pub segments: Vec<u32>,
    /// Their cursors, in the same order.
    pub cursors: Vec<Cursor>,
    /// How many segments the processor has, across its instances (0 until
    /// its tokens are initialized).
    pub segment_count: u32,
    /// The stored cursor of every segment, whoever owns it: read from the
    /// token store every `progress_poll` while this instance does not own
    /// them all (empty otherwise). What tells, here, that another instance
    /// handled an event.
    pub shared_cursors: Vec<Cursor>,
}

impl ProcessorStatus {
    /// Whether the processor, in any of its instances, handled the event at
    /// `position` (and every event before it that was committed then): what
    /// a read model's reader waits for. From this instance's segments when
    /// it owns them all (no lag); otherwise from every segment's stored
    /// cursor, as last read.
    pub fn handled(&self, position: u64) -> bool {
        if self.segment_count > 0 && self.segments.len() == self.segment_count as usize {
            return self.covers(position);
        }
        self.shared_cursors.len() == self.segment_count as usize
            && !self.shared_cursors.is_empty()
            && self.shared_cursors.iter().all(|cursor| cursor.covers(position))
    }

    /// Whether this instance owns segments and they all saw the event at
    /// `position` (the one it belongs to handled it).
    pub fn covers(&self, position: u64) -> bool {
        !self.cursors.is_empty() && self.cursors.iter().all(|cursor| cursor.covers(position))
    }
}

impl<E, S, H, T, M> TrackingProcessor<E, S, H, T, M>
where
    E: DomainEvent,
    S: ReadLog<E> + Clone + 'static,
    H: EventHandler<E, M::Tx> + 'static,
    T: TokenStore<Tx = M::Tx> + 'static,
    M: TransactionManager + 'static,
{
    /// A processor named `name` feeding `handler`, in units of work from
    /// `transactions` that its tokens join. The store must be the one the
    /// publisher appends to (a handle, so it is `Clone`).
    pub fn new(
        name: impl Into<String>,
        store: S,
        live: &LiveEvents<E>,
        handler: H,
        tokens: T,
        transactions: M,
    ) -> Self {
        Self {
            name: name.into(),
            store,
            live: live.clone(),
            wake: None,
            handler: Arc::new(handler),
            tokens: Arc::new(tokens),
            transactions: Arc::new(transactions),
            dead_letters: Arc::new(()),
            policy: None,
            options: TrackingOptions::default(),
            replay_until: AtomicU64::new(0),
            status: watch::channel(ProcessorStatus::default()).0,
        }
    }

    /// Reads the store as soon as `wake` changes (another process committed).
    pub(crate) fn waking_on(mut self, wake: watch::Receiver<u64>) -> Self {
        self.wake = Some(wake);
        self
    }
}

impl<E, S, H, T, M, D> TrackingProcessor<E, S, H, T, M, D>
where
    E: DomainEvent,
    S: ReadLog<E> + Clone + 'static,
    H: EventHandler<E, M::Tx> + 'static,
    T: TokenStore<Tx = M::Tx> + 'static,
    M: TransactionManager + 'static,
    D: DeadLetterQueue<E, M::Tx> + 'static,
{
    /// Its settings.
    pub fn options(&self) -> TrackingOptions {
        self.options
    }

    /// Replaces the default settings.
    pub fn with_options(mut self, options: TrackingOptions) -> Self {
        self.options = options;
        self
    }

    /// Parks events in `queue` after `after` failed attempts (with the later
    /// events of their aggregate) instead of retrying forever. The queue
    /// joins the processor's units of work: a queue that cannot (in memory,
    /// for a database's) is refused here.
    pub fn with_dead_letters<D2>(self, queue: D2, after: u32) -> TrackingProcessor<E, S, H, T, M, D2>
    where
        D2: DeadLetterQueue<E, M::Tx>,
    {
        TrackingProcessor {
            name: self.name,
            store: self.store,
            live: self.live,
            wake: self.wake,
            handler: self.handler,
            tokens: self.tokens,
            transactions: self.transactions,
            dead_letters: Arc::new(queue),
            policy: self.policy,
            options: TrackingOptions {
                dead_letter_after: Some(after.max(1)),
                ..self.options
            },
            replay_until: self.replay_until,
            status: self.status,
        }
    }

    /// Decides with `policy` what happens to a failing event (park it,
    /// retry it, skip it) and to a parked one failing again at a retry
    /// (keep it, evict it), instead of parking after the attempts given to
    /// [`with_dead_letters`](Self::with_dead_letters). Needs a queue.
    pub fn with_enqueue_policy(mut self, policy: impl crate::EnqueuePolicy<E> + 'static) -> Self {
        self.policy = Some(Arc::new(policy));
        self
    }

    /// Shares `policy` (what a projection hands each namespace).
    pub(crate) fn with_shared_policy(mut self, policy: Arc<dyn crate::EnqueuePolicy<E>>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Replays the processor from the start: moves
    /// its tokens back to position 0, then lets the handler clear what it
    /// built ([`EventHandler::reset`]). Every instance must be stopped
    /// first; [`spawn`](Self::spawn) then replays. The events stored so far
    /// are a replay, which a handler that does not
    /// [`REPLAY`](EventHandler::REPLAY) skips.
    ///
    /// # Errors
    ///
    /// [`ResetError::Running`] while an instance still holds a segment (its
    /// tokens are untouched); the store's or the handler's error otherwise.
    /// A failed handler reset leaves the tokens reset: call it again.
    pub async fn reset(&self) -> Result<(), ResetError> {
        self.tokens.initialize(&self.name, self.options.segments).await?;
        let replay_until = self.store.last_position().await?;
        if !self.tokens.reset(&self.name, clock(), replay_until).await? {
            return Err(ResetError::Running);
        }
        let mut tx = self.transactions.begin().await?;
        self.handler
            .reset(&mut ProcessingContext::in_processor(&mut tx, false, &self.name))
            .await
            .map_err(ResetError::Handler)?;
        Ok(self.transactions.commit(tx).await?)
    }

    /// Retries the events this processor parked, each in a unit of work of its own: see
    /// [`retry_dead_letters`](crate::retry_dead_letters). Run it once the
    /// cause is fixed; running instances may keep parking meanwhile.
    ///
    /// # Errors
    ///
    /// The queue's, or a unit of work that cannot begin or commit.
    pub async fn retry_dead_letters(&self) -> Result<crate::RetryOutcome, StoreError> {
        let default = crate::ParkAfter(self.options.dead_letter_after.unwrap_or(1));
        let policy: &dyn crate::EnqueuePolicy<E> = match &self.policy {
            Some(policy) => &**policy,
            None => &default,
        };
        crate::retry_dead_letters(
            &*self.dead_letters,
            &self.name,
            &*self.transactions,
            &*self.handler,
            policy,
        )
        .await
    }

    /// Follows the progress of this processor once it runs, e.g. to wait
    /// until it handled a position (`wait_for(|status| status.covers(p))`).
    pub fn progress(&self) -> watch::Receiver<ProcessorStatus> {
        self.status.subscribe()
    }

    /// Runs the processor until `shutdown` completes: claims segments,
    /// handles their events, then, once `shutdown` completed, finishes the
    /// events in hand and releases its segments for other instances.
    ///
    /// The future is the caller's to run: awaited, in a `JoinSet` with the
    /// rest of the application, or spawned ([`spawn`](Self::spawn) does
    /// that, with a handle to stop it). The segments' workers run on Tokio
    /// tasks of the current runtime.
    ///
    /// ```text
    /// let progress = processor.progress();
    /// tasks.spawn(processor.run(shutdown.cancelled_owned()));
    /// ```
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) {
        let (stop, stop_rx) = watch::channel(false);
        let owner = owner_id(&self.name);
        let progress = Arc::new(Progress {
            segments: Mutex::new(BTreeMap::new()),
            status: self.status.clone(),
        });
        let running = Arc::new(self).coordinate(owner, progress, stop_rx);
        let mut running = pin!(running);
        tokio::select! {
            () = &mut running => {}
            () = shutdown => {
                let _ = stop.send(true);
                running.await;
            }
        }
    }

    /// Starts the processor on a Tokio task ([`run`](Self::run), stopped by
    /// the handle).
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    pub fn spawn(self) -> ProcessorHandle {
        let (stop, mut stop_rx) = watch::channel(false);
        let name = self.name.clone();
        let status = self.progress();
        // A stop, or the handle dropped.
        let shutdown = async move {
            let _ = stop_rx.changed().await;
        };
        let task = tokio::spawn(self.run(shutdown));
        ProcessorHandle {
            name,
            status,
            stop,
            task,
        }
    }

    /// Claims and renews segments, runs a worker per owned segment, and
    /// releases them on stop.
    async fn coordinate(self: Arc<Self>, owner: String, progress: Arc<Progress>, mut stop: watch::Receiver<bool>) {
        let mut backoff = Backoff::new(&self.options);
        let count = loop {
            let started = async {
                let count = self.tokens.initialize(&self.name, self.options.segments).await?;
                Ok::<_, StoreError>((count, self.tokens.replay_until(&self.name).await?))
            };
            match started.await {
                Ok((count, replay_until)) => {
                    self.replay_until.store(replay_until, Ordering::Relaxed);
                    progress.count(count);
                    break count;
                }
                Err(error) => {
                    tracing::warn!(processor = %self.name, %error, "cannot initialize tokens");
                    if backoff.wait(&mut stop).await {
                        return;
                    }
                }
            }
        };

        // Dropping the set (stop, or `abort` simulating a crash) aborts the
        // reader and the workers.
        let mut workers = JoinSet::new();
        let mut owned: HashMap<u32, (AbortHandle, watch::Sender<bool>)> = HashMap::new();
        let routes: Routes<E> = Arc::new(Mutex::new(HashMap::new()));
        let read_up_to = Arc::new(Mutex::new(Cursor::default()));
        let mut reader: Option<watch::Sender<bool>> = None;
        'run: loop {
            while workers.try_join_next().is_some() {}
            owned.retain(|segment, (task, _)| {
                let alive = !task.is_finished();
                if !alive {
                    progress.forget(*segment); // lost its claim or stopped
                }
                alive
            });

            let now = clock();
            let until = now + self.options.claim_timeout.as_millis() as u64;
            for segment in 0..count {
                let mine = owned.contains_key(&segment);
                if !mine && owned.len() as u32 >= self.options.max_segments {
                    continue;
                }
                match self.tokens.claim(&self.name, segment, &owner, now, until).await {
                    Ok(Some(cursor)) if !mine => {
                        let (stop_worker, worker_stop) = watch::channel(false);
                        progress.track(segment, cursor.clone());
                        let done = Arc::new(AtomicU64::new(0));
                        // Routed through the shared reader (with several
                        // segments: with one, it would only add a hop) unless it
                        // already read something this segment did not see.
                        let routed = count > 1 && (reader.is_none() || cursor.includes(&read_up_to.locked()));
                        let inbox = routed.then(|| {
                            let (sender, inbox) = mpsc::channel(self.options.segment_buffer.max(1));
                            let route = Route {
                                sender,
                                from: cursor.clone(),
                                sent: 0,
                                done: Arc::clone(&done),
                            };
                            routes.locked().insert(segment, route);
                            inbox
                        });
                        let worker = Arc::clone(&self).work(
                            owner.clone(),
                            segment,
                            count,
                            cursor,
                            inbox,
                            done,
                            Arc::clone(&progress),
                            worker_stop,
                        );
                        owned.insert(segment, (workers.spawn(worker), stop_worker));
                    }
                    Ok(None) if mine => {
                        // Expired and taken over: stop handling it.
                        routes.locked().remove(&segment);
                        if let Some((_, stop_worker)) = owned.remove(&segment) {
                            let _ = stop_worker.send(true);
                        }
                        progress.forget(segment);
                    }
                    Ok(_) => {} // renewed, or held by someone else
                    Err(error) => tracing::warn!(processor = %self.name, segment, %error, "cannot claim"),
                }
            }

            // Start the shared reader once there is something to route, from
            // what every routed segment saw.
            let start = Cursor::common(routes.locked().values().map(|route| &route.from));
            if let (None, Some(start)) = (&reader, start) {
                start.clone_into(&mut read_up_to.locked());
                let (stop_reader, reader_stop) = watch::channel(false);
                let read = Arc::clone(&self).read(
                    owner.clone(),
                    count,
                    start,
                    Arc::clone(&routes),
                    Arc::clone(&read_up_to),
                    Arc::clone(&progress),
                    reader_stop,
                );
                workers.spawn(read);
                reader = Some(stop_reader);
            }

            // Until the next claim round: while other instances own
            // segments, follow their progress from the token store.
            let next_round = tokio::time::sleep(self.options.claim_interval);
            let mut next_round = pin!(next_round);
            let mut poll = tokio::time::interval(self.options.progress_poll);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let sharing = (owned.len() as u32) < count;
            loop {
                tokio::select! {
                    _ = stop.changed() => break 'run,
                    () = &mut next_round => break,
                    _ = poll.tick(), if sharing => match self.tokens.cursors(&self.name).await {
                        Ok(cursors) => progress.share(cursors),
                        Err(error) => tracing::debug!(processor = %self.name, %error, "cannot read the shared progress"),
                    },
                }
            }
        }

        for (_, stop_worker) in owned.values() {
            let _ = stop_worker.send(true);
        }
        if let Some(stop_reader) = &reader {
            let _ = stop_reader.send(true);
        }
        while workers.join_next().await.is_some() {}
        for segment in owned.keys() {
            let _ = self.tokens.release(&self.name, *segment, &owner).await;
        }
    }

    /// Handles one segment, in order: first the events the shared reader
    /// routes to its `inbox`; once detached (or with no inbox), by reading
    /// the log itself from where it got.
    #[allow(clippy::too_many_arguments)]
    async fn work(
        self: Arc<Self>,
        owner: String,
        segment: u32,
        count: u32,
        mut cursor: Cursor,
        inbox: Option<mpsc::Receiver<(Arc<EventEnvelope<E>>, Cursor)>>,
        done: Arc<AtomicU64>,
        progress: Arc<Progress>,
        mut stop: watch::Receiver<bool>,
    ) {
        if let Some(inbox) = inbox
            && !self
                .work_routed(&owner, segment, &mut cursor, inbox, &done, &progress, &mut stop)
                .await
        {
            return;
        }
        self.work_alone(&owner, segment, count, cursor, &progress, &mut stop)
            .await;
    }

    /// The routed mode: handles what the shared reader queues in `inbox`,
    /// in batches of what is already there, moving `cursor`. `true` once
    /// detached (the inbox closed): read alone from `cursor`; `false`:
    /// stop.
    #[allow(clippy::too_many_arguments)]
    async fn work_routed(
        &self,
        owner: &str,
        segment: u32,
        cursor: &mut Cursor,
        mut inbox: mpsc::Receiver<(Arc<EventEnvelope<E>>, Cursor)>,
        done: &AtomicU64,
        progress: &Progress,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        let batch_size = self.options.batch_size.max(1);
        let mut received = Vec::with_capacity(batch_size);
        let mut batch = Vec::with_capacity(batch_size);
        loop {
            let next = tokio::select! {
                _ = stop.changed() => return false,
                next = inbox.recv() => next,
            };
            // Closed: detached by the reader (or it stopped). Read alone.
            let Some(first) = next else { break };
            received.push(first);
            // What is already waiting, never waiting for more.
            while received.len() < batch_size {
                let Ok(next) = inbox.try_recv() else { break };
                received.push(next);
            }
            let count = received.len() as u64;
            for (event, read) in received.drain(..) {
                if !cursor.covers(event.position) {
                    // What the reader saw up to this event, and what this
                    // segment had seen before it was routed.
                    let seen = cursor.union(&read);
                    cursor.clone_from(&seen);
                    batch.push((event, seen));
                }
            }
            if !self.handle_batch(&batch, segment, owner, progress, stop).await {
                return false;
            }
            batch.clear();
            done.fetch_add(count, Ordering::Release);
        }
        true
    }

    /// The autonomous mode: reads the log from `cursor` on its own,
    /// handling this segment's events in batches, until stopped.
    async fn work_alone(
        &self,
        owner: &str,
        segment: u32,
        count: u32,
        mut cursor: Cursor,
        progress: &Progress,
        stop: &mut watch::Receiver<bool>,
    ) {
        let batch_size = self.options.batch_size.max(1);
        let mut batch = Vec::with_capacity(batch_size);
        let mut backoff = Backoff::new(&self.options);
        'subscribe: loop {
            let from = Start::From(cursor.clone());
            let mut events = pin!(track(
                self.store.clone(),
                &self.live,
                self.wake.clone(),
                from,
                self.options.subscription
            ));
            let mut unsaved = 0_u32;
            // What the stream yielded while a batch was gathered, other than
            // an event: handled as the next item.
            let mut held = None;
            loop {
                let next = match held.take() {
                    Some(next) => next,
                    None => tokio::select! {
                        _ = stop.changed() => return,
                        next = events.next() => next,
                        // Quiet: store how far other segments' events took us,
                        // so the processor's overall progress keeps moving.
                        () = tokio::time::sleep(IDLE_FLUSH), if unsaved > 0 => {
                            unsaved = 0;
                            if !self.store_position(segment, owner, &cursor).await {
                                return;
                            }
                            continue;
                        }
                    },
                };
                let mut item = match next {
                    Some(Ok(next)) => next,
                    Some(Err(error)) => {
                        tracing::warn!(processor = %self.name, segment, %error, "cannot read events");
                        if backoff.wait(stop).await {
                            return;
                        }
                        continue 'subscribe; // resume from the last cursor
                    }
                    None => return, // the live feed is gone: shutting down
                };

                // This event, then what is already waiting (never waiting
                // for more), up to a batch of this segment's events.
                loop {
                    let (event, seen) = item;
                    if segment_of(&event.stream, count) == segment {
                        batch.push((event, seen.clone()));
                        unsaved = 0;
                    } else {
                        // Another segment's event: only move the token now and
                        // then, once this segment's earlier events are handled.
                        unsaved += 1;
                        if unsaved >= 256 {
                            unsaved = 0;
                            if !self.handle_batch(&batch, segment, owner, progress, stop).await
                                || !self.store_position(segment, owner, &seen).await
                            {
                                return;
                            }
                            batch.clear();
                        }
                    }
                    cursor = seen;
                    if batch.len() >= batch_size {
                        break;
                    }
                    match events.next().now_or_never() {
                        Some(Some(Ok(next))) => item = next,
                        Some(other) => {
                            held = Some(other);
                            break;
                        }
                        None => break,
                    }
                }
                if !self.handle_batch(&batch, segment, owner, progress, stop).await {
                    return;
                }
                batch.clear();
                progress.advance(segment, &cursor);
            }
        }
    }

    /// The shared reader: reads the log once from `cursor` and queues each
    /// event for its segment's worker. A segment whose queue is full is
    /// detached (its queue closes, the worker reads on its own). Now and
    /// then, segments with nothing pending are moved to the read cursor.
    #[allow(clippy::too_many_arguments)]
    async fn read(
        self: Arc<Self>,
        owner: String,
        count: u32,
        mut cursor: Cursor,
        routes: Routes<E>,
        read_up_to: Arc<Mutex<Cursor>>,
        progress: Arc<Progress>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut backoff = Backoff::new(&self.options);
        let mut flush = tokio::time::interval(Duration::from_millis(100));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        'subscribe: loop {
            let from = Start::From(cursor.clone());
            let mut events = pin!(track(
                self.store.clone(),
                &self.live,
                self.wake.clone(),
                from,
                self.options.subscription
            ));
            let mut unflushed = 0_u32;
            loop {
                let next = tokio::select! {
                    _ = stop.changed() => break 'subscribe,
                    _ = flush.tick() => {
                        self.flush(&owner, &routes, &cursor, &progress).await;
                        continue;
                    }
                    next = events.next() => next,
                };
                let (event, seen) = match next {
                    Some(Ok(next)) => next,
                    Some(Err(error)) => {
                        tracing::warn!(processor = %self.name, %error, "cannot read events");
                        if backoff.wait(&mut stop).await {
                            break 'subscribe;
                        }
                        continue 'subscribe;
                    }
                    None => break 'subscribe,
                };
                let segment = segment_of(&event.stream, count);
                let sender = routes.locked().get(&segment).map(|route| route.sender.clone());
                if let Some(sender) = sender {
                    // Fast path: room in the queue, no timer. A full queue is
                    // backpressure; only a stall means stuck.
                    let sent = match sender.try_send((Arc::clone(&event), seen.clone())) {
                        Ok(()) => Ok(()),
                        Err(mpsc::error::TrySendError::Full(event)) => {
                            tokio::select! {
                                _ = stop.changed() => break 'subscribe,
                                sent = sender.send_timeout(event, self.options.segment_stall_timeout) => sent.map_err(drop),
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
                    };
                    let mut routes = routes.locked();
                    match (sent, routes.get_mut(&segment)) {
                        (Ok(()), Some(route)) => route.sent += 1,
                        // Stuck, or gone meanwhile: let it read alone.
                        (_, Some(_)) => {
                            tracing::warn!(processor = %self.name, segment, "segment stalled, reading it separately");
                            routes.remove(&segment);
                        }
                        (_, None) => {}
                    }
                }
                cursor = seen;
                cursor.clone_into(&mut read_up_to.locked());
                unflushed += 1;
                if unflushed >= 256 {
                    unflushed = 0;
                    self.flush(&owner, &routes, &cursor, &progress).await;
                }
            }
        }
        // Without a reader, every routed worker carries on alone.
        routes.locked().clear();
    }

    /// Moves the segments with nothing pending to `cursor`: every event of
    /// theirs it saw was handled.
    async fn flush(&self, owner: &str, routes: &Routes<E>, cursor: &Cursor, progress: &Progress) {
        let idle: Vec<u32> = routes
            .locked()
            .iter()
            .filter(|(_, route)| route.done.load(Ordering::Acquire) >= route.sent)
            .map(|(segment, _)| *segment)
            .collect();
        for segment in idle {
            let Some(seen) = progress.advance(segment, cursor) else {
                continue;
            };
            if !self.store_position(segment, owner, &seen).await {
                routes.locked().remove(&segment);
            }
        }
    }

    /// Handles `batch` (each event with the segment's cursor once it is
    /// handled) and moves the token past it: one unit of work for all of
    /// them when there are several, else (or once that failed) one per
    /// event. `false`: stop the worker (stopped, or the claim is lost).
    async fn handle_batch(
        &self,
        batch: &[(Arc<EventEnvelope<E>>, Cursor)],
        segment: u32,
        owner: &str,
        progress: &Progress,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        if batch.len() > 1 && !self.any_parked(batch).await {
            match self.handle_batch_in_unit_of_work(batch, segment, owner).await {
                Ok(TokenUpdate::ClaimLost) => return false,
                Ok(TokenUpdate::Stored | TokenUpdate::Untouched) => {
                    progress.fail(segment, None);
                    if let Some((_, cursor)) = batch.last() {
                        progress.advance(segment, cursor);
                    }
                    return true;
                }
                Err(error) => {
                    // Rolled back: find the failing event one by one.
                    tracing::debug!(processor = %self.name, segment, %error, "batch failed, handling its events one at a time");
                }
            }
        }
        for (event, seen) in batch {
            if !self.handle_and_store(event, seen, segment, owner, progress, stop).await {
                return false;
            }
            progress.advance(segment, seen);
        }
        true
    }

    /// Whether some event of `batch` belongs to a parked sequence: then it
    /// must be parked too, which the one-by-one path does.
    async fn any_parked(&self, batch: &[(Arc<EventEnvelope<E>>, Cursor)]) -> bool {
        if self.options.dead_letter_after.is_none() {
            return false;
        }
        for (event, _) in batch {
            if self
                .dead_letters
                .contains(&self.name, &event.stream.to_string())
                .await
                .unwrap_or(true)
            {
                return true;
            }
        }
        false
    }

    /// Every event of `batch` handled and the token moved past the last one,
    /// in one unit of work; then each one's post-commit step.
    async fn handle_batch_in_unit_of_work(
        &self,
        batch: &[(Arc<EventEnvelope<E>>, Cursor)],
        segment: u32,
        owner: &str,
    ) -> Result<TokenUpdate, HandlerError> {
        let Some((last, cursor)) = batch.last() else {
            return Ok(TokenUpdate::Untouched);
        };
        let replay_until = self.replay_until.load(Ordering::Relaxed);
        let mut tx = self.transactions.begin().await.map_err(HandlerError::from)?;
        for (event, _) in batch {
            let replay = event.position <= replay_until;
            self.handler
                .handle_in(
                    event,
                    &mut ProcessingContext::in_processor(&mut tx, replay, &self.name),
                    INTERNAL,
                )
                .await?;
        }
        let token = TrackingToken {
            processor: &self.name,
            segment,
            owner,
            replay: last.position <= replay_until,
            cursor,
        };
        if !self.tokens.store(&mut tx, token).await.map_err(HandlerError::from)? {
            return Ok(TokenUpdate::ClaimLost); // dropping `tx` rolls back
        }
        self.transactions.commit(tx).await.map_err(HandlerError::from)?;
        for (event, _) in batch {
            self.handler
                .committed_in(event, event.position <= replay_until, INTERNAL)
                .await;
        }
        Ok(TokenUpdate::Stored)
    }

    /// Handles `event` and stores the segment's token as `cursor` (what it
    /// saw once the event is handled), unless the handler stored it in its
    /// own transaction. `false`: stop the worker (stopped, or the claim is
    /// lost).
    async fn handle_and_store(
        &self,
        event: &Arc<EventEnvelope<E>>,
        cursor: &Cursor,
        segment: u32,
        owner: &str,
        progress: &Progress,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        match self.handle(event, cursor, segment, owner, progress, stop).await {
            None | Some(TokenUpdate::ClaimLost) => false,
            Some(TokenUpdate::Stored) => true,
            Some(TokenUpdate::Untouched) => self.store_position(segment, owner, cursor).await,
        }
    }

    /// Handles `event`, retrying; parks it if its aggregate already has
    /// parked events, or once it failed `dead_letter_after` times. `None`:
    /// stopped while retrying.
    async fn handle(
        &self,
        event: &Arc<EventEnvelope<E>>,
        cursor: &Cursor,
        segment: u32,
        owner: &str,
        progress: &Progress,
        stop: &mut watch::Receiver<bool>,
    ) -> Option<TokenUpdate> {
        let replay = event.position <= self.replay_until.load(Ordering::Relaxed);
        if replay && !H::REPLAY {
            return Some(TokenUpdate::Untouched); // a replayed event, for a handler that must not see it again
        }
        let sequence = event.stream.to_string();
        let token = TrackingToken {
            processor: &self.name,
            segment,
            owner,
            replay,
            cursor,
        };
        let parking = self.options.dead_letter_after.is_some();
        if parking && self.is_parked(&sequence, stop).await? {
            let letter = DeadLetter::new(
                sequence,
                Arc::clone(event),
                "an earlier event of this aggregate is parked",
            );
            return self.park(letter, token, stop).await;
        }

        let mut backoff = Backoff::new(&self.options);
        let mut attempts = 0;
        loop {
            match self.handle_in_unit_of_work(event, token).await {
                Ok(update) => {
                    progress.fail(segment, None);
                    return Some(update);
                }
                Err(error) => {
                    attempts += 1;
                    let cause = error.to_string();
                    tracing::warn!(processor = %self.name, position = event.position, attempts, %cause, "event handler failed");
                    if let Some(after) = self.options.dead_letter_after {
                        let mut letter = DeadLetter::new(sequence.clone(), Arc::clone(event), cause.clone());
                        letter.attempts = attempts;
                        let decision = match &self.policy {
                            Some(policy) => policy.decide(&letter, &error),
                            None => crate::EnqueuePolicy::<E>::decide(&crate::ParkAfter(after), &letter, &error),
                        };
                        match decision {
                            crate::EnqueueDecision::Retry => {}
                            crate::EnqueueDecision::Enqueue(diagnostics)
                            | crate::EnqueueDecision::Requeue(diagnostics) => {
                                progress.fail(segment, None);
                                letter.diagnostics = diagnostics;
                                return self.park(letter, token, stop).await;
                            }
                            crate::EnqueueDecision::Evict | crate::EnqueueDecision::Ignore => {
                                progress.fail(segment, None);
                                tracing::warn!(processor = %self.name, position = event.position, %cause, "event given up by the enqueue policy");
                                return Some(TokenUpdate::Untouched); // the segment moves past it
                            }
                        }
                    }
                    progress.fail(segment, Some(cause));
                    if backoff.wait(stop).await {
                        return None;
                    }
                }
            }
        }
    }

    /// One attempt: `event` handled and the token stored in one unit of
    /// work, committed together; then the handler's post-commit step. A
    /// lost claim rolls the unit back.
    async fn handle_in_unit_of_work(
        &self,
        event: &EventEnvelope<E>,
        token: TrackingToken<'_>,
    ) -> Result<TokenUpdate, HandlerError> {
        let mut tx = self.transactions.begin().await.map_err(HandlerError::from)?;
        self.handler
            .handle_in(
                event,
                &mut ProcessingContext::in_processor(&mut tx, token.replay, token.processor),
                INTERNAL,
            )
            .await?;
        if !self.tokens.store(&mut tx, token).await.map_err(HandlerError::from)? {
            // Another instance owns the segment: dropping `tx` rolls back.
            return Ok(TokenUpdate::ClaimLost);
        }
        self.transactions.commit(tx).await.map_err(HandlerError::from)?;
        self.handler.committed_in(event, token.replay, INTERNAL).await;
        Ok(TokenUpdate::Stored)
    }

    /// Whether `sequence` has parked events. A failing lookup is not "no":
    /// the event would overtake the parked ones. It is asked again with the
    /// same backoff as parking. `None`: stopped while retrying.
    async fn is_parked(&self, sequence: &str, stop: &mut watch::Receiver<bool>) -> Option<bool> {
        let mut backoff = Backoff::new(&self.options);
        loop {
            match self.dead_letters.contains(&self.name, sequence).await {
                Ok(parked) => return Some(parked),
                Err(error) => {
                    tracing::warn!(processor = %self.name, %error, "cannot look the sequence up in the dead letters");
                    if backoff.wait(stop).await {
                        return None;
                    }
                }
            }
        }
    }

    /// Parks `event` and moves the segment's token past it, in one unit of
    /// work, retrying with backoff if the queue or the token store fails.
    /// `None`: stopped while retrying.
    async fn park(
        &self,
        letter: DeadLetter<E>,
        token: TrackingToken<'_>,
        stop: &mut watch::Receiver<bool>,
    ) -> Option<TokenUpdate> {
        let mut backoff = Backoff::new(&self.options);
        loop {
            match self.park_once(letter.clone(), token).await {
                Ok(update) => return Some(update),
                Err(error) => {
                    tracing::warn!(processor = %self.name, %error, "cannot park the event");
                    if backoff.wait(stop).await {
                        return None;
                    }
                }
            }
        }
    }

    /// One attempt at [`park`](Self::park). A lost claim rolls the letter
    /// back: whoever holds the segment now parks it.
    async fn park_once(&self, mut letter: DeadLetter<E>, token: TrackingToken<'_>) -> Result<TokenUpdate, StoreError> {
        let mut tx = self.transactions.begin().await?;
        letter.parked_at = Some(std::time::SystemTime::now());
        self.dead_letters.enqueue_in(&mut tx, &self.name, letter).await?;
        if !self.tokens.store(&mut tx, token).await? {
            return Ok(TokenUpdate::ClaimLost); // dropping `tx` rolls the letter back
        }
        self.transactions.commit(tx).await?;
        Ok(TokenUpdate::Stored)
    }

    /// Stores the segment's cursor, in a unit of work of its own (no event
    /// was handled: other segments' events moved it). `false`: the claim
    /// was lost.
    async fn store_position(&self, segment: u32, owner: &str, cursor: &Cursor) -> bool {
        let token = TrackingToken {
            processor: &self.name,
            segment,
            owner,
            replay: false,
            cursor,
        };
        let stored = async {
            let mut tx = self.transactions.begin().await?;
            let owned = self.tokens.store(&mut tx, token).await?;
            if owned {
                self.transactions.commit(tx).await?;
            }
            Ok::<_, StoreError>(owned)
        };
        match stored.await {
            Ok(owned) => owned,
            Err(error) => {
                // Only means handling events again after a restart.
                tracing::warn!(processor = %self.name, segment, %error, "cannot store the position");
                true
            }
        }
    }
}

/// A routed segment, as the shared reader sees it.
struct Route<E> {
    /// The worker's queue: each event, with the reader's cursor once read.
    sender: mpsc::Sender<(Arc<EventEnvelope<E>>, Cursor)>,
    /// What the segment had seen when it was routed.
    from: Cursor,
    /// Events queued for it.
    sent: u64,
    /// Events it finished.
    done: Arc<AtomicU64>,
}

/// The routed segments, shared by the coordinator and the reader.
type Routes<E> = Arc<Mutex<HashMap<u32, Route<E>>>>;

/// How long a worker waits for its next event before storing how far
/// other segments' events took it.
const IDLE_FLUSH: Duration = Duration::from_millis(100);

/// Per-segment progress of one instance, published as its status.
struct Progress {
    /// Cursor and current failure of each owned segment.
    segments: Mutex<BTreeMap<u32, (Cursor, Option<String>)>>,
    /// Where the summary is published.
    status: watch::Sender<ProcessorStatus>,
}

impl Progress {
    /// Republishes the whole status: on segment ownership changes only.
    fn rebuild(&self, change: impl FnOnce(&mut BTreeMap<u32, (Cursor, Option<String>)>)) {
        let mut segments = self.segments.locked();
        change(&mut segments);
        self.status.send_modify(|status| {
            status.position = lowest(&segments);
            status.failing = segments.values().find_map(|(_, failing)| failing.clone());
            status.segments = segments.keys().copied().collect();
            status.cursors = segments.values().map(|(cursor, _)| cursor.clone()).collect();
        });
    }

    /// The processor has `count` segments.
    fn count(&self, count: u32) {
        self.status.send_modify(|status| status.segment_count = count);
    }

    /// Every segment's stored cursor, as just read: watchers are woken only
    /// if it moved.
    fn share(&self, cursors: Vec<Cursor>) {
        self.status.send_if_modified(|status| {
            let moved = status.shared_cursors != cursors;
            status.shared_cursors = cursors;
            moved
        });
    }

    fn track(&self, segment: u32, cursor: Cursor) {
        self.rebuild(|segments| {
            segments.insert(segment, (cursor, None));
        });
    }

    fn forget(&self, segment: u32) {
        self.rebuild(|segments| {
            segments.remove(&segment);
        });
    }

    /// Adds what `seen` saw to the segment's cursor and returns it (`None`
    /// if the segment is not owned). Watchers are woken only if it moved.
    fn advance(&self, segment: u32, seen: &Cursor) -> Option<Cursor> {
        let mut segments = self.segments.locked();
        let index = segments.keys().position(|owned| *owned == segment)?;
        let state = segments.get_mut(&segment)?;
        if state.0.includes(seen) {
            return Some(state.0.clone());
        }
        state.0 = state.0.union(seen);
        let cursor = state.0.clone();
        let lowest = lowest(&segments);
        self.status
            .send_if_modified(|status| match status.cursors.get_mut(index) {
                Some(published) => {
                    cursor.clone_into(published);
                    status.position = lowest;
                    true
                }
                None => false,
            });
        Some(cursor)
    }

    fn fail(&self, segment: u32, failing: Option<String>) {
        let mut segments = self.segments.locked();
        let Some(state) = segments.get_mut(&segment) else {
            return;
        };
        if state.1 == failing {
            return;
        }
        state.1 = failing;
        let failing = segments.values().find_map(|(_, failing)| failing.clone());
        self.status.send_modify(|status| status.failing = failing);
    }
}

/// The lowest watermark of the owned segments (0 when none).
fn lowest(segments: &BTreeMap<u32, (Cursor, Option<String>)>) -> u64 {
    segments
        .values()
        .map(|(cursor, _)| cursor.watermark())
        .min()
        .unwrap_or(0)
}

/// Controls a spawned [`TrackingProcessor`] instance.
#[must_use = "dropping the handle leaves the processor running, with no way to stop it"]
pub struct ProcessorHandle {
    /// The processor's name.
    name: String,
    /// Its progress.
    status: watch::Receiver<ProcessorStatus>,
    /// Set to `true` to stop it.
    stop: watch::Sender<bool>,
    /// The coordinator task (its workers end with it).
    task: tokio::task::JoinHandle<()>,
}

impl ::std::fmt::Debug for ProcessorHandle {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("ProcessorHandle").finish_non_exhaustive()
    }
}

impl ProcessorHandle {
    /// The processor's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its current progress.
    pub fn status(&self) -> ProcessorStatus {
        self.status.borrow().clone()
    }

    /// Follows its progress, e.g. to wait until it handled a position from
    /// another task (`wait_for(|status| status.covers(p))`).
    pub fn progress(&self) -> watch::Receiver<ProcessorStatus> {
        self.status.clone()
    }

    /// Waits until the segments this instance owns saw the event at
    /// `position` (its own segment handled it, and every event before it
    /// that was committed then): the way to read your own writes from a
    /// read model. Returns at once if they did; waits forever if it stops
    /// before.
    pub async fn caught_up(&self, position: u64) {
        let mut status = self.status.clone();
        let ready = |status: &ProcessorStatus| status.covers(position);
        // An error only means the processor stopped: nothing more will come.
        if status.wait_for(ready).await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Stops the processor after the events it is handling, waits for it,
    /// and releases its segments for other instances.
    pub async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }

    /// Kills the processor without releasing its segments, as a crash would,
    /// and waits until it no longer runs: other instances take its segments
    /// over once the claims expire. Events it was handling may have been
    /// handled (and their positions stored) just before.
    pub async fn abort(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// A unique claim owner for this processor instance.
fn owner_id(processor: &str) -> String {
    static INSTANCES: AtomicU64 = AtomicU64::new(0);
    let instance = INSTANCES.fetch_add(1, Ordering::Relaxed);
    format!("{processor}@{}-{instance}", std::process::id())
}

/// Milliseconds on the runtime's clock (paused in tests), for claims.
fn clock() -> u64 {
    static EPOCH: OnceLock<tokio::time::Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(tokio::time::Instant::now);
    tokio::time::Instant::now().saturating_duration_since(epoch).as_millis() as u64
}

/// Exponential backoff between retries, interruptible by a stop request.
struct Backoff {
    /// Next wait.
    next: Duration,
    /// Longest wait.
    max: Duration,
}

impl Backoff {
    fn new(options: &TrackingOptions) -> Self {
        Self {
            next: options.retry_initial,
            max: options.retry_max,
        }
    }

    /// Sleeps for the next delay; `true` if a stop was requested meanwhile.
    async fn wait(&mut self, stop: &mut watch::Receiver<bool>) -> bool {
        let delay = self.next;
        self.next = (self.next * 2).min(self.max);
        tokio::select! {
            _ = stop.changed() => true,
            () = tokio::time::sleep(delay) => false,
        }
    }
}

/// Why [`TrackingProcessor::reset`] did not reset.
#[derive(Debug)]
#[non_exhaustive]
pub enum ResetError {
    /// An instance still holds a segment: stop every instance first.
    Running,
    /// The token or event store failed.
    Store(StoreError),
    /// The handler's [`reset`](EventHandler::reset) failed.
    Handler(HandlerError),
}

impl From<StoreError> for ResetError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl std::fmt::Display for ResetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => f.write_str("the processor still runs somewhere: stop it first"),
            Self::Store(error) => write!(f, "cannot reset the tokens: {error}"),
            Self::Handler(error) => write!(f, "the handler failed to reset: {error}"),
        }
    }
}

impl std::error::Error for ResetError {}

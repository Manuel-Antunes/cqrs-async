//! The CQRS unit of an application (or of one module of it): its command and
//! query gateways, its events and the processors that follow them, built in
//! one place by a typed builder.
//!
//! ```text
//! let store = InMemoryEventStore::new();
//! let posts = Arc::new(EventSourcedPostRepository::new(store.clone()));  // yours, over the store
//!
//! let app = Cqrs::new(Events::new(store))                       // in-memory tokens, no transactions
//!     .commands(|events| PostCommandGateway::new(posts, events)) // the unit hands over the publisher
//!     .intercept(Audit(log))
//!     .queries(PostQueryGateway::new(summaries.clone()))
//!     .projection(PostSummaryProjection::new(summaries, updates.clone()))
//!     .policy(NotifyOnPublish::new(notifier))
//!     .subscriptions(updates)                                 // queries wait for the projections
//!     .build();
//!
//! app.commands().send(CreatePost { .. }).await?;
//! app.queries().subscribe(FindPost { id });
//! ```
//!
//! The unit is sugar over what can be assembled by hand: it starts from an
//! [`Events`](crate::Events) (the store, where tokens live and the
//! transactions processors run in), which is also the publisher (append,
//! then broadcast); `commands` hands it to the closure that builds the
//! command gateway, whose handlers hold it and commit with
//! `events.commit(entity, &self.events)`.
//! [`Cqrs::from_publisher`] starts from a publisher built by hand instead
//! (e.g. an [`InlinePublisher`](crate::InlinePublisher) feeding handlers
//! inline, with no runtime).
//!
//! Each step changes the builder's type, so what can be called next is
//! checked at compile time (`projection` needs events), and the unit it
//! builds holds the concrete types: calls through it are as static as
//! calls on the gateways themselves. An application names what it exposes
//! with the port traits its message enums generate
//! (`#[commands(port = PostCommands)]`), not with the unit's type.

use std::sync::Arc;

use crate::Intercepted;
#[cfg(feature = "tokio")]
use crate::{
    DomainEvent, EventFeed, EventHandler, EventStore, Events, LocalProgress, ProcessorHandle, Projected, Projection,
    ProjectionProgress, SubscriptionQueries, TokenStore, TrackingOptions, TransactionManager,
};

/// An application's CQRS unit: the command gateway `C`, the query gateway
/// `Q` and its events `E` (`()` without asynchronous processing). Built with
/// [`Cqrs::new`].
pub struct Cqrs<C, Q, E = ()> {
    /// Routes the commands.
    commands: C,
    /// Routes the queries (and their subscriptions).
    queries: Q,
    /// The committed events.
    events: E,
    /// The processors the unit started (nothing without `tokio`).
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    processing: Processing,
}

/// The processors a unit started, and their progress.
#[cfg(feature = "tokio")]
#[derive(Default)]
struct Processing {
    /// Settings for the processors started from now on.
    options: TrackingOptions,
    /// Every processor's progress, for [`Cqrs::settled`].
    all: Vec<LocalProgress>,
    /// The projections' progress: what subscriptions wait for.
    projections: Vec<LocalProgress>,
    /// The running processors, taken by [`Cqrs::stop`].
    handles: std::sync::Mutex<Vec<ProcessorHandle>>,
}

/// Without `tokio` there is nothing to start.
#[cfg(not(feature = "tokio"))]
#[derive(Default)]
struct Processing;

impl Cqrs<(), (), ()> {
    /// Starts building a unit over `events`, assembled by hand: its store,
    /// its processors' tokens and the transactions they run in
    /// (`Events::new(store).with_tokens(..).with_transactions(..)`). They are
    /// also the publisher its command handlers commit through.
    // The unit's constructor: what it builds is configured step by step.
    #[cfg(feature = "tokio")]
    #[allow(clippy::new_ret_no_self, clippy::type_complexity)]
    pub fn new<Ev, S, T, M>(
        events: Events<Ev, S, T, M>,
    ) -> CqrsBuilder<Events<Ev, S, T, M>, (), (), Events<Ev, S, T, M>>
    where
        S: Clone,
        T: Clone,
        M: Clone,
    {
        CqrsBuilder {
            publisher: Arc::new(events.clone()),
            commands: (),
            queries: (),
            events,
            updates: (),
            processing: Processing::default(),
        }
    }

    /// Starts building a unit whose command handlers commit through
    /// `publisher`, built by hand: e.g. an
    /// [`InlinePublisher`](crate::InlinePublisher) feeding handlers inline, with
    /// no runtime.
    // Without `tokio`, `Processing` is a unit struct.
    #[allow(clippy::default_constructed_unit_structs)]
    pub fn from_publisher<P>(publisher: P) -> CqrsBuilder<P> {
        CqrsBuilder {
            publisher: Arc::new(publisher),
            commands: (),
            queries: (),
            events: (),
            updates: (),
            processing: Processing::default(),
        }
    }
}

impl<C, Q, E> Cqrs<C, Q, E> {
    /// Where commands are sent: `unit.commands().send(..)`.
    pub fn commands(&self) -> &C {
        &self.commands
    }

    /// Where queries are asked, and subscribed to if the unit has
    /// [`subscriptions`](CqrsBuilder::subscriptions).
    pub fn queries(&self) -> &Q {
        &self.queries
    }

    /// The committed events: an [`EventFeed`](crate::EventFeed) with the
    /// `tokio` feature's `Events`.
    pub fn events(&self) -> &E {
        &self.events
    }

    /// Stops every processor the unit started, each after its current
    /// event. Later calls do nothing.
    #[cfg(feature = "tokio")]
    pub async fn stop(&self) {
        let handles = std::mem::take(&mut *self.processing.handles.lock().expect("processors poisoned"));
        for handle in handles {
            handle.stop().await;
        }
    }
}

#[cfg(feature = "tokio")]
impl<C, Q, Ev, S, T, M> Cqrs<C, Q, Events<Ev, S, T, M>>
where
    Ev: DomainEvent,
    S: EventStore<Ev> + Clone + 'static,
    T: Send + Sync,
    M: Send + Sync,
{
    /// Returns once every processor of the unit handled every event
    /// committed before the call: queries then see every change made
    /// before it. A failing read of the head returns at once.
    pub async fn settled(&self) {
        let Ok(head) = self.events.last_position().await else {
            return;
        };
        for progress in &self.processing.all {
            progress.reached(head).await;
        }
    }
}

/// Builds a [`Cqrs`] unit step by step; each step's type says what is
/// set. `C` is the commands, `Q` the queries, `E` the events and `U` the
/// query update emitters, `()` until set.
///
/// A step that needs another one does not exist before it. The command
/// gateway is set once:
///
/// ```compile_fail,E0599
/// let _ = cqrs::Cqrs::from_publisher(()).commands(|_| 1_u8).commands(|_| 2_u8);
/// ```
///
/// and processors follow the unit's events, so a unit without them has
/// none:
///
/// ```compile_fail,E0599
/// # #[derive(Clone, cqrs::DomainEvent, cqrs::TagResolver)] struct Ticked;
/// # struct Counter;
/// # #[cqrs::projection(namespace = "counter")]
/// # impl cqrs::EventHandler for Counter {
/// #     async fn handle(&self, _: &cqrs::EventEnvelope<Ticked>, _: &mut cqrs::ProcessingContext<'_>) -> Result<(), cqrs::HandlerError> { Ok(()) }
/// # }
/// let _ = cqrs::Cqrs::from_publisher(()).projection(Counter);
/// ```
pub struct CqrsBuilder<P, C = (), Q = (), E = (), U = ()> {
    /// What the command handlers commit through.
    publisher: Arc<P>,
    /// The command gateway.
    commands: C,
    /// The query gateway.
    queries: Q,
    /// The events.
    events: E,
    /// Where projections push query updates.
    updates: U,
    /// The processors started so far.
    processing: Processing,
}

impl<P, Q, E, U> CqrsBuilder<P, (), Q, E, U> {
    /// The command gateway, built by `gateway` from the publisher the unit
    /// makes (`|events| PostCommandGateway::new(posts, events)`): the
    /// handlers hold it and commit through it. It is shared, so the gateway
    /// takes it as an `Arc<P>`, which is a publisher too.
    pub fn commands<G>(self, gateway: impl FnOnce(Arc<P>) -> G) -> CqrsBuilder<P, G, Q, E, U> {
        let CqrsBuilder {
            publisher,
            queries,
            events,
            updates,
            processing,
            ..
        } = self;
        let commands = gateway(Arc::clone(&publisher));
        CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        }
    }
}

impl<P, C, Q, E, U> CqrsBuilder<P, C, Q, E, U> {
    /// Every command goes through `interceptor` (a tuple chains several).
    pub fn intercept<I>(self, interceptor: I) -> CqrsBuilder<P, Intercepted<C, I>, Q, E, U> {
        let CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        } = self;
        let commands = Intercepted::new(commands, interceptor);
        CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        }
    }
}

impl<P, C, E, U> CqrsBuilder<P, C, (), E, U> {
    /// The query gateway (built over the read models it answers from).
    pub fn queries<Q>(self, queries: Q) -> CqrsBuilder<P, C, Q, E, U> {
        let CqrsBuilder {
            publisher,
            commands,
            events,
            updates,
            processing,
            ..
        } = self;
        CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        }
    }
}

impl<P, C, Q, U> CqrsBuilder<P, C, Q, (), U> {
    /// The events of a unit built [`from_publisher`](Cqrs::from_publisher):
    /// with the `tokio` feature's `Events`, the unit can start processors
    /// and serve subscriptions.
    pub fn events<E>(self, events: E) -> CqrsBuilder<P, C, Q, E, U> {
        let CqrsBuilder {
            publisher,
            commands,
            queries,
            updates,
            processing,
            ..
        } = self;
        CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        }
    }
}

#[cfg(feature = "tokio")]
impl<P, C, Q, Ev, S, T, M, U> CqrsBuilder<P, C, Q, Events<Ev, S, T, M>, U>
where
    Ev: DomainEvent + Clone,
    S: EventStore<Ev> + Clone + 'static,
    T: TokenStore<Tx = M::Tx> + Clone + 'static,
    M: TransactionManager + Clone + 'static,
{
    /// Settings (segments, batch size, dead letters...) for the processors
    /// started after this call.
    #[must_use]
    pub fn options(mut self, options: TrackingOptions) -> Self {
        self.processing.options = options;
        self
    }

    /// Starts a processor running `projection` (named by its namespace).
    /// The unit's subscriptions wait for it before their first result.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    #[must_use]
    pub fn projection<H>(mut self, projection: H) -> Self
    where
        H: Projection + EventHandler<Ev, M::Tx> + 'static,
    {
        let progress = self.start(projection);
        self.processing.projections.push(progress);
        self
    }

    /// Starts a processor running `policy` (named by its namespace): a
    /// side effect, which subscriptions do not wait for.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    #[must_use]
    pub fn policy<H>(mut self, policy: H) -> Self
    where
        H: Projection + EventHandler<Ev, M::Tx> + 'static,
    {
        self.start(policy);
        self
    }

    /// Spawns `handler`'s processor and keeps its handle.
    fn start<H>(&mut self, handler: H) -> LocalProgress
    where
        H: Projection + EventHandler<Ev, M::Tx> + 'static,
    {
        let handle = self
            .events
            .projection(handler)
            .with_options(self.processing.options)
            .spawn();
        let progress = LocalProgress::of(&handle);
        self.processing.all.push(progress.clone());
        self.processing
            .handles
            .get_mut()
            .expect("processors poisoned")
            .push(handle);
        progress
    }
}

#[cfg(feature = "tokio")]
impl<P, C, Q, E> CqrsBuilder<P, C, Q, E, ()> {
    /// Makes the queries subscribable: the current result once every
    /// projection of the unit handled what was committed when the
    /// subscription started (a client sees its own commands), then what
    /// the projections emit through `updates` (Axon's subscription
    /// queries).
    pub fn subscriptions<U>(self, updates: Arc<U>) -> CqrsBuilder<P, C, Q, E, Arc<U>> {
        let CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            processing,
            ..
        } = self;
        CqrsBuilder {
            publisher,
            commands,
            queries,
            events,
            updates,
            processing,
        }
    }
}

impl<P, C, Q, E> CqrsBuilder<P, C, Q, E, ()> {
    /// The unit.
    pub fn build(self) -> Cqrs<C, Q, E> {
        let CqrsBuilder {
            commands,
            queries,
            events,
            processing,
            ..
        } = self;
        Cqrs {
            commands,
            queries,
            events,
            processing,
        }
    }
}

#[cfg(feature = "tokio")]
impl<P, C, Q, Ev, S, T, M, U> CqrsBuilder<P, C, Q, Events<Ev, S, T, M>, Arc<U>>
where
    Ev: DomainEvent,
    S: EventStore<Ev> + Clone + 'static,
    T: Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// The unit, its queries subscribable.
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Cqrs<C, SubscriptionQueries<Q, U, Projected<Events<Ev, S, T, M>, Vec<LocalProgress>, Ev>>, Events<Ev, S, T, M>>
    {
        let CqrsBuilder {
            commands,
            queries,
            events,
            updates,
            processing,
            ..
        } = self;
        let queries =
            SubscriptionQueries::new(queries, updates).after_projection(events.clone(), processing.projections.clone());
        Cqrs {
            commands,
            queries,
            events,
            processing,
        }
    }
}

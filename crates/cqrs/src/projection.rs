//! Projections (Axon 5's event handling components, grouped by namespace):
//! one per aggregate root, a struct holding what its handlers need, with
//! one event handler per namespace. Each namespace runs in a processor of
//! its own (its token, its unit of work), named `"{projection}.{namespace}"`.
//!
//! ```
//! # use std::sync::Mutex;
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created, Published }
//! #[cqrs::projection(name = "post", events = PostEvent, namespaces("counts", "notifications"))]
//! #[derive(Default)]
//! struct PostProjection {
//!     published: Mutex<u64>,
//!     sent: Mutex<Vec<String>>,
//! }
//!
//! /// A read model: rebuilt by a replay. Everything it does is undone with
//! /// its transaction, so it takes events by batches.
//! #[cqrs::event_handler(namespace = "counts", batch = 64)]
//! impl EventHandler for PostProjection {
//!     async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
//!         if let PostEvent::Published = event.payload {
//!             *self.published.lock().unwrap() += 1;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! /// A side effect: never repeated by a replay.
//! #[cqrs::event_handler(namespace = "notifications")]
//! impl EventHandler for PostProjection {
//!     const REPLAY: bool = false;
//!
//!     async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
//!         if let PostEvent::Published = event.payload {
//!             self.sent.lock().unwrap().push(format!("{} published", event.stream));
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # use cqrs::Projection;
//! assert_eq!(PostProjection::NAMESPACES, ["counts", "notifications"]);
//! // Cqrs::new(events).projection(PostProjection::default()) runs both.
//! ```
//!
//! The compiler checks both ways. A namespace listed without its handler:
//!
//! ```compile_fail,E0277
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created }
//! #[cqrs::projection(name = "post", events = PostEvent, namespaces("counts", "notifications"))]
//! struct PostProjection;
//!
//! #[cqrs::event_handler(namespace = "counts")]
//! impl EventHandler for PostProjection {
//!     async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> { Ok(()) }
//! }
//!
//! fn runs<P: cqrs::Route<()>>(_: P) {}
//! runs(PostProjection); // "notifications" has no handler
//! ```
//!
//! a batch on a handler of no namespace (a processor's batch is in its
//! options):
//!
//! ```compile_fail
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created }
//! struct Counts;
//!
//! #[cqrs::event_handler(batch = 8)]
//! impl EventHandler for Counts {
//!     async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> { Ok(()) }
//! }
//! ```
//!
//! and a handler for a namespace the projection does not list:
//!
//! ```compile_fail,E0277
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created }
//! #[cqrs::projection(name = "post", events = PostEvent, namespaces("counts"))]
//! struct PostProjection;
//!
//! #[cqrs::event_handler(namespace = "notificatons")]
//! impl EventHandler for PostProjection {
//!     async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> { Ok(()) }
//! }
//! ```
//!
//! # Subscription queries
//!
//! `updates(..)` lists the queries the projection's changes are pushed to
//! (Axon's subscription queries): it gets a channel per query, named
//! `QueryUpdateEmitter<Self>`, and a namespace emits through them in its
//! `updated`, on every instance, once it handled the event. The unit's
//! queries subscribe to them (`app.queries().subscribe(FindPost { id })`),
//! and `subscriptions = Name` names the trait of whatever does:
//!
//! ```text
//! #[cqrs::projection(name = "post", events = PostEvent, namespaces("summaries"),
//!                    updates(FindPost, ListPublished), subscriptions = PostSubscriptions)]
//! struct PostProjection<S> { summaries: Arc<S> }
//!
//! #[cqrs::event_handler(namespace = "summaries")]
//! impl<S, Tx> EventHandler for PostProjection<S> {
//!     async fn handle(..) { .. }
//!
//!     async fn updated(&self, event: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
//!         updates.emit(|query: &FindPost| query.id == id, Some(summary));   // the channel of `FindPost`
//!     }
//! }
//! ```
//!
//! Emitting a query the projection does not list does not compile:
//!
//! ```compile_fail,E0277
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext, QueryUpdateEmitter};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created }
//! # #[cqrs::query(output = u64, error = ())] #[derive(Clone)] struct Count;
//! # #[cqrs::query(output = u64, error = ())] #[derive(Clone)] struct Total;
//! # #[cqrs::query(output = u64, error = ())] #[derive(Clone)] struct Max;
//! #[cqrs::projection(name = "post", events = PostEvent, namespaces("counts"), updates(Count, Max))]
//! struct PostProjection;
//!
//! #[cqrs::event_handler(namespace = "counts")]
//! impl EventHandler for PostProjection {
//!     async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> { Ok(()) }
//!
//!     async fn updated(&self, _: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
//!         updates.emit(|_: &Total| true, 1); // not in `updates(..)`
//!     }
//! }
//! ```
//!
//! and neither does an `updated` on a handler of no namespace (it has no
//! projection to emit through):
//!
//! ```compile_fail
//! # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
//! # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created }
//! struct Counts;
//!
//! #[cqrs::event_handler]
//! impl EventHandler for Counts {
//!     async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> { Ok(()) }
//!     async fn updated(&self, _: &EventEnvelope<PostEvent>, _: &()) {}
//! }
//! ```
//!
//! The unit of work belongs to the processor's
//! [`TransactionManager`](crate::TransactionManager), not to the
//! projection: a handler writing SQL names the database's transaction in
//! its context (`ProcessingContext<'_, PgTransaction>`); one that does not
//! runs with any.

use std::{future::Future, sync::Arc};

use crate::{DomainEvent, EventEnvelope, EventHandler, HandlerError, ProcessingContext};

/// A projection: a name, the events it is built from, and its namespaces,
/// each run by a processor of its own. Implemented by
/// `#[cqrs::projection(name = "...", events = Event, namespaces(...))]`.
pub trait Projection: HasUpdates + Send + Sync + 'static {
    /// Its name: the prefix of its namespaces' processors and tokens, so two
    /// projections never share one.
    const NAME: &'static str;

    /// The events it is built from (one aggregate's event enum).
    type Event: DomainEvent;

    /// Its namespaces, in order: one handler and one processor each.
    const NAMESPACES: &'static [&'static str];

    /// The processor (and token key) of `namespace`:
    /// `"{NAME}.{namespace}"`.
    fn processor_name(namespace: usize) -> String {
        format!("{}.{}", Self::NAME, Self::NAMESPACES[namespace])
    }
}

/// A projection whose every namespace has its handler, for units of work
/// `Tx`: what runs it. Implemented by `#[cqrs::projection]`, for the units
/// of work every handler of the projection accepts.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot run in units of work `{Tx}`",
    note = "each namespace its `#[cqrs::projection]` lists needs a `#[cqrs::event_handler(namespace = \"...\")] impl EventHandler for {Self}`, in units of work `{Tx}`"
)]
pub trait Route<Tx: Send>: Projection {
    /// Hands `event` to the handler of `namespace` (an index into
    /// [`NAMESPACES`](Projection::NAMESPACES)).
    fn handle(
        &self,
        namespace: usize,
        event: &EventEnvelope<Self::Event>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;

    /// The handler of `namespace`'s post-commit step.
    fn committed(&self, namespace: usize, event: &EventEnvelope<Self::Event>) -> impl Future<Output = ()> + Send;

    /// The handler of `namespace`'s reset, before a replay.
    fn reset(
        &self,
        namespace: usize,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;

    /// Whether the handler of `namespace` sees replays
    /// ([`EventHandler::REPLAY`]): a read model does, a side effect does not.
    fn replays(namespace: usize) -> bool;

    /// The events per unit of work `namespace` asks for
    /// (`#[cqrs::event_handler(namespace = "...", batch = N)]`): it wins over
    /// the processors' options. `None`: theirs.
    fn batch_size(namespace: usize) -> Option<usize>;

    /// Whether the handler of `namespace` emits query updates (it has an
    /// `updated`): only those get an emitter on each instance.
    fn emits(namespace: usize) -> bool;

    /// The handler of `namespace`'s `updated`: on every instance, once the
    /// namespace handled `event`, it emits through `updates`.
    fn updated(
        &self,
        namespace: usize,
        event: &EventEnvelope<Self::Event>,
        updates: &<Self as HasUpdates>::Updates,
    ) -> impl Future<Output = ()> + Send;
}

/// The channels of a projection's subscription queries
/// ([`QueryUpdateEmitter<Self>`](crate::QueryUpdateEmitter)): written by
/// `#[cqrs::projection(.., updates(FindPost, ..))]`, a channel per query;
/// `()` without `updates`. Implemented without bounds, so the handlers of
/// a generic projection can name it.
#[doc(hidden)]
pub trait HasUpdates {
    /// The channels.
    type Updates: Default + Send + Sync + 'static;
}

/// The handler of one namespace of a projection, the namespace named by
/// its ID ([`namespace_id`]): what `#[cqrs::event_handler(namespace =
/// "...")]` turns an `impl EventHandler` into. Its methods are
/// [`EventHandler`]'s.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no handler for one of the namespaces its projection lists",
    note = "add `#[cqrs::event_handler(namespace = \"...\")] impl EventHandler for {Self}` for it"
)]
pub trait NamespaceHandler<const ID: u64, E: DomainEvent, Tx: Send = ()>:
    HasNamespace<ID> + HasUpdates + Send + Sync
{
    /// As [`EventHandler::REPLAY`].
    const REPLAY: bool = true;

    /// Events handled in one unit of work, with one token update, when the
    /// namespace sets it (`batch = N`). Larger is much faster, as long as
    /// everything the handler does is undone with its transaction: a batch
    /// that fails is rolled back and handled again one event at a time, so
    /// a side effect (a notification) would happen again. `None`: the
    /// processors' options (1 by default, as Axon's).
    const BATCH_SIZE: Option<usize> = None;

    /// As [`EventHandler::handle`].
    fn handle(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;

    /// As [`EventHandler::committed`].
    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send {
        let _ = event;
        async {}
    }

    /// As [`EventHandler::reset`].
    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        let _ = context;
        async { Ok(()) }
    }

    /// Whether it has an [`updated`](Self::updated) (set by the macro).
    const EMITS: bool = false;

    /// Emits the query updates `event` makes, through the projection's
    /// channels (`updates.emit(|query: &FindPost| .., result)`): on
    /// **every** instance, once this namespace handled the event, whichever
    /// instance did, so a subscriber connected to any instance sees every
    /// change. After the commit, outside the unit of work; not in replays
    /// (it follows the events committed from the start of the instance).
    /// Default: nothing.
    fn updated(
        &self,
        event: &EventEnvelope<E>,
        updates: &<Self as HasUpdates>::Updates,
    ) -> impl Future<Output = ()> + Send {
        let _ = (event, updates);
        async {}
    }
}

/// A projection lists the namespace with this ID: what a namespace's
/// handler requires, so one for a namespace the projection does not list
/// does not compile. Implemented by `#[cqrs::projection]`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not list this namespace",
    note = "add it to `namespaces(...)` in the `#[cqrs::projection]` of `{Self}` (check its spelling)"
)]
pub trait HasNamespace<const ID: u64> {
    /// The namespace.
    const NAME: &'static str;
}

/// The ID of the namespace `name`: FNV-1a of its bytes, computed at compile
/// time, as `&str` cannot be a type parameter on stable Rust.
#[doc(hidden)]
pub const fn namespace_id(name: &str) -> u64 {
    let bytes = name.as_bytes();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut index = 0;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        index += 1;
    }
    hash
}

/// One namespace of a projection, as the event handler its processor runs.
#[cfg_attr(not(feature = "tokio"), allow(dead_code))] // processors need Tokio
pub(crate) struct NamespaceOf<P> {
    /// The projection, shared by its namespaces' processors.
    pub(crate) projection: Arc<P>,
    /// Which namespace.
    pub(crate) namespace: usize,
}

impl<P: Route<Tx>, Tx: Send> EventHandler<P::Event, Tx> for NamespaceOf<P> {
    // Decided per namespace, at run time: see `handle_in`.
    const REPLAY: bool = true;

    fn handle(
        &self,
        event: &EventEnvelope<P::Event>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.projection.handle(self.namespace, event, context)
    }

    fn committed(&self, event: &EventEnvelope<P::Event>) -> impl Future<Output = ()> + Send {
        self.projection.committed(self.namespace, event)
    }

    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.projection.reset(self.namespace, context)
    }

    async fn handle_in(
        &self,
        event: &EventEnvelope<P::Event>,
        context: &mut ProcessingContext<'_, Tx>,
        _: crate::message::sealed::Internal,
    ) -> Result<(), HandlerError> {
        if context.replay() && !P::replays(self.namespace) {
            return Ok(());
        }
        self.projection.handle(self.namespace, event, context).await
    }

    async fn committed_in(&self, event: &EventEnvelope<P::Event>, replay: bool, _: crate::message::sealed::Internal) {
        if !replay || P::replays(self.namespace) {
            self.projection.committed(self.namespace, event).await;
        }
    }
}

/// A shared projection is the projection.
impl<P: HasUpdates> HasUpdates for Arc<P> {
    type Updates = P::Updates;
}

impl<P: Projection> Projection for Arc<P> {
    const NAME: &'static str = P::NAME;
    type Event = P::Event;
    const NAMESPACES: &'static [&'static str] = P::NAMESPACES;
}

/// A shared projection runs as the projection.
impl<P: Route<Tx>, Tx: Send> Route<Tx> for Arc<P> {
    fn handle(
        &self,
        namespace: usize,
        event: &EventEnvelope<Self::Event>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).handle(namespace, event, context)
    }

    fn committed(&self, namespace: usize, event: &EventEnvelope<Self::Event>) -> impl Future<Output = ()> + Send {
        (**self).committed(namespace, event)
    }

    fn reset(
        &self,
        namespace: usize,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).reset(namespace, context)
    }

    fn replays(namespace: usize) -> bool {
        P::replays(namespace)
    }

    fn batch_size(namespace: usize) -> Option<usize> {
        P::batch_size(namespace)
    }

    fn emits(namespace: usize) -> bool {
        P::emits(namespace)
    }

    fn updated(
        &self,
        namespace: usize,
        event: &EventEnvelope<Self::Event>,
        updates: &P::Updates,
    ) -> impl Future<Output = ()> + Send {
        (**self).updated(namespace, event, updates)
    }
}

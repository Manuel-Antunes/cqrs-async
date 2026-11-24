//! Messages and their handlers: [`Command`]s change state, [`Query`]s read
//! it, each answered by exactly one handler
//! ([`CommandHandler`](crate::CommandHandler), [`QueryHandler`]). Committed events go to
//! [`EventHandler`]s. Gateways ([`CommandGateway`], [`QueryGateway`]) are
//! the ports callers send through.

use std::{fmt, future::Future, sync::Arc};

use crate::{DomainEvent, EventEnvelope, Metadata};

/// Something a handler answers. The output and error types belong to the
/// message, not to the handler: whoever sends a message knows exactly what
/// comes back without knowing who handles it.
///
/// Implemented by `#[derive(Command)]` / `#[derive(Query)]`, together with
/// the [`Command`] or [`Query`] marker.
pub trait Message: Send + 'static {
    /// What a successful handling returns (`()` for most commands).
    type Output: Send + 'static;
    /// What a failed handling returns.
    type Error: Send + 'static;
}

/// An intent to change state; sent with [`CommandGateway::send`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a command",
    note = "derive it with `#[derive(Command)]`; queries are sent with `.query(...)`"
)]
pub trait Command: Message {}

/// A request for data that changes nothing; sent with
/// [`QueryGateway::query`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a query",
    note = "derive it with `#[derive(Query)]`; commands are sent with `.send(...)`"
)]
pub trait Query: Message {}

/// Answers one query type `Q`: the read side's use case (Axon's
/// `@QueryHandler`). A struct answers several queries with one impl per
/// query, sharing its dependencies (usually the read model): it is the query
/// gateway itself (`#[cqrs::query_gateway]`).
///
/// The query is a generic parameter, so one type implements it once per
/// query; whether the message is a [`Command`] or a [`Query`] decides how it
/// can be sent, and commands have their own [`CommandHandler`](crate::CommandHandler).
///
/// The returned future is concrete (`impl Future`): calls are statically
/// dispatched and nothing is boxed. Implement it with a plain `async fn`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not answer `{Q}`",
    note = "implement `QueryHandler<{Q}>` for it, usually with `#[cqrs::query_handler]`"
)]
pub trait QueryHandler<Q: Query>: Send + Sync {
    /// Runs the use case for `query`.
    fn handle(&self, query: Q) -> impl Future<Output = Result<OutputOf<Q>, ErrorOf<Q>>> + Send;

    /// Runs the use case for `query`, sent with `metadata` (correlation id,
    /// user...). What gateways call. By default the metadata is ignored.
    fn handle_with(
        &self,
        query: Q,
        metadata: Metadata,
    ) -> impl Future<Output = Result<OutputOf<Q>, ErrorOf<Q>>> + Send {
        let _ = metadata;
        self.handle(query)
    }
}

/// `M::Output`, shorter in signatures.
pub type OutputOf<M> = <M as Message>::Output;

/// `M::Error`, shorter in signatures.
pub type ErrorOf<M> = <M as Message>::Error;

// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<Q: Query, H: QueryHandler<Q>> QueryHandler<Q> for Arc<H> {
    fn handle(&self, query: Q) -> impl Future<Output = Result<OutputOf<Q>, ErrorOf<Q>>> + Send {
        (**self).handle(query)
    }

    fn handle_with(
        &self,
        query: Q,
        metadata: Metadata,
    ) -> impl Future<Output = Result<OutputOf<Q>, ErrorOf<Q>>> + Send {
        (**self).handle_with(query, metadata)
    }
}

/// That a type has a handler impl for `M`, whatever its bounds: written by
/// `#[command_handler]` / `#[query_handler]` next to the impl, and required
/// by `#[command_gateway]` / `#[query_gateway]` for every message they list,
/// so a listed message without its handler is a compile error where the
/// gateway is declared. A handler implemented without the attribute writes
/// it by hand, without bounds: `impl<R> Handles<FindPost> for Posts<R> {}`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `{M}` in `handles(...)` but has no handler for it",
    label = "no `#[cqrs::command_handler]` / `#[cqrs::query_handler]` impl for `{M}`",
    note = "write `#[cqrs::command_handler] impl CommandHandler<{M}> for ...` (or `QueryHandler<{M}>`), or remove `{M}` from `handles(...)`"
)]
pub trait Handles<M> {}

/// What a processor hands an event handler with each event (Axon's
/// `ProcessingContext`): the unit of work it runs in, and whether the event
/// is replayed. Built by the processor; handlers only read it.
pub struct ProcessingContext<'a, Tx = ()> {
    /// The unit of work: a database transaction, or `()` in memory.
    transaction: &'a mut Tx,
    /// Whether the event is replayed.
    replay: bool,
    /// The processor handling it (`""` outside one).
    processor: &'a str,
}

impl<'a, Tx> ProcessingContext<'a, Tx> {
    /// A context over `transaction`, outside any processor. `replay`: the
    /// event was handled before a [reset](crate::TrackingProcessor::reset).
    pub fn new(transaction: &'a mut Tx, replay: bool) -> Self {
        Self::in_processor(transaction, replay, "")
    }

    /// A context over `transaction` in the processor named `processor`
    /// (for a projection, `"{projection}.{namespace}"`).
    pub fn in_processor(transaction: &'a mut Tx, replay: bool, processor: &'a str) -> Self {
        Self {
            transaction,
            replay,
            processor,
        }
    }

    /// The processor handling the event (Axon's processing context knows
    /// it too): for a projection, `"{projection}.{namespace}"`; `""` outside
    /// a processor. What an interceptor around a whole projection tells its
    /// namespaces apart by.
    pub fn processor(&self) -> &str {
        self.processor
    }

    /// The unit of work, to write the read model in (it commits with the
    /// processor's token).
    pub fn transaction(&mut self) -> &mut Tx {
        self.transaction
    }

    /// Whether the event is replayed (Axon's `ReplayStatus`).
    pub fn replay(&self) -> bool {
        self.replay
    }
}

/// Reacts to committed events of one event type (usually an aggregate's event
/// enum): Axon's event handling component. Read models (projections) and
/// side effects (policies, notifications) share this one contract.
///
/// Usually written with `#[cqrs::event_handler]` on the impl, which fills
/// in the event type and the unit of work from `handle`'s signature, and run
/// by a processor named when it is made (`events.processor(name, handler)`).
/// The handlers of an aggregate's projection are written the same way, one
/// per namespace (`#[cqrs::event_handler(namespace = "...")]`, see
/// [`Projection`](crate::Projection)):
///
/// ```
/// # use std::sync::Mutex;
/// # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
/// # #[derive(Clone, cqrs::DomainEvent)] enum PostEvent { Created, Published }
/// #[derive(Default)]
/// struct PublishedCount {
///     count: Mutex<u64>,
/// }
///
/// #[cqrs::event_handler]
/// impl EventHandler for PublishedCount {
///     async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
///         match event.payload {
///             PostEvent::Published => *self.count.lock().unwrap() += 1,
///             PostEvent::Created => {}
///         }
///         Ok(())
///     }
///
///     // Optional: what to clear before a replay rebuilds it.
///     async fn reset(&self, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
///         *self.count.lock().unwrap() = 0;
///         Ok(())
///     }
/// }
///
/// // events.processor("published-count", PublishedCount::default()).spawn();
/// ```
///
/// `Tx` is the unit of work: a tracking processor opens one per event from
/// its token store (a database transaction, or `()` in memory), stores the
/// token in it and commits both together, so an event applied to a read
/// model in that database is applied exactly once. A handler that does not
/// write there is generic over `Tx` and runs with any store (the macro
/// makes it so when the context names no `Tx`).
pub trait EventHandler<E: DomainEvent, Tx: Send = ()>: Send + Sync {
    /// Whether it sees the events of a replay (Axon's `@DisallowReplay`
    /// when `false`): a projection rebuilds from them, a side effect (an
    /// e-mail) must not happen twice. Default: `true`.
    const REPLAY: bool = true;

    /// Reacts to one committed event, within the context's unit of work.
    /// An error rolls it back (with the token): the processor retries the
    /// event, or parks it. Outside a database transaction, delivery is
    /// at-least-once, so side effects should be idempotent (the envelope's
    /// `position`/`sequence` help detect duplicates).
    fn handle(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;

    /// After the unit of work committed: where query updates are emitted,
    /// so subscribers never see a change that was rolled back. Default:
    /// nothing.
    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send {
        let _ = event;
        async {}
    }

    /// Clears what it built before a replay rebuilds it (Axon's
    /// `@ResetHandler`), in its own unit of work, called by
    /// [`TrackingProcessor::reset`](crate::TrackingProcessor::reset).
    /// Default: nothing to clear.
    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        let _ = context;
        async { Ok(()) }
    }

    /// [`handle`](Self::handle), unless the event is replayed and
    /// [`REPLAY`](Self::REPLAY) is `false`: what a processor calls. A group
    /// overrides it to decide per member. Sealed: only this crate can call
    /// or override it (its last argument cannot be named elsewhere).
    #[doc(hidden)]
    fn handle_in(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        _: sealed::Internal,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        async move {
            if context.replay() && !Self::REPLAY {
                return Ok(());
            }
            self.handle(event, context).await
        }
    }

    /// [`committed`](Self::committed), with the same replay rule as
    /// `handle_in`. Sealed too.
    #[doc(hidden)]
    fn committed_in(
        &self,
        event: &EventEnvelope<E>,
        replay: bool,
        _: sealed::Internal,
    ) -> impl Future<Output = ()> + Send {
        async move {
            if !replay || Self::REPLAY {
                self.committed(event).await;
            }
        }
    }
}

// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, Tx: Send, H: EventHandler<E, Tx>> EventHandler<E, Tx> for Arc<H> {
    const REPLAY: bool = H::REPLAY;

    fn handle(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).handle(event, context)
    }

    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send {
        (**self).committed(event)
    }

    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).reset(context)
    }

    fn handle_in(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        internal: sealed::Internal,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).handle_in(event, context, internal)
    }

    fn committed_in(
        &self,
        event: &EventEnvelope<E>,
        replay: bool,
        internal: sealed::Internal,
    ) -> impl Future<Output = ()> + Send {
        (**self).committed_in(event, replay, internal)
    }
}

/// Implements [`EventHandler`] for a tuple: a processing group (Axon), its
/// members fed in order, the first failure stopping the rest. Run by one
/// processor, they share its unit of work and its token: they commit all or
/// none. In a replay, members that do not [`REPLAY`](EventHandler::REPLAY)
/// are skipped one by one. A tuple means this everywhere, inline too;
/// [`Isolated`](crate::Isolated) is the other meaning, for inline handlers.
macro_rules! tuple_event_handler {
    ($($handler:ident $index:tt),+) => {
        impl<E: DomainEvent, Tx: Send, $($handler: EventHandler<E, Tx>),+> EventHandler<E, Tx> for ($($handler,)+) {
            /// Replayed if any member is.
            const REPLAY: bool = false $(|| $handler::REPLAY)+;

            async fn handle(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
                $(self.$index.handle(event, context).await?;)+
                Ok(())
            }

            async fn committed(&self, event: &EventEnvelope<E>) {
                $(self.$index.committed(event).await;)+
            }

            async fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
                $(self.$index.reset(context).await?;)+
                Ok(())
            }

            async fn handle_in(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>, internal: sealed::Internal) -> Result<(), HandlerError> {
                $(self.$index.handle_in(event, context, internal).await?;)+
                Ok(())
            }

            async fn committed_in(&self, event: &EventEnvelope<E>, replay: bool, internal: sealed::Internal) {
                $(self.$index.committed_in(event, replay, internal).await;)+
            }
        }
    };
}

tuple_event_handler!(A 0);
tuple_event_handler!(A 0, B 1);
tuple_event_handler!(A 0, B 1, C 2);
tuple_event_handler!(A 0, B 1, C 2, D 3);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5, H 6);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5, H 6, I 7);

/// Routes one message type to its handler. Implemented by
/// `#[command_gateway]` / `#[query_gateway]`, once per message in
/// `handles(...)`; callers use the [`CommandGateway`] / [`QueryGateway`]
/// methods.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no handler for `{M}`",
    label = "nothing in this gateway handles `{M}`",
    note = "add `{M}` to the gateway's `handles(...)`, with its handler impl"
)]
pub trait Dispatch<M: Message>: Send + Sync {
    /// Hands `message` and its `metadata` to the handler responsible for
    /// `M`. Wrappers forward both, so metadata is never lost on the way.
    fn dispatch_with(&self, message: M, metadata: Metadata)
    -> impl Future<Output = Result<M::Output, M::Error>> + Send;
}

// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<M: Message, G: Dispatch<M>> Dispatch<M> for Arc<G> {
    fn dispatch_with(
        &self,
        message: M,
        metadata: Metadata,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        (**self).dispatch_with(message, metadata)
    }
}

/// A nameable port for sending one command type: what other components
/// (HTTP adapters, sagas, other use cases) depend on instead of a concrete
/// gateway, so a test double fits too. Every [`Dispatch`] of a [`Command`]
/// gets it for free.
pub trait CommandGateway<C: Command>: Dispatch<C> {
    /// Sends the command to its handler and waits for the result.
    fn send(&self, command: C) -> impl Future<Output = Result<C::Output, C::Error>> + Send {
        self.dispatch_with(command, Metadata::new())
    }

    /// Sends the command with `metadata` (Axon's `send(command, metadata)`):
    /// a command handler stores it with every event it saves, so the events
    /// carry the correlation id, user, etc. of the request that caused them.
    fn send_with(&self, command: C, metadata: Metadata) -> impl Future<Output = Result<C::Output, C::Error>> + Send {
        self.dispatch_with(command, metadata)
    }
}

impl<C: Command, G: Dispatch<C>> CommandGateway<C> for G {}

/// The query counterpart of [`CommandGateway`].
pub trait QueryGateway<Q: Query>: Dispatch<Q> {
    /// Asks the query of its handler and waits for the answer.
    fn query(&self, query: Q) -> impl Future<Output = Result<Q::Output, Q::Error>> + Send {
        self.dispatch_with(query, Metadata::new())
    }
}

impl<Q: Query, G: Dispatch<Q>> QueryGateway<Q> for G {}

/// Why an event handler failed: the original error, so a log or a dead
/// letter can walk its causes. Converts from any `std::error::Error`, so
/// handlers can use `?` (it deliberately does not implement `Error` itself,
/// the same trick `anyhow` uses; [`as_error`](Self::as_error) lends it as
/// one).
pub struct HandlerError(Box<dyn std::error::Error + Send + Sync>);

impl HandlerError {
    /// An error from a plain message, for failures that are not an
    /// `std::error::Error` value.
    pub fn msg(message: impl fmt::Display) -> Self {
        Self(message.to_string().into())
    }

    /// The original error.
    pub fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &*self.0
    }

    /// The original error, owned.
    pub fn into_inner(self) -> Box<dyn std::error::Error + Send + Sync> {
        self.0
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self(Box::new(error))
    }
}

impl From<HandlerError> for Box<dyn std::error::Error + Send + Sync> {
    fn from(error: HandlerError) -> Self {
        error.0
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HandlerError").field(&self.0).finish()
    }
}

/// The seal of [`EventHandler`]'s internal methods.
pub(crate) mod sealed {
    /// A value only this crate can make, and a type only it can name: an
    /// argument of a method that nobody else can call nor override.
    #[derive(Clone, Copy, Debug)]
    pub struct Internal(());

    /// The seal.
    pub(crate) const INTERNAL: Internal = Internal(());
}

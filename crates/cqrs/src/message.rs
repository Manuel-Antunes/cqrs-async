//! Messages and their handlers: [`Command`]s change state, [`Query`]s read
//! it, each answered by exactly one [`Handler`]. Committed events go to
//! [`EventHandler`]s. Gateways ([`CommandGateway`], [`QueryGateway`]) are
//! the ports callers send through.

use std::{fmt, future::Future, sync::Arc};

use crate::{DomainEvent, EventEnvelope, Metadata};

/// Something a [`Handler`] answers. The output and error types belong to the
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

/// Handles exactly one message type, named by the associated type: one
/// struct per use case, holding the dependencies that use case needs.
///
/// The message is an associated type, not a generic parameter, so a gateway
/// can route by the handler's type alone. It is one trait for commands and
/// queries; whether the message is a [`Command`] or a [`Query`] decides how it
/// can be sent.
///
/// The returned future is concrete (`impl Future`): calls are statically
/// dispatched and nothing is boxed. Implement it with a plain `async fn`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler",
    note = "implement `Handler` for it; in a `#[derive(Gateway)]`, mark fields that are not handlers with `#[gateway(skip)]`"
)]
pub trait Handler: Send + Sync {
    /// The one message this handler answers.
    type Message: Message;

    /// Runs the use case for `message`.
    fn handle(
        &self,
        message: Self::Message,
    ) -> impl Future<Output = Result<OutputOf<Self::Message>, ErrorOf<Self::Message>>> + Send;

    /// Runs the use case for `message`, sent with `metadata` (correlation
    /// id, user...). What gateways call. By default the metadata is ignored;
    /// a [`CommandHandler`](crate::CommandHandler) stores it with the events
    /// it saves.
    fn handle_with(
        &self,
        message: Self::Message,
        metadata: Metadata,
    ) -> impl Future<Output = Result<OutputOf<Self::Message>, ErrorOf<Self::Message>>> + Send {
        let _ = metadata;
        self.handle(message)
    }
}

/// `M::Output`, shorter in signatures.
pub type OutputOf<M> = <M as Message>::Output;

/// `M::Error`, shorter in signatures.
pub type ErrorOf<M> = <M as Message>::Error;

// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<H: Handler> Handler for Arc<H> {
    type Message = H::Message;

    fn handle(
        &self,
        message: H::Message,
    ) -> impl Future<Output = Result<OutputOf<H::Message>, ErrorOf<H::Message>>> + Send {
        (**self).handle(message)
    }

    fn handle_with(
        &self,
        message: H::Message,
        metadata: Metadata,
    ) -> impl Future<Output = Result<OutputOf<H::Message>, ErrorOf<H::Message>>> + Send {
        (**self).handle_with(message, metadata)
    }
}

/// What a processor hands an event handler with each event (Axon's
/// `ProcessingContext`): the unit of work it runs in, and whether the event
/// is replayed. Built by the processor; handlers only read it.
pub struct ProcessingContext<'a, Tx = ()> {
    /// The unit of work: a database transaction, or `()` in memory.
    transaction: &'a mut Tx,
    /// Whether the event is replayed.
    replay: bool,
}

impl<'a, Tx> ProcessingContext<'a, Tx> {
    /// A context over `transaction`. `replay`: the event was handled before
    /// a [reset](crate::TrackingProcessor::reset).
    pub fn new(transaction: &'a mut Tx, replay: bool) -> Self {
        Self { transaction, replay }
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
/// Usually written with `#[cqrs::projection]` on the impl, which fills in
/// the event type and the unit of work from `handle`'s signature, and
/// names the processor (`namespace = "..."`):
///
/// ```
/// # use std::sync::Mutex;
/// # use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};
/// # #[derive(Clone, cqrs::DomainEvent, cqrs::TagResolver)] enum PostEvent { Created, Published }
/// #[derive(Default)]
/// struct PublishedCount {
///     count: Mutex<u64>,
/// }
///
/// #[cqrs::projection(namespace = "published-count")]
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
/// # fn named<P: cqrs::Projection>(_: &P) -> &'static str { P::NAMESPACE }
/// assert_eq!(named(&PublishedCount::default()), "published-count");
/// // events.projection(PublishedCount::default()).spawn();
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
    /// overrides it to decide per member.
    fn handle_in(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        async move {
            if context.replay() && !Self::REPLAY {
                return Ok(());
            }
            self.handle(event, context).await
        }
    }

    /// [`committed`](Self::committed), with the same replay rule as
    /// [`handle_in`](Self::handle_in).
    fn committed_in(&self, event: &EventEnvelope<E>, replay: bool) -> impl Future<Output = ()> + Send {
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
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).handle_in(event, context)
    }

    fn committed_in(&self, event: &EventEnvelope<E>, replay: bool) -> impl Future<Output = ()> + Send {
        (**self).committed_in(event, replay)
    }
}

/// Implements [`EventHandler`] for a tuple: a processing group (Axon), its
/// members fed in order by one processor, in one unit of work, with one
/// token: they commit all or none. In a replay, members that do not
/// [`REPLAY`](EventHandler::REPLAY) are skipped one by one.
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

            async fn handle_in(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
                $(self.$index.handle_in(event, context).await?;)+
                Ok(())
            }

            async fn committed_in(&self, event: &EventEnvelope<E>, replay: bool) {
                $(self.$index.committed_in(event, replay).await;)+
            }
        }
    };
}

tuple_event_handler!(A 0, B 1);
tuple_event_handler!(A 0, B 1, C 2);
tuple_event_handler!(A 0, B 1, C 2, D 3);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5, H 6);
tuple_event_handler!(A 0, B 1, C 2, D 3, F 4, G 5, H 6, I 7);

/// Routes one message type to its handler. Implemented by
/// `#[derive(Gateway)]`, once per handler field; callers use the
/// [`CommandGateway`] / [`QueryGateway`] methods.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no handler for `{M}`",
    label = "nothing in this gateway handles `{M}`",
    note = "add a field whose type implements `Handler<Message = {M}>`"
)]
pub trait Dispatch<M: Message>: Send + Sync {
    /// Hands `message` and its `metadata` to the handler responsible for
    /// `M`. Wrappers forward both, so metadata is never lost on the way.
    fn dispatch_with(
        &self,
        message: M,
        metadata: Metadata,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send;

    /// Hands `message` to the handler responsible for `M`, without metadata.
    fn dispatch(&self, message: M) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.dispatch_with(message, Metadata::new())
    }
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

    fn dispatch(&self, message: M) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        (**self).dispatch(message)
    }
}

/// An enum of messages (`#[derive(Commands)]` / `#[derive(Queries)]`)
/// whose every variant `G` dispatches: what lets a gateway marked
/// `#[gateway(commands = Enum)]` take the enum itself.
#[diagnostic::on_unimplemented(
    message = "`{G}` does not handle every message of `{Self}`",
    label = "some variant of `{Self}` has no handler in `{G}`",
    note = "add a handler field for each variant's message, or remove the variant"
)]
pub trait DispatchVia<G: ?Sized>: Message {
    /// Hands the variant's message and `metadata` to `gateway`, wrapping
    /// its answer in the matching variant of the output enum.
    fn dispatch_via(self, gateway: &G, metadata: Metadata) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// A nameable port for sending one command type: what other components
/// (HTTP adapters, sagas, other use cases) depend on instead of a concrete
/// gateway, so a test double fits too. Every [`Dispatch`] of a [`Command`]
/// gets it for free.
pub trait CommandGateway<C: Command>: Dispatch<C> {
    /// Sends the command to its handler and waits for the result.
    fn send(&self, command: C) -> impl Future<Output = Result<C::Output, C::Error>> + Send {
        self.dispatch(command)
    }

    /// Sends the command with `metadata` (Axon's `send(command, metadata)`):
    /// a command handler stores it with every event it saves, so the events
    /// carry the correlation id, user, etc. of the request that caused them.
    fn send_with(
        &self,
        command: C,
        metadata: Metadata,
    ) -> impl Future<Output = Result<C::Output, C::Error>> + Send {
        self.dispatch_with(command, metadata)
    }
}

impl<C: Command, G: Dispatch<C>> CommandGateway<C> for G {}

/// The query counterpart of [`CommandGateway`].
pub trait QueryGateway<Q: Query>: Dispatch<Q> {
    /// Asks the query of its handler and waits for the answer.
    fn query(&self, query: Q) -> impl Future<Output = Result<Q::Output, Q::Error>> + Send {
        self.dispatch(query)
    }
}

impl<Q: Query, G: Dispatch<Q>> QueryGateway<Q> for G {}

/// Why an event handler failed. Converts from any `std::error::Error`, so
/// handlers can use `?` (it deliberately does not implement `Error` itself,
/// the same trick `anyhow` uses).
pub struct HandlerError(String);

impl HandlerError {
    /// An error from a plain message, for failures that are not an
    /// `std::error::Error` value.
    pub fn msg(message: impl fmt::Display) -> Self {
        Self(message.to_string())
    }
}

impl<E: std::error::Error> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self(error.to_string())
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HandlerError({:?})", self.0)
    }
}

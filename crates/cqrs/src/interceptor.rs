//! Interceptors (Axon's dispatch and handler interceptors): logging,
//! metrics, tracing, authorization, enriching metadata. One wrapper,
//! [`Intercepted`], for every kind of handler:
//!
//! ```text
//! // Commands and queries: around a whole gateway.
//! let commands = Intercepted::new(PostCommandGateway::new(posts), (Timing, RequireUser));
//! commands.send_with(PublishPost { id }, metadata).await
//!
//! // Events: around an event handler or a projection, inside its unit of
//! // work, in a tracking processor.
//! events.projection(Intercepted::new(summaries, (Timing, Tracing))).spawn();
//! ```
//!
//! One [`Interceptor`] impl generic over the message covers every message a
//! gateway routes; one [`EventInterceptor`] impl covers every event. A type
//! may implement both. Tuples of either chain them, the first outermost,
//! as nested closures: as static as hand-written calls.

use std::future::Future;

use crate::{
    Dispatch, DomainEvent, EventEnvelope, EventHandler, HandlerError, Message, Metadata, ProcessingContext, Projection,
};

/// Runs around the handling of messages `M`: it may look at or change the
/// message and its metadata, refuse it (by returning an error without
/// calling `next`), or look at the result.
///
/// Implement it for every message (`impl<M: Message> Interceptor<M>`), or
/// for those that can express its refusal, e.g.
/// `impl<C: Command> Interceptor<C> for RequireUser where C::Error: From<Unauthorized>`.
pub trait Interceptor<M: Message>: Send + Sync {
    /// Handles `message` by calling `next` with it (or not).
    fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> impl Future<Output = Result<M::Output, M::Error>> + Send
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send;
}

/// Chains the interceptors of a tuple: each one's `next` is the following
/// one, the last one's is the real `next`. Expands to nested closures, so
/// the chain is as static as hand-written calls.
macro_rules! chain {
    ($this:ident, $message:ident, $metadata:ident, $next:ident; $last:tt) => {
        $this.$last.intercept($message, $metadata, $next)
    };
    ($this:ident, $message:ident, $metadata:ident, $next:ident; $first:tt $($rest:tt)+) => {
        $this.$first.intercept($message, $metadata, move |$message, $metadata| {
            chain!($this, $message, $metadata, $next; $($rest)+)
        })
    };
}

/// Implements [`Interceptor`] for a tuple: they run in the order written,
/// the first one outermost.
macro_rules! tuple_interceptor {
    ($($interceptor:ident $index:tt),+) => {
        impl<M: Message, $($interceptor: Interceptor<M>),+> Interceptor<M> for ($($interceptor,)+) {
            fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> impl Future<Output = Result<M::Output, M::Error>> + Send
            where
                N: FnOnce(M, Metadata) -> F + Send,
                F: Future<Output = Result<M::Output, M::Error>> + Send,
            {
                let this = self;
                chain!(this, message, metadata, next; $($index)+)
            }
        }
    };
}

tuple_interceptor!(A 0);
tuple_interceptor!(A 0, B 1);
tuple_interceptor!(A 0, B 1, C 2);
tuple_interceptor!(A 0, B 1, C 2, D 3);
tuple_interceptor!(A 0, B 1, C 2, D 3, E 4);
tuple_interceptor!(A 0, B 1, C 2, D 3, E 4, F5 5);
tuple_interceptor!(A 0, B 1, C 2, D 3, E 4, F5 5, G 6);
tuple_interceptor!(A 0, B 1, C 2, D 3, E 4, F5 5, G 6, H 7);

/// Runs around the handling of each event of type `E` by an event handler
/// or a projection (Axon's handler interceptors on event processors),
/// inside its unit of work: it gets the handler's [`ProcessingContext`], so
/// what it writes in `context.transaction()` commits or rolls back with the
/// handler's changes and the processor's token. It may look at the event,
/// time or trace its handling, fail it (the processor retries it, or parks
/// it), or skip it by returning `Ok(())` without calling `next`: the event
/// then counts as handled.
///
/// `Tx` is the unit of work, as in [`EventHandler`]: implement it for every
/// one (`impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx>`) unless the
/// interceptor writes in it.
///
/// ```
/// use cqrs::{DomainEvent, EventChain, EventEnvelope, EventInterceptor, HandlerError, ProcessingContext};
///
/// /// Skips replayed events for the handlers it wraps.
/// struct LiveOnly;
///
/// impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for LiveOnly {
///     async fn intercept<N: EventChain<Tx>>(
///         &self,
///         _: &EventEnvelope<E>,
///         context: &mut ProcessingContext<'_, Tx>,
///         next: N,
///     ) -> Result<(), HandlerError> {
///         if context.replay() {
///             return Ok(());
///         }
///         next.proceed(context).await
///     }
/// }
/// ```
pub trait EventInterceptor<E: DomainEvent, Tx = ()>: Send + Sync {
    /// Handles `event` by calling `next.proceed(context)` (or not).
    fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// The rest of the chain an [`EventInterceptor`] runs around (Axon's
/// interceptor chain): the next interceptors, then the handler. Consumed by
/// [`proceed`](Self::proceed), so the handler runs at most once.
pub trait EventChain<Tx>: Send {
    /// Runs the rest of the chain in `context`.
    fn proceed(self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// The end of the chain: the handler's [`handle`](EventHandler::handle).
struct Handle<'h, 'e, H, E> {
    handler: &'h H,
    event: &'e EventEnvelope<E>,
}

impl<E: DomainEvent, Tx: Send, H: EventHandler<E, Tx>> EventChain<Tx> for Handle<'_, '_, H, E> {
    fn proceed(self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.handler.handle(self.event, context)
    }
}

/// The end of the chain in a processor: the handler's
/// [`handle_in`](EventHandler::handle_in), which a group splits by
/// [`REPLAY`](EventHandler::REPLAY).
struct HandleIn<'h, 'e, H, E> {
    handler: &'h H,
    event: &'e EventEnvelope<E>,
}

impl<E: DomainEvent, Tx: Send, H: EventHandler<E, Tx>> EventChain<Tx> for HandleIn<'_, '_, H, E> {
    fn proceed(self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.handler.handle_in(self.event, context)
    }
}

/// One interceptor of a tuple, then the rest of the chain.
struct Then<'i, 'e, I, E, N> {
    interceptor: &'i I,
    event: &'e EventEnvelope<E>,
    next: N,
}

impl<E: DomainEvent, Tx: Send, I: EventInterceptor<E, Tx>, N: EventChain<Tx>> EventChain<Tx> for Then<'_, '_, I, E, N> {
    fn proceed(self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.interceptor.intercept(self.event, context, self.next)
    }
}

/// Nests the interceptors of a tuple into one chain, the first outermost.
macro_rules! chain_events {
    ($this:ident, $event:ident, $next:ident; $last:tt) => {
        Then { interceptor: &$this.$last, event: $event, next: $next }
    };
    ($this:ident, $event:ident, $next:ident; $first:tt $($rest:tt)+) => {
        Then { interceptor: &$this.$first, event: $event, next: chain_events!($this, $event, $next; $($rest)+) }
    };
}

/// Implements [`EventInterceptor`] for a tuple: they run in the order
/// written, the first one outermost.
macro_rules! tuple_event_interceptor {
    ($($interceptor:ident $index:tt),+) => {
        impl<E: DomainEvent, Tx: Send, $($interceptor: EventInterceptor<E, Tx>),+> EventInterceptor<E, Tx> for ($($interceptor,)+) {
            fn intercept<N: EventChain<Tx>>(
                &self,
                event: &EventEnvelope<E>,
                context: &mut ProcessingContext<'_, Tx>,
                next: N,
            ) -> impl Future<Output = Result<(), HandlerError>> + Send {
                let this = self;
                chain_events!(this, event, next; $($index)+).proceed(context)
            }
        }
    };
}

tuple_event_interceptor!(A 0);
tuple_event_interceptor!(A 0, B 1);
tuple_event_interceptor!(A 0, B 1, C 2);
tuple_event_interceptor!(A 0, B 1, C 2, D 3);
tuple_event_interceptor!(A 0, B 1, C 2, D 3, F5 4);
tuple_event_interceptor!(A 0, B 1, C 2, D 3, F5 4, G 5);
tuple_event_interceptor!(A 0, B 1, C 2, D 3, F5 4, G 5, H 6);
tuple_event_interceptor!(A 0, B 1, C 2, D 3, F5 4, G 5, H 6, I 7);

/// A gateway, an event handler or a projection whose messages or events go
/// through `I` first.
pub struct Intercepted<G, I> {
    /// Where messages or events go once intercepted.
    inner: G,
    /// What they go through.
    interceptor: I,
}

impl<G, I> Intercepted<G, I> {
    /// `inner`, with every message or event going through `interceptor`.
    pub fn new(inner: G, interceptor: I) -> Self {
        Self { inner, interceptor }
    }

    /// The wrapped gateway or handler.
    pub fn inner(&self) -> &G {
        &self.inner
    }

    /// The interceptor messages go through.
    pub fn interceptor(&self) -> &I {
        &self.interceptor
    }
}

impl<M, G, I> Dispatch<M> for Intercepted<G, I>
where
    M: Message,
    G: Dispatch<M>,
    I: Interceptor<M>,
{
    fn dispatch_with(&self, message: M, metadata: Metadata) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.interceptor
            .intercept(message, metadata, |message, metadata| self.inner.dispatch_with(message, metadata))
    }
}

/// An event handler (or projection) whose events go through `I`, inside
/// its unit of work. Replayed events it skips skip `I` too; `committed`
/// runs as is.
impl<E, Tx, H, I> EventHandler<E, Tx> for Intercepted<H, I>
where
    E: DomainEvent,
    Tx: Send,
    H: EventHandler<E, Tx>,
    I: EventInterceptor<E, Tx>,
{
    const REPLAY: bool = H::REPLAY;

    fn handle(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.interceptor.intercept(event, context, Handle { handler: &self.inner, event })
    }

    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send {
        self.inner.committed(event)
    }

    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self.inner.reset(context)
    }

    async fn handle_in(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        if context.replay() && !H::REPLAY {
            return Ok(());
        }
        self.interceptor.intercept(event, context, HandleIn { handler: &self.inner, event }).await
    }

    fn committed_in(&self, event: &EventEnvelope<E>, replay: bool) -> impl Future<Output = ()> + Send {
        self.inner.committed_in(event, replay)
    }
}

/// An intercepted projection keeps its namespace.
impl<P: Projection, I: Send + Sync> Projection for Intercepted<P, I> {
    const NAMESPACE: &'static str = P::NAMESPACE;
}

//! A zero-macro bus: handlers live in a type-level list and the compiler
//! finds the one whose `Handler::Message` is the message sent.
//!
//! `bus.send(CreatePost { .. })` infers the index `I` (the handler's position
//! in the list). Exactly one match compiles; none is an "unimplemented"
//! error; two are an ambiguity error (E0283). Both happen at compile time.
//!
//! The index is the price: code generic over a `Bus<L>` must carry the `I`
//! parameter. For a nameable port use `#[derive(Gateway)]` instead.

use std::{future::Future, marker::PhantomData};

use crate::{Command, Handler, Message, Metadata, Query};

/// The empty handler list: a bus with nothing registered.
pub struct Nil;

/// A non-empty handler list: one handler followed by the rest of the list.
/// Each [`Bus::register`] wraps the current list in a new `Cons`.
pub struct Cons<H, T> {
    /// The most recently registered handler.
    head: H,
    /// The handlers registered before it.
    tail: T,
}

/// Type-level index: the handler is the head of the list. Never built at
/// runtime; it only tells [`Route`] impls apart.
pub struct Here;

/// Type-level index: the handler is in the tail, at index `I` from there.
/// The compiler infers the whole `There<There<Here>>` chain.
pub struct There<I>(PhantomData<I>);

/// Finds the handler for `M` in a handler list and calls it.
///
/// `I` is the handler's position, inferred by the compiler: there is one impl
/// for "it is the head" and one for "it is somewhere in the tail", and only
/// the path that ends at a handler for `M` type-checks.
#[diagnostic::on_unimplemented(
    message = "no handler for `{M}` is registered on this bus",
    label = "nothing registered handles `{M}`",
    note = "register a `Handler<Message = {M}>` with `Bus::register`"
)]
pub trait Route<M: Message, I>: Send + Sync {
    /// Passes `message` and its `metadata` to the handler at position `I`.
    fn route(&self, message: M, metadata: Metadata) -> impl Future<Output = Result<M::Output, M::Error>> + Send;
}

impl<M, H, T> Route<M, Here> for Cons<H, T>
where
    M: Message,
    H: Handler<Message = M>,
    T: Send + Sync,
{
    fn route(&self, message: M, metadata: Metadata) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.head.handle_with(message, metadata)
    }
}

impl<M, H, T, I> Route<M, There<I>> for Cons<H, T>
where
    M: Message,
    H: Send + Sync,
    T: Route<M, I>,
{
    fn route(&self, message: M, metadata: Metadata) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.tail.route(message, metadata)
    }
}

/// Handlers composed explicitly, without a container or global state.
///
/// ```
/// # use cqrs::{Bus, Command, Handler, Message};
/// struct Ping;
/// impl Message for Ping { type Output = &'static str; type Error = (); }
/// impl Command for Ping {}
///
/// struct PingHandler;
/// impl Handler for PingHandler {
///     type Message = Ping;
///     async fn handle(&self, _: Ping) -> Result<&'static str, ()> { Ok("pong") }
/// }
///
/// let bus = Bus::new().register(PingHandler);
/// assert_eq!(cqrs::testing::block_on(bus.send(Ping)), Ok("pong"));
/// ```
///
/// A message nobody handles does not compile:
///
/// ```compile_fail,E0277
/// # use cqrs::{Bus, Command, Message};
/// struct Ping;
/// impl Message for Ping { type Output = (); type Error = (); }
/// impl Command for Ping {}
/// let _ = Bus::new().send(Ping);
/// ```
///
/// Neither does a message handled twice:
///
/// ```compile_fail,E0283
/// # use cqrs::{Bus, Command, Handler, Message};
/// struct Ping;
/// impl Message for Ping { type Output = (); type Error = (); }
/// impl Command for Ping {}
/// struct A;
/// impl Handler for A { type Message = Ping; async fn handle(&self, _: Ping) -> Result<(), ()> { Ok(()) } }
/// struct B;
/// impl Handler for B { type Message = Ping; async fn handle(&self, _: Ping) -> Result<(), ()> { Ok(()) } }
/// let _ = Bus::new().register(A).register(B).send(Ping);
/// ```
pub struct Bus<L = Nil> {
    /// The registered handlers, as a [`Cons`] list ending in [`Nil`].
    handlers: L,
}

impl Bus<Nil> {
    /// A bus with no handlers; add them with [`register`](Bus::register).
    pub fn new() -> Self {
        Self { handlers: Nil }
    }
}

impl Default for Bus<Nil> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> Bus<L> {
    /// Adds a handler and returns the extended bus. The bus type grows with
    /// each handler, which is what lets the compiler route without a registry.
    pub fn register<H: Handler>(self, handler: H) -> Bus<Cons<H, L>> {
        Bus {
            handlers: Cons {
                head: handler,
                tail: self.handlers,
            },
        }
    }

    /// Sends a command to the one registered handler for `C`. A missing or
    /// duplicated handler is a compile error.
    pub fn send<C, I>(&self, command: C) -> impl Future<Output = Result<C::Output, C::Error>> + Send
    where
        C: Command,
        L: Route<C, I>,
    {
        self.handlers.route(command, Metadata::new())
    }

    /// Sends a command with `metadata`, stored with the events it causes.
    pub fn send_with<C, I>(
        &self,
        command: C,
        metadata: Metadata,
    ) -> impl Future<Output = Result<C::Output, C::Error>> + Send
    where
        C: Command,
        L: Route<C, I>,
    {
        self.handlers.route(command, metadata)
    }

    /// Asks a query of the one registered handler for `Q`. A missing or
    /// duplicated handler is a compile error.
    pub fn query<Q, I>(&self, query: Q) -> impl Future<Output = Result<Q::Output, Q::Error>> + Send
    where
        Q: Query,
        L: Route<Q, I>,
    {
        self.handlers.route(query, Metadata::new())
    }
}

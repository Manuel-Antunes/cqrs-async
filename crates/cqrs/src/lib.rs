//! Proof of concept for the CQRS/Event Sourcing core described in
//! `cqrs_study.md`.
//!
//! Every contract here is dispatched statically: there is no `dyn`, no
//! `Box<dyn Future>`, no `Any` and no `TypeId` registry. Routing a message to
//! its handler is resolved by the type checker, so a missing or duplicated
//! handler is a compile error instead of a runtime one.
//!
//! - [`Aggregate`] / [`EventAppender`]: domain side; decisions are `&self`
//!   methods that append events to the appender they are given (Axon 5).
//! - [`Command`] / [`Query`] messages and their [`Handler`]s (one struct per
//!   message): application side.
//! - [`EventStore`] / [`EventSourced`]: persistence with optimistic
//!   concurrency; events reach the [`EventProcessor`] only after commit.
//!   Handlers commit through their [`Appender`] (`events.commit(entity)`),
//!   which carries the [`EventPublisher`] their gateway was given
//!   ([`WithPublisher`]); repositories are the application's own ports,
//!   whose adapters load with [`load`].
//! - `#[derive(Gateway)]` (a struct of handlers, routed by their message
//!   type) and [`Bus`] (zero-macro, type-level list): dispatch.
//! - [`Cqrs`]: an application's unit (publisher, gateways, events and
//!   processors), wired by a typed builder.
//! - [`EventFeed`]: the committed events as a `Stream`, a first-class port;
//!   [`TokenStore`]: per segment, how far event processors got and who owns
//!   it; [`DeadLetterQueue`]: events parked after failing, per sequence.
//!
//! # Features
//!
//! - `derive` (default): the derives and `#[derive(Gateway)]`.
//! - `tokio` (default): live event processing, [`Events`]: catch-up
//!   subscriptions, tracking processors and the broadcast after commit.
//!   Without it the crate is runtime-agnostic: every contract is a `Future`
//!   or a `Stream` (`futures-core`), and any runtime can implement them.

#![warn(missing_docs)]

// Lets the derives always emit `::cqrs::...`, even inside this crate's tests.
extern crate self as cqrs;

mod aggregate;
mod bus;
mod command_handler;
mod criteria;
mod cursor;
mod dead_letter;
mod event;
mod interceptor;
mod feed;
mod message;
mod projection;
mod event_sourced;
mod snapshot;
mod store;
mod token;
mod unit;
#[cfg(feature = "json")]
mod upcast;
#[cfg(feature = "tokio")]
mod streaming;
pub mod testing;

pub use aggregate::{Aggregate, EventAppender};
pub use bus::{Bus, Cons, Here, Nil, Route, There};
pub use command_handler::{
    Appender, CommandHandler, DispatchIn, DispatchInVia, EventPublisher, Pending, Published, WithPublisher, dispatch_command,
};
pub use criteria::{AppendCondition, Criterion, EventCriteria, Tag, TagResolver};
pub use cursor::{Cursor, Gap, Horizon};
pub use dead_letter::{
    DeadLetter, DeadLetterQueue, InMemoryDeadLetters, RetryOutcome, retry_dead_letters,
};
pub use event::{DomainEvent, EventEnvelope, Metadata, StreamId};
pub use interceptor::{EventChain, EventInterceptor, Intercepted, Interceptor};
pub use feed::{EventFeed, QuerySubscriptions, RefreshedBy};
pub use message::{
    Command, CommandGateway, Dispatch, DispatchVia, ErrorOf, EventHandler, Handler, HandlerError, Message,
    ProcessingContext,
    OutputOf, Query, QueryGateway,
};
pub use event_sourced::{
    load, load_in, EventProcessor, EventSourced, HandlerFailure, Loaded, NotFound, ProcessingFailure,
    Saved,
};
pub use projection::{Projection, TransactionalTokenStore};
pub use snapshot::{InMemorySnapshots, SnapshotStore};
pub use store::{EventStore, ExpectedVersion, InMemoryEventStore, Recorded, StoreError, TaggedEvent};
#[cfg(feature = "json")]
pub use upcast::{StoredEvent, Upcast, Upcaster, upcast};
pub use unit::{Cqrs, CqrsBuilder};
pub use token::{InMemoryTokenStore, TokenStore, TokenUpdate, TrackingToken, segment_of};
#[cfg(feature = "tokio")]
pub use streaming::{
    Events, GlobalProgress, LiveEvents, LiveQueries, LocalProgress, ProcessorHandle,
    CatchUp, Projected, ProjectionProgress, ProcessorStatus, QueryUpdateEmitter, ResetError, SubscriptionOptions, SubscriptionQueries, UpdatesOf, TrackingOptions,
    TrackingProcessor, subscribe,
};

#[cfg(feature = "derive")]
pub use cqrs_derive::{
    AggregateRoot, Command, Commands, DomainEvent, Gateway, Queries, Query, QueryUpdates, TagResolver, command_handler,
    projection, query_handler,
};

/// Everything a domain or application module usually needs.
pub mod prelude {
    pub use crate::{
        Aggregate, Command, CommandGateway, DomainEvent, Appender, CommandHandler, EventAppender, EventEnvelope, EventFeed,
        EventPublisher, WithPublisher,
        EventHandler, Handler, HandlerError, Message, Query, QueryGateway,
        QuerySubscriptions, RefreshedBy, TagResolver,
    };
}

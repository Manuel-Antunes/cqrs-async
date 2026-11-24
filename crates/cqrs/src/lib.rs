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
//! - [`Command`] / [`Query`] messages and their handlers
//!   ([`CommandHandler<C>`], [`QueryHandler<Q>`], one impl per message):
//!   application side.
//! - [`EventStore`] / [`AggregateLoader`] / [`InlinePublisher`]: persistence with optimistic
//!   concurrency; events reach the [`EventProcessor`] only after commit.
//!   Handlers record what they decided in their [`CommandContext`]
//!   (`events.record(entity)`), and their gateway's [`UnitOfWork`] appends
//!   it and commits, in the handler's transaction if it takes one;
//!   repositories are the application's own ports, whose adapters load with
//!   an [`AggregateLoader`].
//! - `#[command_gateway]` / `#[query_gateway]` (a struct handling the
//!   messages it lists in `handles(...)`, one handler impl each): dispatch.
//!   A gateway is a [`Dispatch`] impl per message: the attribute writes
//!   them, and they can be written by hand.
//! - [`Cqrs`]: an application's unit (publisher, gateways, events and
//!   processors), wired by a typed builder.
//! - [`EventFeed`]: the committed events as a `Stream`, a first-class port;
//!   [`TokenStore`]: per segment, how far event processors got and who owns
//!   it; [`DeadLetterQueue`]: events parked after failing, per sequence.
//!
//! # Features
//!
//! - `derive` (default): the derives and attributes (`#[command_gateway]`,
//!   `#[projection]`...).
//! - `json`: events stored as JSON, upcasters, and the `Json` codec.
//! - `protobuf`: the `Protobuf` [`Codec`], for `prost` messages.
//! - `tokio` (default): live event processing, [`Events`]: catch-up
//!   subscriptions, tracking processors and the broadcast after commit.
//!   Without it the crate is runtime-agnostic: every contract is a `Future`
//!   or a `Stream` (`futures-core`), and any runtime can implement them.

#![warn(missing_docs)]

// Lets the derives always emit `::cqrs::...`, even inside this crate's tests.
extern crate self as cqrs;

mod aggregate;
mod codec;
mod command_handler;
mod criteria;
mod cursor;
mod dead_letter;
mod event;
mod feed;
mod forward;
mod inline;
mod interceptor;
mod loader;
mod message;
mod projection;
mod snapshot;
mod store;
#[cfg(feature = "tokio")]
mod streaming;
mod sync;
pub mod testing;
mod token;
mod transaction;
mod unit;
#[cfg(feature = "json")]
mod upcast;

pub use aggregate::{Aggregate, EventAppender};
#[cfg(feature = "json")]
pub use codec::Json;
#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Codec, CodecError};
pub use command_handler::{
    Autocommit, CommandContext, CommandHandler, EventPublisher, InTransaction, Pending, UnitMode, UnitOfWork, Write,
    append_writes, dispatch_command,
};
pub use criteria::{AppendCondition, Criterion, EventCriteria, Tag, TagResolver};
pub use cursor::{Cursor, Gap, Horizon};
pub use dead_letter::{DeadLetter, DeadLetterQueue, InMemoryDeadLetters, RetryOutcome, retry_dead_letters};
pub use event::{DomainEvent, EventEnvelope, Metadata, StreamId};
pub use feed::{EventFeed, QuerySubscriptions};
pub use forward::{FixedTopic, Forward, Outgoing, Publish, RoutedTopic, TopicOf};
pub use inline::{
    EventProcessor, HandlerFailure, InlineProjection, InlinePublisher, Isolated, LogFailures, OnFailure,
    ProcessingFailure,
};
pub use interceptor::{EventChain, EventInterceptor, Intercepted, Interceptor};
pub use loader::{AggregateLoader, Loaded, New, NotFound};
pub use message::Handles;
pub use message::{
    Command, CommandGateway, Dispatch, ErrorOf, EventHandler, HandlerError, Message, OutputOf, ProcessingContext,
    Query, QueryGateway, QueryHandler,
};
#[doc(hidden)]
pub use projection::{HasNamespace, HasUpdates, NamespaceHandler, namespace_id};
pub use projection::{Projection, Route};
pub use snapshot::{InMemorySnapshots, SnapshotStore};
pub use store::{
    AppendEvents, EventStore, ExpectedVersion, InMemoryEventStore, LoadStream, LogPage, ReadLog, Recorded,
    SourceEvents, StoreError, TaggedEvent,
};
#[cfg(feature = "tokio")]
pub use streaming::{
    CatchUp, EmitUpdates, Events, GlobalProgress, LiveEvents, ProcessorHandle, ProcessorProgress, ProcessorStatus,
    Projected, ProjectionHandle, ProjectionProcessors, ProjectionProgress, QueryUpdateChannel, QueryUpdateEmitter,
    ResetError, SubscriptionOptions, SubscriptionQueries, TrackingOptions, TrackingProcessor, UpdatesOf, emit_updates,
    head_of, subscribe,
};
pub use token::{InMemoryTokenStore, TokenStore, TrackingToken, segment_of};
pub use transaction::TransactionManager;
#[cfg(feature = "tokio")]
pub use unit::Units;
pub use unit::{Cqrs, CqrsBuilder};
#[cfg(feature = "json")]
pub use upcast::{StoredEvent, Upcast, Upcaster, upcast};

#[cfg(feature = "derive")]
pub use cqrs_derive::{
    AggregateRoot, Command, DomainEvent, Query, QueryUpdates, aggregate, command, command_gateway, command_handler,
    event_handler, projection, query, query_gateway, query_handler,
};

/// Everything a domain or application module usually needs.
pub mod prelude {
    pub use crate::{
        Aggregate, Command, CommandContext, CommandGateway, CommandHandler, DomainEvent, EventAppender, EventEnvelope,
        EventFeed, EventHandler, EventPublisher, HandlerError, Message, Query, QueryGateway, QueryHandler,
        QuerySubscriptions, TagResolver,
    };
}

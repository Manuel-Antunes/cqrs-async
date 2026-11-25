//! CQRS and event sourcing for Rust, statically dispatched: no handler
//! registry behind `dyn`, no `Box<dyn Future>`, no `Any` and no `TypeId`
//! (only errors are boxed, in [`HandlerError`]).
//! Routing a message to its handler is resolved by the type checker, so a
//! missing or duplicated handler is a compile error instead of a runtime one.
//!
//! A library, not a framework: the application owns its repositories, its
//! database and its transactions, and lends them to the handlers.
//!
//! # Quickstart
//!
//! An account opened and credited through commands; its balance, a read
//! model kept by a projection, answered by a query. In memory and without
//! a runtime: the projection runs inline, right after each commit.
//!
//! ```
//! use std::{collections::HashMap, sync::{Arc, Mutex}};
//!
//! use cqrs::{
//!     Cqrs, aggregate::AggregateLoader, prelude::*, processing::InlinePublisher,
//!     store::{InMemoryEventStore, StoreError}, testing::block_on,
//! };
//!
//! // The domain: what happened to an account, and the account deciding.
//! #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
//! enum AccountEvent {
//!     Opened,
//!     Deposited { amount: u64 },
//! }
//!
//! #[cqrs::aggregate(name = "account", event = AccountEvent)]
//! struct Account { id: u32, balance: u64 }
//!
//! impl Account {
//!     fn apply(&mut self, event: &AccountEvent) {
//!         if let AccountEvent::Deposited { amount } = event {
//!             self.balance += amount;
//!         }
//!     }
//!
//!     fn deposit(&self, amount: u64, events: &mut impl EventAppender<AccountEvent>) -> Result<(), BankError> {
//!         if amount == 0 {
//!             return Err(BankError::NothingToDeposit);
//!         }
//!         events.append(AccountEvent::Deposited { amount });
//!         Ok(())
//!     }
//! }
//!
//! #[derive(Debug, PartialEq)]
//! enum BankError { NothingToDeposit, NoSuchAccount, Store(String) }
//!
//! impl From<StoreError> for BankError {
//!     fn from(error: StoreError) -> Self { Self::Store(error.to_string()) }
//! }
//!
//! // The commands, and the gateway handling them, one impl each.
//! #[cqrs::command(error = BankError)]
//! struct Open { id: u32 }
//!
//! #[cqrs::command(error = BankError)]
//! struct Deposit { id: u32, amount: u64 }
//!
//! #[cqrs::command_gateway(handles(Open, Deposit))]
//! struct Bank<P> {
//!     accounts: AggregateLoader<Account, InMemoryEventStore<AccountEvent>>,
//!     #[gateway(unit)]
//!     events: P,
//! }
//!
//! #[cqrs::command_handler]
//! impl<P> CommandHandler<Open> for Bank<P> {
//!     async fn handle(&self, command: &Open, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
//!         ctx.append(AccountEvent::Opened);
//!         ctx.record(New::<Account>(command.id)); // a new stream: fails if it exists
//!         Ok(())
//!     }
//! }
//!
//! #[cqrs::command_handler]
//! impl<P> CommandHandler<Deposit> for Bank<P> {
//!     async fn handle(&self, command: &Deposit, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
//!         let account = self.accounts.load(&command.id).await?.ok_or(BankError::NoSuchAccount)?;
//!         account.deposit(command.amount, ctx)?;
//!         ctx.record(account); // appended and committed once the handler returns
//!         Ok(())
//!     }
//! }
//!
//! // The read side: a projection keeps the balances, a query reads them.
//! type Balances = Arc<Mutex<HashMap<u32, u64>>>;
//!
//! #[cqrs::projection(name = "account", events = AccountEvent, namespaces("balances"))]
//! struct AccountProjection { balances: Balances }
//!
//! #[cqrs::event_handler(namespace = "balances")]
//! impl EventHandler for AccountProjection {
//!     async fn handle(&self, event: &EventEnvelope<AccountEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
//!         let id = event.aggregate_id::<Account>().ok_or_else(|| HandlerError::msg("not an account"))?;
//!         let mut balances = self.balances.lock().unwrap();
//!         let balance = balances.entry(id).or_default();
//!         if let AccountEvent::Deposited { amount } = event.payload {
//!             *balance += amount;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[cqrs::query(output = Option<u64>, error = BankError)]
//! struct Balance { id: u32 }
//!
//! #[cqrs::query_gateway(handles(Balance))]
//! struct Accounts { balances: Balances }
//!
//! #[cqrs::query_handler]
//! impl QueryHandler<Balance> for Accounts {
//!     async fn handle(&self, query: Balance) -> Result<Option<u64>, BankError> {
//!         Ok(self.balances.lock().unwrap().get(&query.id).copied())
//!     }
//! }
//!
//! // Wiring: the store, the publisher running the projection inline, the unit.
//! let store = InMemoryEventStore::new();
//! let balances = Balances::default();
//! let publisher = InlinePublisher::new(store.clone())
//!     .with_projection(AccountProjection { balances: Arc::clone(&balances) });
//! let bank = Cqrs::from_publisher(publisher)
//!     .commands(|events| Bank { accounts: AggregateLoader::new(store), events })
//!     .queries(Accounts { balances })
//!     .build();
//!
//! block_on(async {
//!     bank.commands().send(Open { id: 1 }).await?;
//!     bank.commands().send(Deposit { id: 1, amount: 30 }).await?;
//!     assert_eq!(bank.commands().send(Deposit { id: 1, amount: 0 }).await, Err(BankError::NothingToDeposit));
//!     assert_eq!(bank.queries().query(Balance { id: 1 }).await?, Some(30));
//!     Ok::<(), BankError>(())
//! })?;
//! # Ok::<(), BankError>(())
//! ```
//!
//! # Modules
//!
//! - [`aggregate`](mod@aggregate): the domain side. An [`Aggregate`]
//!   decides in `&self` methods that append events to an
//!   [`EventAppender`]; an
//!   [`AggregateLoader`] rebuilds it from its
//!   events (and snapshots).
//! - [`command`](mod@command): commands and their handlers
//!   ([`CommandHandler<C>`](command::CommandHandler), one impl per command on
//!   the gateway struct), run in a unit of work that appends what they
//!   recorded and commits, in their transaction if they take one.
//! - [`query`](mod@query): queries, their handlers, and subscription queries (the
//!   current result, then the updates a projection pushes).
//! - [`event`]: events, their envelope, metadata and tags (DCB criteria).
//! - [`store`]: the ports a store implements (load, append, source, read the
//!   log), the in-memory store, codecs and upcasters.
//! - [`processing`]: event handlers and projections, tracking processors,
//!   tokens, transactions, dead letters, interceptors, inline processing.
//! - [`Cqrs`]: an application's unit (gateways, events, processors), wired
//!   by a typed builder. [`prelude`] has what most modules use.
//!
//! The macros live at the root: `#[cqrs::aggregate]`, `#[cqrs::command]`,
//! `#[cqrs::query]`, `#[cqrs::command_gateway]`, `#[cqrs::query_gateway]`,
//! `#[cqrs::command_handler]`, `#[cqrs::query_handler]`,
//! `#[cqrs::projection]`, `#[cqrs::event_handler]` and
//! `#[derive(cqrs::DomainEvent)]`.
//!
//! # Features
//!
//! - `derive` (default): the derives and attributes.
//! - `json`: events stored as JSON, upcasters, and the `Json` codec.
//! - `protobuf`: the `Protobuf` codec, for `prost` messages.
//! - `tokio` (default): live event processing ([`Events`]):
//!   catch-up subscriptions, tracking processors, subscription queries and
//!   the broadcast after commit. Without it the crate is runtime-agnostic:
//!   every contract is a `Future` or a `Stream` (`futures-core`), and any
//!   runtime can implement them.

#![warn(missing_docs, missing_debug_implementations)]

// Lets the derives always emit `::cqrs::...`, even inside this crate's tests.
extern crate self as cqrs;

#[path = "aggregate.rs"]
mod aggregate_root;
mod codec;
mod command_handler;
mod criteria;
mod cursor;
mod dead_letter;
#[path = "event.rs"]
mod event_envelope;
#[path = "store.rs"]
mod event_store;
mod feed;
mod forward;
mod inline;
mod interceptor;
mod loader;
mod message;
mod projection;
mod schedule;
mod snapshot;
#[cfg(feature = "tokio")]
mod streaming;
mod sync;
pub mod testing;
mod token;
mod transaction;
mod unit;
#[cfg(feature = "json")]
mod upcast;

// What the crate's own modules name as `crate::Item`; the public paths are
// the modules below.
#[allow(unused_imports)]
pub(crate) use self::{aggregate::*, command::*, event::*, processing::*, query::*, store::*};
pub(crate) use projection::HasUpdates;

pub use interceptor::{Intercepted, Interceptor};
pub use message::{Dispatch, ErrorOf, Handles, Message, OutputOf};
#[cfg(feature = "tokio")]
pub use unit::Units;
pub use unit::{Cqrs, CqrsBuilder};

#[cfg(feature = "derive")]
pub use cqrs_derive::{
    DomainEvent, QueryUpdates, aggregate, command, command_gateway, command_handler, event_handler, projection, query,
    query_gateway, query_handler,
};

pub mod aggregate {
    //! The domain side: aggregates decide in `&self` methods, appending
    //! events to an [`EventAppender`]; an [`AggregateLoader`] rebuilds them
    //! from their events, from a snapshot if there is one.

    pub use crate::aggregate_root::{Aggregate, EventAppender};
    pub use crate::loader::{AggregateLoader, Loaded, New, NotFound};
    pub use crate::snapshot::{InMemorySnapshots, SnapshotStore};
}

pub mod command {
    //! Commands: one [`CommandHandler<C>`] impl per command on the gateway
    //! struct (`#[cqrs::command_gateway]`), each run in a unit of work
    //! ([`UnitOfWork`]) that appends what the handler recorded in its
    //! [`CommandContext`] and commits.

    pub use crate::command_handler::{
        Autocommit, CommandContext, CommandHandler, EventPublisher, InTransaction, Pending, UnitMode, UnitOfWork,
        Write, append_writes, dispatch_command,
    };
    pub use crate::message::{Command, CommandGateway};
    #[cfg(feature = "tokio")]
    pub use crate::schedule::deliver_scheduled;
    pub use crate::schedule::{CommandSchedule, DeliveryOptions, InMemorySchedule, ScheduleToken, Scheduled};
}

pub mod query {
    //! Queries: one [`QueryHandler<Q>`] impl per query on the gateway struct
    //! (`#[cqrs::query_gateway]`), and subscription queries: the current
    //! result, then what a projection emits through its
    //! `QueryUpdateEmitter` (with the `tokio` feature).

    pub use crate::feed::QuerySubscriptions;
    pub use crate::message::{Query, QueryGateway, QueryHandler};
    #[cfg(feature = "tokio")]
    pub use crate::streaming::{
        CatchUp, EmitUpdates, Projected, QueryUpdateChannel, QueryUpdateEmitter, SubscriptionQueries, UpdatesOf,
        emit_updates, head_of,
    };
    #[cfg(feature = "derive")]
    pub use cqrs_derive::QueryUpdates;
}

pub mod event {
    //! Events: the [`DomainEvent`] contract, the [`EventEnvelope`] a committed
    //! event travels in, its [`Metadata`], and its tags, which criteria
    //! ([`EventCriteria`]) select events by.

    pub use crate::criteria::{AppendCondition, Criterion, EventCriteria, Tag, TagResolver};
    pub use crate::event_envelope::{DomainEvent, EventEnvelope, Metadata, StreamId};
    #[cfg(feature = "derive")]
    pub use cqrs_derive::DomainEvent;
}

pub mod store {
    //! What a store implements, one port per capability ([`LoadStream`],
    //! [`AppendEvents`], [`SourceEvents`], [`ReadLog`]), the in-memory
    //! [`InMemoryEventStore`], reading the log as a stream ([`EventFeed`]),
    //! and how events are encoded ([`Codec`], upcasters).

    #[cfg(feature = "json")]
    pub use crate::codec::Json;
    #[cfg(feature = "protobuf")]
    pub use crate::codec::Protobuf;
    pub use crate::codec::{Codec, CodecError};
    pub use crate::cursor::{Cursor, Gap, Horizon};
    pub use crate::event_store::{
        AppendEvents, EventStore, ExpectedVersion, InMemoryEventStore, LoadStream, LogPage, ReadLog, Recorded,
        SourceEvents, StoreError, TaggedEvent,
    };
    pub use crate::feed::EventFeed;
    #[cfg(feature = "tokio")]
    pub use crate::streaming::{LiveEvents, SubscriptionOptions, subscribe};
    #[cfg(feature = "json")]
    pub use crate::upcast::{StoredEvent, Upcast, Upcaster, upcast};
}

pub mod processing {
    //! What follows the committed events: [`EventHandler`]s and
    //! [`Projection`]s, run by tracking processors (with the `tokio`
    //! feature, from [`Events`]) or inline ([`InlinePublisher`]), with their
    //! tokens, transactions, dead letters and interceptors.

    pub use crate::dead_letter::{
        DeadLetter, DeadLetterQueue, EnqueueDecision, EnqueuePolicy, InMemoryDeadLetters, ParkAfter, RetryOutcome,
        retry_dead_letters,
    };
    pub use crate::forward::{FixedTopic, Forward, Outgoing, Publish, RoutedTopic, TopicOf};
    pub use crate::inline::{
        EventProcessor, HandlerFailure, InlineProjection, InlinePublisher, Isolated, LogFailures, OnFailure,
        ProcessingFailure,
    };
    pub use crate::interceptor::{EventChain, EventInterceptor};
    pub use crate::message::{EventHandler, HandlerError, ProcessingContext};
    pub use crate::projection::{Projection, Route};
    #[cfg(feature = "tokio")]
    pub use crate::streaming::{
        Events, GlobalProgress, ProcessorHandle, ProcessorProgress, ProcessorStatus, ProjectionHandle,
        ProjectionProcessors, ProjectionProgress, ResetError, TrackingOptions, TrackingProcessor,
    };
    pub use crate::token::{InMemoryTokenStore, TokenStore, TrackingToken, segment_of};
    pub use crate::transaction::TransactionManager;
}

/// What most domain and application modules use: `use cqrs::prelude::*;`.
pub mod prelude {
    pub use crate::aggregate::{Aggregate, EventAppender, Loaded, New, NotFound};
    pub use crate::command::{Command, CommandContext, CommandGateway, CommandHandler};
    pub use crate::event::{DomainEvent, EventEnvelope, Metadata, TagResolver};
    pub use crate::processing::{EventHandler, HandlerError, ProcessingContext};
    #[cfg(feature = "tokio")]
    pub use crate::query::QueryUpdateEmitter;
    pub use crate::query::{Query, QueryGateway, QueryHandler, QuerySubscriptions};
}

/// The paths the macros write: stable whatever module an item lives in.
#[doc(hidden)]
pub mod __private {
    pub use crate::projection::{HasNamespace, HasUpdates, NamespaceHandler, namespace_id};
    pub use crate::{
        Dispatch, ErrorOf, Handles, Message, OutputOf, aggregate::*, command::*, event::*, processing::*, query::*,
        store::*,
    };
}

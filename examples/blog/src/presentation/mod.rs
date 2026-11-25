//! Presentation layer: the application exposed as a GraphQL API over HTTP
//! and WebSocket.
//!
//! - [`graphql`]: the schema. Queries and mutations go through the
//!   application's gateways; subscriptions stream committed events.
//! - [`http`]: the axum router serving it.
//!
//! Like the application, it is written against ports only: [`BlogApi`]
//! names what it needs (the gateways and the event feed), and the
//! composition root decides what implements it. No resolver knows the event
//! store, the repository or the read model.

pub mod graphql;
pub mod http;

use std::sync::Arc;

use cqrs::{Cqrs, store::EventFeed};

use crate::{
    application::{
        commands::PostCommands,
        queries::{PostQueries, PostSubscriptions},
    },
    domain::PostEvent,
};

/// Everything the API reaches into: one port per kind of message.
///
/// The bounds are the contract: the API compiles against any application
/// that can send these commands, answer these queries and stream these
/// events, a test double included.
pub trait BlogApi: Send + Sync + 'static {
    /// Sends the commands.
    type Commands: PostCommands;
    /// Answers the queries, and streams their results as they change.
    type Queries: PostQueries + PostSubscriptions;
    /// Streams the committed events (the framework's port: no feed to write).
    type Events: EventFeed<PostEvent>;

    /// The command side.
    fn commands(&self) -> &Self::Commands;

    /// The query side.
    fn queries(&self) -> &Self::Queries;

    /// The event feed.
    fn events(&self) -> &Self::Events;
}

/// A shared API is an API.
impl<A: BlogApi> BlogApi for Arc<A> {
    type Commands = A::Commands;
    type Queries = A::Queries;
    type Events = A::Events;

    fn commands(&self) -> &Self::Commands {
        (**self).commands()
    }

    fn queries(&self) -> &Self::Queries {
        (**self).queries()
    }

    fn events(&self) -> &Self::Events {
        (**self).events()
    }
}

/// A CQRS unit exposing these ports is the API: whatever its gateways,
/// interceptors and stores.
impl<C, Q, E> BlogApi for Cqrs<C, Q, E>
where
    C: PostCommands + 'static,
    Q: PostQueries + PostSubscriptions + 'static,
    E: EventFeed<PostEvent> + 'static,
{
    type Commands = C;
    type Queries = Q;
    type Events = E;

    fn commands(&self) -> &C {
        Cqrs::commands(self)
    }

    fn queries(&self) -> &Q {
        Cqrs::queries(self)
    }

    fn events(&self) -> &E {
        Cqrs::events(self)
    }
}

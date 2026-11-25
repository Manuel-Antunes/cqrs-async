//! Streams as ports: committed events ([`EventFeed`]) and subscription
//! queries ([`QuerySubscriptions`]).

use std::{future::Future, sync::Arc};

use futures_core::Stream;

use crate::{DomainEvent, EventEnvelope, Query, QueryGateway, StoreError};

/// Committed events of type `E`, as they happen: what subscriptions
/// (GraphQL, SSE, WebSocket), sagas and long-running reactions consume.
///
/// The framework provides it (`Events`, with the `tokio` feature), so an
/// application never writes its own feed: it names this port, and the
/// composition root hands it the framework's implementation.
///
/// Delivery is complete and ordered: a subscription yields every event after
/// the given position exactly once, in position order, then keeps following
/// new ones.
pub trait EventFeed<E: DomainEvent>: Send + Sync {
    /// Every committed event with `position > after`, then the new ones as
    /// they are committed. The stream ends after yielding an error.
    ///
    /// The stream does not borrow `self` (`use<Self, E>`), so it can outlive
    /// the call: returned from a GraphQL resolver, an SSE handler, or moved
    /// into a task.
    fn subscribe(
        &self,
        after: u64,
    ) -> impl Stream<Item = Result<Arc<EventEnvelope<E>>, StoreError>> + Send + use<Self, E>;

    /// The position of the last committed event: subscribing after it yields
    /// only what happens from now on.
    fn last_position(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

/// Subscription queries: the query's current result, then a new
/// one each time it changes. The client subscribes to the **same shape** it
/// queries, so no event-to-API conversion is needed.
///
/// A new result is computed only after the projection has handled the event
/// that may change it, so an update never shows a stale read model; equal
/// consecutive results are not repeated.
pub trait QuerySubscriptions<Q: Query>: QueryGateway<Q> {
    /// The current result of `query`, then each new one, until dropped.
    fn subscribe(&self, query: Q) -> impl Stream<Item = Result<Q::Output, Q::Error>> + Send + use<Self, Q>;
}

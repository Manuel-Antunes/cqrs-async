//! Projections (Axon 5's event handling components and processors): an
//! [`EventHandler`](crate::EventHandler) holding a read model's
//! dependencies, written with `#[cqrs::projection(namespace = "...")]` on
//! its impl and run by a tracking processor named after it (see the
//! example on [`EventHandler`](crate::EventHandler)).
//!
//! The unit of work belongs to the token store ([`TransactionalTokenStore`]),
//! not to the projection: one writing SQL names the database's transaction
//! in its context (`ProcessingContext<'_, PgTransaction>`); one that does
//! not runs with any store.

use std::future::Future;

use crate::{StoreError, TokenStore, TokenUpdate, TrackingToken};

/// An event handling component with a namespace: the tracking processor
/// running it (Axon's `@Namespace`), and the key of its tokens.
/// Implemented by `#[cqrs::projection(namespace = "...")]`;
/// [`Events::projection`](crate::Events::projection) runs it.
pub trait Projection: Send + Sync {
    /// The processor's name.
    const NAMESPACE: &'static str;
}

/// A shared projection keeps its namespace.
impl<P: Projection> Projection for std::sync::Arc<P> {
    const NAMESPACE: &'static str = P::NAMESPACE;
}

/// A token store whose writes can join a transaction the handler also
/// writes in (Axon's transaction manager). `Transaction` is what handlers
/// run in.
pub trait TransactionalTokenStore: TokenStore {
    /// The unit of work: a database transaction, or `()` in memory.
    type Transaction: Send;

    /// Starts a unit of work.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, StoreError>> + Send;

    /// Stores `token`'s cursor within `tx`, if its owner still holds the
    /// segment ([`TokenUpdate::ClaimLost`] otherwise).
    fn store_in(
        &self,
        tx: &mut Self::Transaction,
        token: TrackingToken<'_>,
    ) -> impl Future<Output = Result<TokenUpdate, StoreError>> + Send;

    /// Commits `tx`.
    fn commit(&self, tx: Self::Transaction) -> impl Future<Output = Result<(), StoreError>> + Send;
}

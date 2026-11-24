//! Projections (Axon 5's event handling components and processors): an
//! [`EventHandler`](crate::EventHandler) holding a read model's
//! dependencies, written with `#[cqrs::projection(namespace = "...")]` on
//! its impl and run by a tracking processor named after it (see the
//! example on [`EventHandler`](crate::EventHandler)).
//!
//! The unit of work belongs to the processor's
//! [`TransactionManager`](crate::TransactionManager), not to the
//! projection: one writing SQL names the database's transaction in its
//! context (`ProcessingContext<'_, PgTransaction>`); one that does not runs
//! with any.

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

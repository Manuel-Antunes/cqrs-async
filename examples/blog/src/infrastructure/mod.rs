//! Infrastructure layer: adapters implementing the inner layers' ports.
//!
//! - [`EventSourcedPostRepository`]: the command side's
//!   [`PostRepository`](crate::application::commands::PostRepository), each
//!   post rebuilt from its events.
//! - [`InMemoryPostSummaryRepository`]: the read model's
//!   [`PostSummaryRepository`](crate::application::queries::PostSummaryRepository),
//!   in memory.
//! - [`SeaOrmPostRepository`] and [`SeaOrmPostSummaries`]: the same ports on
//!   PostgreSQL with SeaORM, state and read model in the library's
//!   transactions.
//! - [`InMemoryNotifier`]: a [`Notifier`](crate::application::projections::Notifier)
//!   that records instead of sending.
//! - [`Audit`] and [`Measured`]: interceptors around the command gateway
//!   and the projection (audit log, projection metrics).

/// The posts' repository, by their events.
mod event_sourced_post_repository;
/// The notifier adapter.
mod notifier;
/// Interceptors: cross-cutting concerns kept out of the handlers.
mod observability;
/// The read model's repository, in memory.
mod post_summary_repository;
/// PostgreSQL with SeaORM.
pub mod postgres;

pub use event_sourced_post_repository::EventSourcedPostRepository;
pub use notifier::InMemoryNotifier;
pub use observability::{Audit, AuditEntry, AuditLog, Measured, ProjectionMetrics};
pub use post_summary_repository::InMemoryPostSummaryRepository;
pub use postgres::{SeaOrmPostRepository, SeaOrmPostSummaries};

//! Infrastructure layer: adapters implementing the inner layers' ports.
//!
//! - The command side's
//!   [`PostRepository`](crate::application::commands::PostRepository) needs
//!   no adapter here for posts rebuilt from their events: the repository the
//!   engine hands out is one (the port's file implements it for it).
//! - [`InMemoryPostSummaryRepository`]: the read model's
//!   [`PostSummaryRepository`](crate::application::queries::PostSummaryRepository),
//!   in memory.
//! - [`SeaOrmPostRepository`] and [`SeaOrmPostSummaries`]: the same ports on
//!   PostgreSQL with SeaORM, state, ids and read model in the library's
//!   transactions.
//! - [`InMemoryNotifier`]: a [`Notifier`](crate::application::projections::Notifier)
//!   that records instead of sending.
//! - [`Audit`] and [`Measured`]: interceptors around the command gateway
//!   and the projection (audit log, projection metrics).

/// The notifier adapter.
mod notifier;
/// Interceptors: cross-cutting concerns kept out of the handlers.
mod observability;
/// The read model's repository, in memory.
mod post_summary_repository;
/// PostgreSQL with SeaORM.
pub mod postgres;

pub use notifier::InMemoryNotifier;
pub use observability::{Audit, AuditEntry, AuditLog, Measured, ProjectionMetrics};
pub use post_summary_repository::InMemoryPostSummaryRepository;
pub use postgres::{SeaOrmPostRepository, SeaOrmPostSummaries};

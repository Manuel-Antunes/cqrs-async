//! The blog's infrastructure on PostgreSQL, with SeaORM: the library's
//! stores keep the events and the tokens; SeaORM, the blog's own choice,
//! keeps the posts' state and the read model, in the same transactions.
//!
//! - [`SeaOrm`](crate::infrastructure::postgres::SeaOrm): SeaORM over a borrowed sqlx transaction (the `cqrs-sea-orm`
//!   crate).
//! - [`SeaOrmPostRepository`]: the posts' state, saved with their events.
//! - [`SeaOrmPostSummaries`]: the read model, written in the projection's
//!   unit of work.
//! - [`Migrator`](crate::infrastructure::postgres::Migrator): the blog's migrations, the library's tables among them.

/// The tables, as SeaORM entities.
pub mod entities;
/// The schema.
mod migration;
/// The posts' repository.
mod post_repository;
/// The read model.
mod post_summaries;

pub use cqrs_sea_orm::SeaOrm;
pub use migration::Migrator;
pub use post_repository::SeaOrmPostRepository;
pub use post_summaries::SeaOrmPostSummaries;

//! The blog's infrastructure on PostgreSQL, with SeaORM: the library's
//! stores keep the events and the tokens; SeaORM, the blog's own choice,
//! keeps the posts' state and the read model, in the same transactions.
//!
//! - [`SeaOrm`]: SeaORM over a borrowed sqlx transaction.
//! - [`SeaOrmPostRepository`]: the posts' state, saved with their events.
//! - [`SeaOrmPostSummaries`]: the read model, written in the projection's
//!   unit of work.
//! - [`Migrator`]: the blog's migrations, the library's tables among them.

/// The tables, as SeaORM entities.
pub mod entities;
/// The schema.
mod migration;
/// The posts' repository.
mod post_repository;
/// The read model.
mod post_summaries;
/// SeaORM in a borrowed transaction.
mod view;

pub use migration::Migrator;
pub use post_repository::SeaOrmPostRepository;
pub use post_summaries::SeaOrmPostSummaries;
pub use view::SeaOrm;

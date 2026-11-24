//! PostgreSQL storage for the `cqrs` crate, over a plain `sqlx` pool: an
//! [`EventStore`](cqrs::EventStore) (with [`Upcaster`]s for old event
//! shapes), a [`SnapshotStore`](cqrs::SnapshotStore), a
//! [`TokenStore`](cqrs::TokenStore) and the
//! [`TransactionManager`](cqrs::TransactionManager) they share
//! ([`PostgresTransactions`]): [`Projection`](cqrs::Projection)s over
//! [`PgTransaction`] write their read model in the transaction that moves
//! the processor's token.
//!
//! ```text
//! let pool = PgPool::connect(&url).await?;
//! cqrs_postgres::migrate(&pool).await?;
//! let events = Events::new(PostgresEventStore::<PostEvent>::new(pool.clone(), "posts"))
//!     .with_tokens(PostgresTokenStore::new(pool.clone()))
//!     .with_transactions(PostgresTransactions::new(pool));
//! ```
//!
//! # Global order (Axon's JDBC design)
//!
//! Appends run in parallel: positions come from a sequence, so a commit may
//! become visible before an older one, leaving a temporary gap, and an
//! append that loses a race leaves a permanent one. Readers of the global
//! log (tracking processors, subscriptions, live queries) read through a
//! [`Cursor`](cqrs::Cursor), Axon's gap-aware tracking token: they keep
//! going past a gap and pick it up once it is committed.
//!
//! Axon gives a gap up after a timeout, so a transaction slower than that
//! loses its event. Here, each append takes a transaction id before its
//! positions (`cqrs_positions`), and each read reports its snapshot: a gap
//! seen in a snapshot can only be filled by a transaction older than that
//! snapshot's `xmax`, so once the oldest running transaction is newer, the
//! gap is given up, never earlier.
//!
//! # Criteria (Axon 5's DCB)
//!
//! Tags live in `cqrs_event_tags (tag, position)`, so reading the events of
//! a criterion is an index range per tag, not a scan of the log.
//!
//! Consistency is per piece of the domain, not per database: every append
//! takes advisory locks, shared, on keys derived from its events (each tag
//! from a `#[tag]` field, each event type), so appends never wait for each
//! other. Reading a criterion (`source`) and a conditional append take that
//! criterion's keys exclusive: they wait for the matching appends under way
//! and hold off new ones, so the check sees every matching event and later
//! ones take positions above the head the decision was read at. Decisions on
//! different tags (two courses) run in parallel; two on the same tag take
//! turns.
//!
//! # SQL checked at compile time
//!
//! Every statement is a `sqlx::query!`, checked against the schema in
//! `migrations/` when the crate builds: its columns, its parameters' types,
//! its results' types and nullability. The check runs against the database
//! named by `CQRS_POSTGRES_URL` (see `sqlx.toml`; never an application's
//! `DATABASE_URL`), or, without it, against the cache in `.sqlx/`, which is
//! what builds use by default. After changing a statement, with the
//! migrations applied to that database, refresh the cache:
//!
//! ```text
//! cargo sqlx prepare -- -p cqrs-postgres            # with sqlx-cli, or:
//! touch crates/cqrs-postgres/src/*.rs && SQLX_OFFLINE_DIR=$PWD/crates/cqrs-postgres/.sqlx \
//!     CQRS_POSTGRES_URL=postgres://... cargo build -p cqrs-postgres
//! ```

mod dead_letters;
mod event_store;
mod snapshot_store;
mod token_store;

pub use dead_letters::PostgresDeadLetters;
pub use event_store::PostgresEventStore;
pub use snapshot_store::PostgresSnapshotStore;
pub use token_store::{PgTransaction, PostgresTokenStore, PostgresTransactions};
// The JSON shape and upcasters live in the core (`json` feature), shared
// with any other store keeping JSON.
pub use cqrs::{StoredEvent, Upcast, Upcaster, upcast};
/// The `sqlx` this crate is built on: [`PgPool`](sqlx::PgPool) and
/// [`PgTransaction`] are in its API, so a new major version of `sqlx` is a
/// breaking change of this crate. Use this one to name them.
pub use sqlx;

use cqrs::StoreError;
use sqlx::PgPool;

/// The schema, as SQL migrations in order (name, statements): for the
/// application's own migration tool (`sqlx migrate`, `sea-orm-migration`,
/// refinery...), which then owns when they run.
///
/// The tables keep their names (`cqrs_events`, `cqrs_event_tags`,
/// `cqrs_snapshots`, `cqrs_tokens`, `cqrs_dead_letters`): every statement
/// of this crate is checked against them at compile time. To keep them apart
/// from the application's, run the migrations and the stores in a schema of
/// their own, chosen by the connection's `search_path`
/// (`PgConnectOptions::options([("search_path", "events")])`).
pub const MIGRATIONS: &[(&str, &str)] = &[("0001_cqrs", include_str!("../migrations/0001_cqrs.sql"))];

/// Creates the tables (idempotent), for applications that let the crate
/// run its own [`MIGRATIONS`]: in one implicit transaction behind a lock, so
/// instances starting together run them one after the other.
///
/// # Errors
///
/// If the database refuses the statements.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    let mut schema = String::from("SELECT pg_advisory_xact_lock(7239048102394);\n");
    for (_, statements) in MIGRATIONS {
        schema.push_str(statements);
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(schema))
        .execute(pool)
        .await
        .map_err(backend)?;
    Ok(())
}

/// Any database failure, as the core sees it: the original error, kept as
/// the source.
fn backend(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> StoreError {
    StoreError::backend(error)
}

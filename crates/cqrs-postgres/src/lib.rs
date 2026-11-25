//! PostgreSQL storage for the `cqrs` crate, over a plain `sqlx` pool: an
//! [`EventStore`](cqrs::store::EventStore) (with [`Upcaster`]s for old event
//! shapes), a [`SnapshotStore`](cqrs::aggregate::SnapshotStore), a
//! [`TokenStore`](cqrs::processing::TokenStore) and the
//! [`TransactionManager`](cqrs::processing::TransactionManager) they share
//! ([`PostgresTransactions`]): [`Projection`](cqrs::processing::Projection)s over
//! [`PgTransaction`] write their read model in the transaction that moves
//! the processor's token.
//!
//! ```text
//! let pg = cqrs_postgres::Postgres::new(PgPool::connect(&url).await?);
//! pg.migrate().await?;                                   // or MIGRATIONS, in your tool
//! let posts = Cqrs::new(pg.events::<PostEvent>("posts")) // one log per aggregate root
//!     .commands(..).projection(..).build();
//! let users = Cqrs::new(pg.events::<UserEvent>("users")).build();
//! ```
//!
//! [`Postgres`] is only a shortcut: the same by hand is
//! `Events::new(PostgresEventStore::new(pool, "posts")).with_tokens(PostgresTokenStore::new(pool)).with_transactions(PostgresTransactions::new(pool))`.
//!
//! # Global order
//!
//! Appends run in parallel: positions come from a sequence, so a commit may
//! become visible before an older one, leaving a temporary gap, and an
//! append that loses a race leaves a permanent one. Readers of the global
//! log (tracking processors, subscriptions, live queries) read through a
//! [`Cursor`](cqrs::store::Cursor), Axon's gap-aware tracking token: they keep
//! going past a gap and pick it up once it is committed.
//!
//! Axon gives a gap up after a timeout, so a transaction slower than that
//! loses its event. Here, each append takes a transaction id before its
//! positions (`cqrs_positions`), and each read reports its snapshot: a gap
//! seen in a snapshot can only be filled by a transaction older than that
//! snapshot's `xmax`, so once the oldest running transaction is newer, the
//! gap is given up, never earlier.
//!
//! # Criteria
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

#![warn(missing_docs, missing_debug_implementations)]

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
pub use cqrs::store::{StoredEvent, Upcast, Upcaster, upcast};
/// The `sqlx` this crate is built on: [`sqlx::PgPool`] and
/// [`PgTransaction`] are in its API, so a new major version of `sqlx` is a
/// breaking change of this crate. Use this one to name them.
pub use sqlx;

use cqrs::store::StoreError;
use sqlx::PgPool;

/// The events service of one log over PostgreSQL: its store, the tokens
/// and the transactions its processors (and commands) run in.
#[cfg(feature = "tokio")]
pub type PgEvents<E, U = ()> =
    cqrs::processing::Events<E, PostgresEventStore<E, U>, PostgresTokenStore, PostgresTransactions>;

/// The infrastructure every aggregate root of an application shares on one
/// database, defined once: the pool, the token store and the transactions.
/// Each root gets its own log ([`events`](Self::events)); cloning is cheap
/// (handles over the pool).
#[derive(Clone)]
pub struct Postgres {
    /// The connections.
    pool: PgPool,
    /// The processors' tokens, shared by every log (keyed by processor).
    tokens: PostgresTokenStore,
    /// The units of work, of processors and commands.
    transactions: PostgresTransactions,
}

impl ::std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("Postgres").finish_non_exhaustive()
    }
}

impl Postgres {
    /// The infrastructure over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self {
            tokens: PostgresTokenStore::new(pool.clone()),
            transactions: PostgresTransactions::new(pool.clone()),
            pool,
        }
    }

    /// The pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The store of the log `log`.
    pub fn store<E>(&self, log: &str) -> PostgresEventStore<E> {
        PostgresEventStore::new(self.pool.clone(), log)
    }

    /// The token store.
    pub fn tokens(&self) -> &PostgresTokenStore {
        &self.tokens
    }

    /// The transaction manager.
    pub fn transactions(&self) -> &PostgresTransactions {
        &self.transactions
    }

    /// The events service of the log `log`: what `Cqrs::new` takes for one
    /// aggregate root.
    #[cfg(feature = "tokio")]
    pub fn events<E>(&self, log: &str) -> PgEvents<E> {
        cqrs::processing::Events::new(self.store(log))
            .with_tokens(self.tokens.clone())
            .with_transactions(self.transactions.clone())
    }

    /// Runs the [`MIGRATIONS`] (see [`migrate`]).
    ///
    /// # Errors
    ///
    /// If the database refuses the statements.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        migrate(&self.pool).await
    }
}

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

/// A `bigint` the schema never stores negative (a position, a sequence, a
/// version, a count), as a `u64`: a negative one is a backend error, never
/// a silently wrapped number.
fn unsigned(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::backend(format!("{value} where the schema stores no negative number")))
}

/// A segment index or count as its `integer` column stores it (segments
/// are few: saturating is never reached).
fn to_i32(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

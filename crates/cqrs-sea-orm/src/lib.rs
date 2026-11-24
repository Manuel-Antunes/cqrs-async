//! SeaORM in the transactions of `cqrs-postgres`: [`SeaOrm`], a borrowed
//! view of a [`PgTransaction`] that SeaORM's query builders run on.
//!
//! The transaction the library hands out (a processor's unit of work, a
//! command's) is sqlx's, the one its stores append and move tokens in, with
//! their statements checked at compile time. SeaORM runs on top of it:
//!
//! ```text
//! // in a command handler taking the unit's transaction, or a projection's handler
//! post::Entity::insert(row).exec(&SeaOrm::new(tx)).await?;
//! ```
//!
//! SeaORM's own `DatabaseTransaction` cannot be the common type: it keeps
//! its connection private and does not lend it.

#![warn(missing_docs)]

use cqrs_postgres::{
    PgTransaction,
    sqlx::{self, PgConnection, Postgres},
};
use sea_orm::{ConnectionTrait, DbBackend, DbErr, ExecResult, QueryResult, RuntimeErr, Statement, Values};
use sea_query_sqlx::SqlxValues;
use tokio::sync::Mutex;

/// SeaORM over a borrowed sqlx transaction: what lets an application write
/// with SeaORM in the library's units of work (a processor's, where a read
/// model commits with its token; a command's, where an entity's state
/// commits with its events).
///
/// It implements SeaORM's `ConnectionTrait`, so every query builder runs on
/// it (`Entity::insert(..).exec(&SeaOrm::new(tx))`). It borrows the
/// transaction: it can neither commit it nor outlive it, which the borrow
/// checker enforces. The opposite does not work: SeaORM's
/// `DatabaseTransaction` does not lend its sqlx connection, and the
/// library's statements are sqlx's, checked at compile time.
///
pub struct SeaOrm<'a> {
    /// The transaction's connection. SeaORM queries through `&self`, so it
    /// is lent to one statement at a time.
    connection: Mutex<&'a mut PgConnection>,
}

impl<'a> SeaOrm<'a> {
    /// SeaORM in `tx`.
    pub fn new(tx: &'a mut PgTransaction) -> Self {
        Self {
            connection: Mutex::new(&mut **tx),
        }
    }
}

/// `statement` as a sqlx query with its values bound.
fn query(statement: &Statement) -> sqlx::query::Query<'_, Postgres, SqlxValues> {
    let values = statement.values.clone().unwrap_or(Values(Vec::new()));
    sqlx::query_with(sqlx::AssertSqlSafe(statement.sql.as_str()), SqlxValues(values))
}

/// A failed statement, as SeaORM reports one.
fn failed(error: sqlx::Error, query: bool) -> DbErr {
    let error = RuntimeErr::SqlxError(error.into());
    if query { DbErr::Query(error) } else { DbErr::Exec(error) }
}

#[async_trait::async_trait]
impl<'a> ConnectionTrait for SeaOrm<'a> {
    fn get_database_backend(&self) -> DbBackend {
        DbBackend::Postgres
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        let mut connection = self.connection.lock().await;
        let done = query(&statement).execute(&mut **connection).await;
        done.map(ExecResult::from).map_err(|error| failed(error, false))
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        let mut connection = self.connection.lock().await;
        let done = sqlx::raw_sql(sqlx::AssertSqlSafe(sql)).execute(&mut **connection).await;
        done.map(ExecResult::from).map_err(|error| failed(error, false))
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        let mut connection = self.connection.lock().await;
        let row = query(&statement).fetch_optional(&mut **connection).await;
        row.map(|row| row.map(QueryResult::from))
            .map_err(|error| failed(error, true))
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        let mut connection = self.connection.lock().await;
        let rows = query(&statement).fetch_all(&mut **connection).await;
        rows.map(|rows| rows.into_iter().map(QueryResult::from).collect())
            .map_err(|error| failed(error, true))
    }
}

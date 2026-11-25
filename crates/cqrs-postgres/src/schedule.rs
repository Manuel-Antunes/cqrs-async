//! Commands sent later, kept in PostgreSQL (`cqrs_scheduled`).

use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cqrs::{
    command::{CommandSchedule, ScheduleToken, Scheduled},
    event::Metadata,
    store::StoreError,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;

use crate::{PgTransaction, backend, event_store::from_micros, unsigned};

/// The commands of type `C` to send later, in the queue `queue` of
/// `cqrs_scheduled` (one queue per command type). Times are the database's
/// (`now()`), not each instance's clock; deliverers on several instances
/// claim different commands (`FOR UPDATE SKIP LOCKED`). Commands are stored
/// as JSON.
pub struct PostgresSchedule<C> {
    /// The connections.
    pool: PgPool,
    /// Its queue.
    queue: Arc<str>,
    /// The command type.
    command: PhantomData<fn() -> C>,
}

impl<C> PostgresSchedule<C> {
    /// The schedule `queue` over `pool`.
    pub fn new(pool: PgPool, queue: &str) -> Self {
        Self {
            pool,
            queue: queue.into(),
            command: PhantomData,
        }
    }
}

impl<C> Clone for PostgresSchedule<C> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            queue: Arc::clone(&self.queue),
            command: PhantomData,
        }
    }
}

impl<C> ::std::fmt::Debug for PostgresSchedule<C> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("PostgresSchedule")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl<C: Serialize + DeserializeOwned + Send + Sync> PostgresSchedule<C> {
    /// Schedules `command` for `due`, in a transaction of its own (from a
    /// command handler, use [`schedule_in`](CommandSchedule::schedule_in)
    /// with its transaction instead).
    ///
    /// # Errors
    ///
    /// The database's, or a command that does not serialize.
    pub async fn schedule(
        &self,
        due: SystemTime,
        command: &C,
        metadata: &Metadata,
    ) -> Result<ScheduleToken, StoreError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let token = self.schedule_in(&mut tx, due, command, metadata).await?;
        tx.commit().await.map_err(backend)?;
        Ok(token)
    }
}

/// Microseconds since the epoch, as the statements bind times.
fn micros(time: SystemTime) -> i64 {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
}

/// A duration in microseconds.
fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

impl<C: Serialize + DeserializeOwned + Send + Sync> CommandSchedule<C, PgTransaction> for PostgresSchedule<C> {
    async fn schedule_in(
        &self,
        tx: &mut PgTransaction,
        due: SystemTime,
        command: &C,
        metadata: &Metadata,
    ) -> Result<ScheduleToken, StoreError> {
        let id = sqlx::query_scalar!(
            "INSERT INTO cqrs_scheduled (queue, due_at, payload, metadata) \
             VALUES ($1, timestamptz 'epoch' + $2::bigint * interval '1 microsecond', $3::text::json, $4::text::json) \
             RETURNING id",
            &*self.queue,
            micros(due),
            serde_json::to_string(command).map_err(backend)?,
            serde_json::to_string(metadata).map_err(backend)?,
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(ScheduleToken(unsigned(id)?))
    }

    async fn cancel(&self, token: ScheduleToken) -> Result<bool, StoreError> {
        let cancelled = sqlx::query!(
            "DELETE FROM cqrs_scheduled WHERE id = $1 AND queue = $2 AND failed_at IS NULL",
            i64::try_from(token.0).map_err(backend)?,
            &*self.queue,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(cancelled.rows_affected() == 1)
    }

    async fn reschedule(&self, token: ScheduleToken, due: SystemTime) -> Result<bool, StoreError> {
        let moved = sqlx::query!(
            "UPDATE cqrs_scheduled SET due_at = timestamptz 'epoch' + $3::bigint * interval '1 microsecond', \
             claimed_until = NULL WHERE id = $1 AND queue = $2 AND failed_at IS NULL",
            i64::try_from(token.0).map_err(backend)?,
            &*self.queue,
            micros(due),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(moved.rows_affected() == 1)
    }

    async fn claim_due(&self, lease: Duration, limit: usize) -> Result<Vec<Scheduled<C>>, StoreError> {
        let rows = sqlx::query!(
            r#"UPDATE cqrs_scheduled SET claimed_until = now() + $2::bigint * interval '1 microsecond'
               WHERE id IN (
                   SELECT id FROM cqrs_scheduled
                   WHERE queue = $1 AND failed_at IS NULL AND due_at <= now()
                     AND (claimed_until IS NULL OR claimed_until < now())
                   ORDER BY due_at, id LIMIT $3
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING id, (extract(epoch FROM due_at) * 1000000)::bigint AS "due_us!",
                         payload::text AS "payload!", metadata::text AS "metadata!", attempts"#,
            &*self.queue,
            duration_micros(lease),
            i64::try_from(limit).unwrap_or(i64::MAX),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut due = rows
            .into_iter()
            .map(|row| {
                let mut scheduled = Scheduled::new(
                    ScheduleToken(unsigned(row.id)?),
                    from_micros(row.due_us),
                    serde_json::from_str(&row.payload).map_err(backend)?,
                    serde_json::from_str(&row.metadata).map_err(backend)?,
                );
                scheduled.attempts = u32::try_from(row.attempts).map_err(backend)?;
                Ok(scheduled)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        due.sort_by_key(|scheduled| (scheduled.due, scheduled.token));
        Ok(due)
    }

    async fn delivered(&self, token: ScheduleToken) -> Result<(), StoreError> {
        sqlx::query!(
            "DELETE FROM cqrs_scheduled WHERE id = $1 AND queue = $2",
            i64::try_from(token.0).map_err(backend)?,
            &*self.queue,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn failed(&self, token: ScheduleToken, retry_at: Option<SystemTime>, error: &str) -> Result<(), StoreError> {
        sqlx::query!(
            "UPDATE cqrs_scheduled SET attempts = attempts + 1, claimed_until = NULL, last_error = $3, \
             due_at = coalesce(timestamptz 'epoch' + $4::bigint * interval '1 microsecond', due_at), \
             failed_at = CASE WHEN $4::bigint IS NULL THEN now() END \
             WHERE id = $1 AND queue = $2",
            i64::try_from(token.0).map_err(backend)?,
            &*self.queue,
            error,
            retry_at.map(micros),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
}

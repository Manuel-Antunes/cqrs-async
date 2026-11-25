use std::sync::Arc;

use cqrs::{
    processing::{DeadLetter, DeadLetterQueue},
    store::{StoreError, Upcaster},
};
use serde::de::DeserializeOwned;

use crate::{PgTransaction, PostgresEventStore, backend, event_store::from_micros};

/// The dead-letter queue in PostgreSQL: events a processor gave up on,
/// parked in `cqrs_dead_letters` (processor, sequence, position, cause, and
/// what the enqueue policy keeps: attempts, last touched, diagnostics).
///
/// A letter records the event's position and is read back from the event
/// store's log, so the event is neither copied nor decoded twice: give the
/// queue the store the processor reads from.
///
/// It is the queue of a [`PostgresTokenStore`](crate::PostgresTokenStore): a
/// letter enters and leaves it in that store's unit of work
/// ([`PgTransaction`]). Parking commits with the token moving past the
/// event, so a crash right after parking cannot lose the letter, nor leave
/// the token before it; a retry removes the letter in the transaction that
/// applies its event, so the event is applied once even if the process dies
/// or two retries run at the same time. Parking the same event twice leaves
/// one letter.
pub struct PostgresDeadLetters<E, U = ()> {
    /// Where the parked events are read back from.
    store: PostgresEventStore<E, U>,
}

impl<E, U> ::std::fmt::Debug for PostgresDeadLetters<E, U> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("PostgresDeadLetters").finish_non_exhaustive()
    }
}

impl<E, U> PostgresDeadLetters<E, U> {
    /// A queue over the database of `store`, whose schema
    /// [`migrate`](crate::migrate) created.
    pub fn new(store: PostgresEventStore<E, U>) -> Self {
        Self { store }
    }
}

impl<E, U> Clone for PostgresDeadLetters<E, U> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<E, U> DeadLetterQueue<E, PgTransaction> for PostgresDeadLetters<E, U>
where
    E: DeserializeOwned + Send + Sync,
    U: Upcaster,
{
    async fn contains(&self, processor: &str, sequence: &str) -> Result<bool, StoreError> {
        sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2) AS "parked!""#,
            processor,
            sequence,
        )
        .fetch_one(self.store.pool())
        .await
        .map_err(backend)
    }

    async fn sequences(&self, processor: &str) -> Result<Vec<String>, StoreError> {
        sqlx::query_scalar!(
            r#"SELECT DISTINCT sequence AS "sequence!" FROM cqrs_dead_letters WHERE processor = $1 ORDER BY 1"#,
            processor,
        )
        .fetch_all(self.store.pool())
        .await
        .map_err(backend)
    }

    async fn first(&self, processor: &str, sequence: &str) -> Result<Option<DeadLetter<E>>, StoreError> {
        let Some(row) = sqlx::query!(
            r#"SELECT position, cause, attempts,
                      (extract(epoch FROM parked_at) * 1000000)::bigint AS "parked_us!",
                      (extract(epoch FROM last_touched) * 1000000)::bigint AS "touched_us?",
                      diagnostics::text AS "diagnostics!"
               FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2
               ORDER BY position LIMIT 1"#,
            processor,
            sequence,
        )
        .fetch_optional(self.store.pool())
        .await
        .map_err(backend)?
        else {
            return Ok(None);
        };
        let position = u64::try_from(row.position).map_err(backend)?;
        let event = self.store.event_at(position).await?.ok_or_else(|| {
            StoreError::backend(format!(
                "the event at position {position}, parked by {processor}, is not in the log"
            ))
        })?;
        let mut letter = DeadLetter::new(sequence, Arc::new(event), row.cause);
        letter.attempts = u32::try_from(row.attempts).map_err(backend)?;
        letter.parked_at = Some(from_micros(row.parked_us));
        letter.last_touched = row.touched_us.map(from_micros);
        letter.diagnostics = serde_json::from_str(&row.diagnostics).map_err(backend)?;
        Ok(Some(letter))
    }

    async fn enqueue_in(
        &self,
        tx: &mut PgTransaction,
        processor: &str,
        letter: DeadLetter<E>,
    ) -> Result<(), StoreError> {
        sqlx::query!(
            "INSERT INTO cqrs_dead_letters (processor, sequence, position, cause, attempts, diagnostics) \
             VALUES ($1, $2, $3, $4, $5, $6::text::json) ON CONFLICT DO NOTHING",
            processor,
            letter.sequence,
            i64::try_from(letter.event.position).map_err(backend)?,
            letter.cause,
            i32::try_from(letter.attempts).unwrap_or(i32::MAX),
            serde_json::to_string(&letter.diagnostics).map_err(backend)?,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn requeue_in(
        &self,
        tx: &mut PgTransaction,
        processor: &str,
        letter: &DeadLetter<E>,
    ) -> Result<bool, StoreError> {
        let requeued = sqlx::query!(
            "UPDATE cqrs_dead_letters SET cause = $4, attempts = $5, last_touched = now(), \
             diagnostics = $6::text::json WHERE processor = $1 AND sequence = $2 AND position = $3",
            processor,
            letter.sequence,
            i64::try_from(letter.event.position).map_err(backend)?,
            letter.cause,
            i32::try_from(letter.attempts).unwrap_or(i32::MAX),
            serde_json::to_string(&letter.diagnostics).map_err(backend)?,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(requeued.rows_affected() == 1)
    }

    /// Waits for a concurrent removal of the same letter to commit or roll
    /// back, then reports whether this one removed it.
    async fn remove_in(
        &self,
        tx: &mut PgTransaction,
        processor: &str,
        letter: &DeadLetter<E>,
    ) -> Result<bool, StoreError> {
        let removed = sqlx::query!(
            "DELETE FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2 AND position = $3",
            processor,
            letter.sequence,
            i64::try_from(letter.event.position).map_err(backend)?,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(removed.rows_affected() == 1)
    }
}

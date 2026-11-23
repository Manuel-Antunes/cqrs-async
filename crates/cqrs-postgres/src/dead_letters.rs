use std::sync::Arc;

use cqrs::{DeadLetter, DeadLetterQueue, StoreError, TransactionalDeadLetters, Upcaster};
use serde::de::DeserializeOwned;

use crate::{PgTransaction, PostgresEventStore, backend};

/// The dead-letter queue in PostgreSQL: events a processor gave up on,
/// parked in `cqrs_dead_letters` (processor, sequence, position, cause).
///
/// A letter records the event's position and is read back from the event
/// store's log, so the event is neither copied nor decoded twice: give the
/// queue the store the processor reads from.
///
/// Parking happens in the unit of work that moves the processor's token
/// ([`TransactionalDeadLetters`] over [`PgTransaction`]): a crash right after
/// parking cannot lose the letter, nor leave the token before it. Parking the
/// same event twice leaves one letter.
pub struct PostgresDeadLetters<E, U = ()> {
    /// Where the parked events are read back from.
    store: PostgresEventStore<E, U>,
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

impl<E, U> DeadLetterQueue<E> for PostgresDeadLetters<E, U>
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
            "SELECT position, cause FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2 \
             ORDER BY position LIMIT 1",
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
            StoreError::Backend(format!(
                "the event at position {position}, parked by {processor}, is not in the log"
            ))
        })?;
        Ok(Some(DeadLetter {
            sequence: sequence.to_owned(),
            event: Arc::new(event),
            cause: row.cause,
        }))
    }

    async fn remove_first(&self, processor: &str, sequence: &str) -> Result<(), StoreError> {
        sqlx::query!(
            "DELETE FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2 AND position = \
             (SELECT min(position) FROM cqrs_dead_letters WHERE processor = $1 AND sequence = $2)",
            processor,
            sequence,
        )
        .execute(self.store.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }
}

impl<E, U> TransactionalDeadLetters<E, PgTransaction> for PostgresDeadLetters<E, U>
where
    E: DeserializeOwned + Send + Sync,
    U: Upcaster,
{
    async fn enqueue_in(
        &self,
        tx: &mut PgTransaction,
        processor: &str,
        letter: DeadLetter<E>,
    ) -> Result<(), StoreError> {
        sqlx::query!(
            "INSERT INTO cqrs_dead_letters (processor, sequence, position, cause) VALUES ($1, $2, $3, $4) \
             ON CONFLICT DO NOTHING",
            processor,
            letter.sequence,
            i64::try_from(letter.event.position).map_err(backend)?,
            letter.cause,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(())
    }
}

use cqrs::{Cursor, Gap, StoreError, TokenStore, TokenUpdate, TrackingToken, TransactionalTokenStore};
use sqlx::PgPool;

use crate::backend;

/// Tracking processors' tokens in PostgreSQL: one row per (processor,
/// segment), shared by every instance using the database.
///
/// Claims use the **database's** clock (`now()`), so instances on different
/// machines agree on expiry; only the duration of the caller's claim is
/// used.
#[derive(Clone)]
pub struct PostgresTokenStore {
    /// The connection pool.
    pool: PgPool,
}

impl PostgresTokenStore {
    /// A token store over `pool`, whose schema [`migrate`](crate::migrate)
    /// created.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Stores the cursor of `processor`'s `segment` (`position`, `gaps` and
/// their horizons), if `owner` still holds it (affects one row, or none).
fn move_token<'q>(
    processor: &'q str,
    segment: u32,
    owner: &'q str,
    position: u64,
    gaps: &'q [i64],
    horizons: &'q [i64],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query!(
        "UPDATE cqrs_tokens SET position = $4, gaps = $5, gap_horizons = $6 \
         WHERE processor = $1 AND segment = $2 AND owner = $3",
        processor,
        segment as i32,
        owner,
        i64::try_from(position).unwrap_or(i64::MAX),
        gaps,
        horizons,
    )
}

/// A cursor as its columns store it.
fn columns(cursor: &Cursor) -> (Vec<i64>, Vec<i64>) {
    cursor
        .gaps()
        .iter()
        .map(|gap| (gap.position as i64, gap.horizon as i64))
        .unzip()
}

/// A cursor from its columns.
fn cursor(position: i64, gaps: &[i64], horizons: &[i64]) -> Cursor {
    let gaps = gaps.iter().zip(horizons).map(|(&position, &horizon)| Gap {
        position: position as u64,
        horizon: horizon as u64,
    });
    Cursor::new(position as u64, gaps)
}

impl TokenStore for PostgresTokenStore {
    async fn initialize(&self, processor: &str, segments: u32) -> Result<u32, StoreError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Instances starting together: one creates, the others see its count.
        sqlx::query!("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", processor)
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;
        let existing = sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM cqrs_tokens WHERE processor = $1"#,
            processor
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        let count = if existing == 0 {
            let count = segments.max(1);
            sqlx::query!(
                "INSERT INTO cqrs_tokens (processor, segment) SELECT $1, generate_series(0, $2 - 1)",
                processor,
                count as i32,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            count
        } else {
            existing as u32
        };
        tx.commit().await.map_err(backend)?;
        Ok(count)
    }

    async fn claim(
        &self,
        processor: &str,
        segment: u32,
        owner: &str,
        now: u64,
        until: u64,
    ) -> Result<Option<Cursor>, StoreError> {
        let lease_ms = until.saturating_sub(now) as f64;
        let token = sqlx::query!(
            "UPDATE cqrs_tokens \
             SET owner = $3, claimed_until = now() + make_interval(secs => $4::float8 / 1000.0) \
             WHERE processor = $1 AND segment = $2 \
               AND (owner IS NULL OR owner = $3 OR claimed_until < now()) \
             RETURNING position, gaps, gap_horizons",
            processor,
            segment as i32,
            owner,
            lease_ms,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(token.map(|token| cursor(token.position, &token.gaps, &token.gap_horizons)))
    }

    async fn store(&self, processor: &str, segment: u32, owner: &str, cursor: &Cursor) -> Result<bool, StoreError> {
        let (gaps, horizons) = columns(cursor);
        let updated = move_token(processor, segment, owner, cursor.position(), &gaps, &horizons)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn release(&self, processor: &str, segment: u32, owner: &str) -> Result<(), StoreError> {
        sqlx::query!(
            "UPDATE cqrs_tokens SET owner = NULL, claimed_until = NULL \
             WHERE processor = $1 AND segment = $2 AND owner = $3",
            processor,
            segment as i32,
            owner,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn cursors(&self, processor: &str) -> Result<Vec<Cursor>, StoreError> {
        let tokens = sqlx::query!(
            "SELECT position, gaps, gap_horizons FROM cqrs_tokens WHERE processor = $1 ORDER BY segment",
            processor,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(tokens
            .iter()
            .map(|token| cursor(token.position, &token.gaps, &token.gap_horizons))
            .collect())
    }

    async fn reset(&self, processor: &str, _now: u64, replay_until: u64) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Locking the rows makes a concurrent claim wait for the reset.
        let claimed = sqlx::query_scalar!(
            "SELECT owner IS NOT NULL AND claimed_until >= now() FROM cqrs_tokens \
             WHERE processor = $1 FOR UPDATE",
            processor,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?;
        if claimed.contains(&Some(true)) {
            return Ok(false);
        }
        sqlx::query!(
            "UPDATE cqrs_tokens SET position = 0, gaps = '{}', gap_horizons = '{}', owner = NULL, \
             claimed_until = NULL, replay_until = $2 WHERE processor = $1",
            processor,
            i64::try_from(replay_until).unwrap_or(i64::MAX),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(true)
    }

    async fn replay_until(&self, processor: &str) -> Result<u64, StoreError> {
        let until = sqlx::query_scalar!(
            r#"SELECT coalesce(max(replay_until), 0) AS "until!" FROM cqrs_tokens WHERE processor = $1"#,
            processor,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(until as u64)
    }
}

/// The transaction a [`Projection`](cqrs::Projection) over a
/// [`PostgresTokenStore`] writes in: `&mut **tx` is a connection.
pub type PgTransaction = sqlx::Transaction<'static, sqlx::Postgres>;

/// Read models in the same database as the tokens commit with them.
impl TransactionalTokenStore for PostgresTokenStore {
    type Transaction = PgTransaction;

    async fn begin(&self) -> Result<PgTransaction, StoreError> {
        self.pool.begin().await.map_err(backend)
    }

    async fn store_in(&self, tx: &mut PgTransaction, token: TrackingToken<'_>) -> Result<TokenUpdate, StoreError> {
        let (gaps, horizons) = columns(token.cursor);
        let moved = move_token(
            token.processor,
            token.segment,
            token.owner,
            token.cursor.position(),
            &gaps,
            &horizons,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(if moved.rows_affected() == 1 {
            TokenUpdate::Stored
        } else {
            TokenUpdate::ClaimLost
        })
    }

    async fn commit(&self, tx: PgTransaction) -> Result<(), StoreError> {
        tx.commit().await.map_err(backend)
    }
}

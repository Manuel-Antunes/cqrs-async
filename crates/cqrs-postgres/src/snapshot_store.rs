use std::marker::PhantomData;

use cqrs::{
    aggregate::{Aggregate, SnapshotStore},
    event::StreamId,
    store::StoreError,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;

use crate::backend;

/// Snapshots of aggregates `A` in PostgreSQL, as JSON (`serde`): the newest
/// one per aggregate, in `cqrs_snapshots`.
///
/// Snapshots are tagged with a revision of `A`'s shape (Axon's `@Revision`):
/// bump it with [`with_revision`](Self::with_revision) when the shape
/// changes, and older snapshots are ignored (the aggregate is replayed from
/// its events, then snapshotted again). A snapshot that fails to decode is
/// also only a full replay.
pub struct PostgresSnapshotStore<A> {
    /// The connection pool.
    pool: PgPool,
    /// The shape of `A` snapshots are read and written in.
    revision: i32,
    /// The aggregate type.
    aggregate: PhantomData<fn() -> A>,
}

impl<A> PostgresSnapshotStore<A> {
    /// A store over `pool`, whose schema [`migrate`](crate::migrate)
    /// created, at revision 1.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            revision: 1,
            aggregate: PhantomData,
        }
    }

    /// Reads and writes snapshots of shape `revision` only.
    #[must_use]
    pub fn with_revision(mut self, revision: u32) -> Self {
        self.revision = i32::try_from(revision).unwrap_or(i32::MAX);
        self
    }
}

impl<A> Clone for PostgresSnapshotStore<A> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            revision: self.revision,
            aggregate: PhantomData,
        }
    }
}

impl<A> SnapshotStore<A> for PostgresSnapshotStore<A>
where
    A: Aggregate + Serialize + DeserializeOwned,
{
    async fn load(&self, id: &A::Id) -> Result<Option<(A, u64)>, StoreError> {
        let stream = StreamId::of::<A>(id);
        let row = sqlx::query!(
            "SELECT state, version FROM cqrs_snapshots \
             WHERE stream_type = $1 AND stream_id = $2 AND revision = $3",
            stream.aggregate_type(),
            stream.id(),
            self.revision,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        row.map(|row| Ok((serde_json::from_value(row.state).map_err(backend)?, row.version as u64)))
            .transpose()
    }

    async fn save(&self, aggregate: &A, version: u64) -> Result<(), StoreError> {
        let stream = StreamId::of::<A>(aggregate.id());
        sqlx::query!(
            "INSERT INTO cqrs_snapshots (stream_type, stream_id, revision, version, state) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (stream_type, stream_id) DO UPDATE \
             SET revision = EXCLUDED.revision, version = EXCLUDED.version, state = EXCLUDED.state \
             WHERE cqrs_snapshots.version < EXCLUDED.version OR cqrs_snapshots.revision <> EXCLUDED.revision",
            stream.aggregate_type(),
            stream.id(),
            self.revision,
            i64::try_from(version).unwrap_or(i64::MAX),
            serde_json::to_value(aggregate).map_err(backend)?,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
}

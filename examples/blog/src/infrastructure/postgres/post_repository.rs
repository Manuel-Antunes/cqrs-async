use cqrs::{Announce, Appender, Loaded, PublishIn, StoreError};
use cqrs_postgres::{PgTransaction, sqlx::PgPool};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, Set, SqlxPostgresConnector, Statement,
    sea_query::OnConflict,
};

use super::{SeaOrm, entities::post};
use crate::{
    application::commands::PostRepository,
    domain::{Post, PostEvent, PostId},
};

/// The posts' repository with SeaORM: each post's state in the `posts`
/// table, saved in the transaction that appends its events (through the
/// publisher `P`), so they commit together or not at all. Finding a post
/// reads one row, whatever its history.
///
/// The row keeps the version of the post's stream: a save expects the
/// stream at that version, so two concurrent changes still conflict.
pub struct SeaOrmPostRepository<P> {
    /// Where its transactions are opened.
    pool: PgPool,
    /// SeaORM on the same pool, for reads outside a transaction.
    db: DatabaseConnection,
    /// Appends the events in the repository's transaction.
    events: P,
}

impl<P> SeaOrmPostRepository<P> {
    /// A repository over `pool`, appending through `events`.
    pub fn new(pool: PgPool, events: P) -> Self {
        Self {
            db: SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone()),
            pool,
            events,
        }
    }
}

/// A SeaORM failure, as the library's ports report one.
fn backend(error: sea_orm::DbErr) -> StoreError {
    StoreError::backend(error)
}

impl<P> PostRepository for SeaOrmPostRepository<P>
where
    P: PublishIn<PostEvent, PgTransaction> + Announce<PostEvent>,
{
    async fn next_id(&self) -> Result<PostId, StoreError> {
        let next = Statement::from_string(DbBackend::Postgres, "SELECT nextval('post_ids') AS id");
        let row = self.db.query_one_raw(next).await.map_err(backend)?;
        let id: i64 = row
            .ok_or_else(|| StoreError::backend("the post_ids sequence returned nothing"))?
            .try_get("", "id")
            .map_err(backend)?;
        Ok(PostId(u64::try_from(id).map_err(StoreError::backend)?))
    }

    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        let key = i64::try_from(id.0).map_err(StoreError::backend)?;
        let Some(row) = post::Entity::find_by_id(key).one(&self.db).await.map_err(backend)? else {
            return Ok(None);
        };
        let version = u64::try_from(row.version).map_err(StoreError::backend)?;
        Ok(Some(Loaded::at(Post::restore(id, row.title, row.published), version)))
    }

    async fn save(&self, post: Loaded<Post>, appender: &mut Appender<PostEvent>) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::backend)?;
        let committed = appender.commit_in(&mut tx, post, &self.events).await?;
        let state = committed.entity();
        let row = post::ActiveModel {
            id: Set(i64::try_from(cqrs::Aggregate::id(&**state).0).map_err(StoreError::backend)?),
            title: Set(state.title().to_owned()),
            published: Set(state.is_published()),
            version: Set(state.version().map_or(0, |version| version as i64)),
        };
        let upsert = OnConflict::column(post::Column::Id)
            .update_columns([post::Column::Title, post::Column::Published, post::Column::Version])
            .to_owned();
        post::Entity::insert(row)
            .on_conflict(upsert)
            .exec(&SeaOrm::new(&mut tx))
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(StoreError::backend)?;
        committed.announce();
        Ok(())
    }
}

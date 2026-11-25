use cqrs::{
    aggregate::{Aggregate, Loaded},
    store::StoreError,
};
use cqrs_postgres::PgTransaction;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Set, Statement, sea_query::OnConflict};

use super::{SeaOrm, entities::post};
use crate::{
    application::commands::PostRepository,
    domain::{Post, PostId},
};

/// The posts' repository with SeaORM: each post's state in the `posts`
/// table, read and written in the transaction of the command's unit of
/// work, which also appends the post's events: they commit together, or
/// not at all. Finding a post reads one row, whatever its history.
///
/// The row keeps the version of the post's stream: the unit appends
/// expecting the stream at that version, so two concurrent changes still
/// conflict.
#[derive(Clone, Copy, Debug, Default)]
pub struct SeaOrmPostRepository;

/// A SeaORM failure, as the library's ports report one.
fn backend(error: sea_orm::DbErr) -> StoreError {
    StoreError::backend(error)
}

impl PostRepository for SeaOrmPostRepository {
    type Tx = PgTransaction;

    async fn next_id(&self, tx: &mut PgTransaction) -> Result<PostId, StoreError> {
        let next = Statement::from_string(DbBackend::Postgres, "SELECT nextval('post_ids') AS id");
        let row = SeaOrm::new(tx).query_one_raw(next).await.map_err(backend)?;
        let id: i64 = row
            .ok_or_else(|| StoreError::backend("the post_ids sequence returned nothing"))?
            .try_get("", "id")
            .map_err(backend)?;
        Ok(PostId(u64::try_from(id).map_err(StoreError::backend)?))
    }

    async fn find(&self, id: PostId, tx: &mut PgTransaction) -> Result<Option<Loaded<Post>>, StoreError> {
        let key = i64::try_from(id.0).map_err(StoreError::backend)?;
        let row = post::Entity::find_by_id(key)
            .one(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let version = u64::try_from(row.version).map_err(StoreError::backend)?;
        Ok(Some(Loaded::at(Post::restore(id, row.title, row.published), version)))
    }

    async fn save(&self, post: &Loaded<Post>, tx: &mut PgTransaction) -> Result<(), StoreError> {
        let row = post::ActiveModel {
            id: Set(i64::try_from(post.id().0).map_err(StoreError::backend)?),
            title: Set(post.title().to_owned()),
            published: Set(post.is_published()),
            version: Set(post.version().map_or(0, |version| version as i64)),
        };
        let upsert = OnConflict::column(post::Column::Id)
            .update_columns([post::Column::Title, post::Column::Published, post::Column::Version])
            .to_owned();
        post::Entity::insert(row)
            .on_conflict(upsert)
            .exec(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        Ok(())
    }
}

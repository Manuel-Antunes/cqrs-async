use cqrs::StoreError;
use cqrs_postgres::{PgTransaction, sqlx::PgPool};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, Set,
    SqlxPostgresConnector,
};

use super::{SeaOrm, entities::post_summary};
use crate::{
    application::queries::{PostSummary, PostSummaryRepository, PostSummaryStore},
    domain::PostId,
};

/// The read model with SeaORM: the queries read `post_summaries` from the
/// pool, and the projection writes it in its processor's transaction
/// ([`PostSummaryStore<PgTransaction>`]), so each change commits with the
/// processor's token and an event is applied once.
pub struct SeaOrmPostSummaries {
    /// SeaORM on the pool, for the queries.
    db: DatabaseConnection,
}

impl SeaOrmPostSummaries {
    /// The read model over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self {
            db: SqlxPostgresConnector::from_sqlx_postgres_pool(pool),
        }
    }
}

/// A SeaORM failure, as the library's ports report one.
fn backend(error: sea_orm::DbErr) -> StoreError {
    StoreError::backend(error)
}

/// A row as the queries see it.
fn summary(row: post_summary::Model) -> PostSummary {
    PostSummary {
        id: PostId(row.id as u64),
        title: row.title,
        published: row.published,
    }
}

/// A post id as its column.
fn key(id: PostId) -> Result<i64, StoreError> {
    i64::try_from(id.0).map_err(StoreError::backend)
}

impl PostSummaryRepository for SeaOrmPostSummaries {
    async fn find(&self, id: PostId) -> Result<Option<PostSummary>, StoreError> {
        let row = post_summary::Entity::find_by_id(key(id)?).one(&self.db).await;
        Ok(row.map_err(backend)?.map(summary))
    }

    async fn published(&self) -> Result<Vec<PostSummary>, StoreError> {
        let rows = post_summary::Entity::find()
            .filter(post_summary::Column::Published.eq(true))
            .order_by_asc(post_summary::Column::Id)
            .all(&self.db)
            .await
            .map_err(backend)?;
        Ok(rows.into_iter().map(summary).collect())
    }
}

impl PostSummaryStore<PgTransaction> for SeaOrmPostSummaries {
    async fn save(&self, tx: &mut PgTransaction, summary: PostSummary) -> Result<(), StoreError> {
        let row = post_summary::ActiveModel {
            id: Set(key(summary.id)?),
            title: Set(summary.title),
            published: Set(summary.published),
        };
        post_summary::Entity::insert(row)
            .exec(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn update(
        &self,
        tx: &mut PgTransaction,
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> Result<bool, StoreError> {
        let db = SeaOrm::new(tx);
        let Some(row) = post_summary::Entity::find_by_id(key(id)?)
            .one(&db)
            .await
            .map_err(backend)?
        else {
            return Ok(false);
        };
        let mut changed = summary(row.clone());
        change(&mut changed);
        let mut row = row.into_active_model();
        row.title = Set(changed.title);
        row.published = Set(changed.published);
        row.update(&db).await.map_err(backend)?;
        Ok(true)
    }

    async fn clear(&self, tx: &mut PgTransaction) -> Result<(), StoreError> {
        post_summary::Entity::delete_many()
            .exec(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        Ok(())
    }
}

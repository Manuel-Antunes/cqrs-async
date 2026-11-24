use sea_orm::{ConnectionTrait, DbBackend, Schema};
use sea_orm_migration::{MigrationName, MigrationTrait, MigratorTrait, SchemaManager, async_trait, sea_orm::DbErr};

use super::entities::{post, post_summary};

/// The blog's migrations, run by SeaORM's migrator: the library's tables are
/// one of them, so the blog decides when they run, in its own tool.
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(EventTables), Box::new(BlogTables)]
    }
}

/// The library's tables: `cqrs_postgres::MIGRATIONS`, as they are.
struct EventTables;

impl MigrationName for EventTables {
    fn name(&self) -> &str {
        "m0001_cqrs"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for EventTables {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (_, statements) in cqrs_postgres::MIGRATIONS {
            manager.get_connection().execute_unprepared(statements).await?;
        }
        Ok(())
    }
}

/// The blog's own: the posts' state, their read model, and the sequence
/// post ids come from.
struct BlogTables;

impl MigrationName for BlogTables {
    fn name(&self) -> &str {
        "m0002_blog"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for BlogTables {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let schema = Schema::new(DbBackend::Postgres);
        manager
            .create_table(schema.create_table_from_entity(post::Entity).if_not_exists().to_owned())
            .await?;
        manager
            .create_table(
                schema
                    .create_table_from_entity(post_summary::Entity)
                    .if_not_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared("CREATE SEQUENCE IF NOT EXISTS post_ids")
            .await?;
        Ok(())
    }
}

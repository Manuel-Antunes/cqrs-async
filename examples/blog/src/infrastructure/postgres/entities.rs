/// The `posts` table: each post's state, kept by
/// [`SeaOrmPostRepository`](super::SeaOrmPostRepository), with the version
/// of its stream it is at.
pub mod post {
    use sea_orm::entity::prelude::*;

    /// One post.
    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "posts")]
    pub struct Model {
        /// The post's id.
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        /// Its title.
        pub title: String,
        /// Whether it is published.
        pub published: bool,
        /// The last sequence of its stream this state includes.
        pub version: i64,
    }

    /// No relations.
    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// The `post_summaries` table: the read model the queries answer from,
/// written by the projection in its processor's transaction.
pub mod post_summary {
    use sea_orm::entity::prelude::*;

    /// One summary.
    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "post_summaries")]
    pub struct Model {
        /// The post's id.
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        /// Its current title.
        pub title: String,
        /// Whether it is public.
        pub published: bool,
    }

    /// No relations.
    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

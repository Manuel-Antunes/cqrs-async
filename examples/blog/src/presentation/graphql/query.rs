use std::marker::PhantomData;

use async_graphql::{Context, ID, Object, Result};
use cqrs::QueryGateway;

use super::{
    error::{from_blog, post_id},
    types::PostNode,
};
use crate::{
    application::queries::{FindPost, ListPublished},
    presentation::BlogApi,
};

/// Reads, from the read model (eventually consistent with the commands).
pub struct QueryRoot<A>(PhantomData<fn() -> A>);

impl<A> Default for QueryRoot<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[Object(name = "Query")]
impl<A: BlogApi> QueryRoot<A> {
    /// One post, or `null` if there is none with this id (yet).
    async fn post(&self, ctx: &Context<'_>, id: ID) -> Result<Option<PostNode>> {
        let api = ctx.data_unchecked::<A>();
        let found = api
            .queries()
            .query(FindPost { id: post_id(&id)? })
            .await
            .map_err(from_blog)?;
        Ok(found.map(PostNode::from))
    }

    /// Every published post, by id.
    async fn published_posts(&self, ctx: &Context<'_>) -> Result<Vec<PostNode>> {
        let api = ctx.data_unchecked::<A>();
        let posts = api
            .queries()
            .query(ListPublished)
            .await
            .map_err(from_blog)?;
        Ok(posts.into_iter().map(PostNode::from).collect())
    }
}

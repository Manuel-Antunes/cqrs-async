use std::marker::PhantomData;

use async_graphql::{Context, ID, Object, Result};
use cqrs::{CommandGateway, Metadata};

use super::error::{from_blog, post_id};
use crate::{
    application::commands::{CreatePost, PublishPost, RetitlePost},
    presentation::BlogApi,
};

/// Changes, through the command gateway. Each returns once its events are
/// committed; the read model catches up right after (subscribe to
/// `postEvents` to know when). The request's [`Metadata`] (see
/// [`http`](crate::presentation::http)) goes with every command.
pub struct MutationRoot<A>(PhantomData<fn() -> A>);

impl<A> Default for MutationRoot<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[Object(name = "Mutation")]
impl<A: BlogApi> MutationRoot<A> {
    /// Writes a draft and returns its id.
    async fn create_post(&self, ctx: &Context<'_>, title: String) -> Result<ID> {
        let api = ctx.data_unchecked::<A>();
        let id = api
            .commands()
            .send_with(CreatePost { title }, metadata(ctx))
            .await
            .map_err(from_blog)?;
        Ok(ID(id.to_string()))
    }

    /// Changes a draft's title. Returns the post's id.
    async fn retitle_post(&self, ctx: &Context<'_>, id: ID, title: String) -> Result<ID> {
        let api = ctx.data_unchecked::<A>();
        api.commands()
            .send_with(
                RetitlePost {
                    id: post_id(&id)?,
                    title,
                },
                metadata(ctx),
            )
            .await
            .map_err(from_blog)?;
        Ok(id)
    }

    /// Makes a draft public. Returns the post's id.
    async fn publish_post(&self, ctx: &Context<'_>, id: ID) -> Result<ID> {
        let api = ctx.data_unchecked::<A>();
        api.commands()
            .send_with(PublishPost { id: post_id(&id)? }, metadata(ctx))
            .await
            .map_err(from_blog)?;
        Ok(id)
    }
}

/// The metadata of the request being resolved (none outside HTTP).
fn metadata(ctx: &Context<'_>) -> Metadata {
    ctx.data_opt::<Metadata>().cloned().unwrap_or_default()
}

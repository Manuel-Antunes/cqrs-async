use std::{future::ready, marker::PhantomData};

use async_graphql::{Context, ID, Result, Subscription};
use cqrs::{EventFeed, QuerySubscriptions};
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};

use super::{
    error::{from_blog, from_store, position, post_id},
    types::{PostEventKind, PostEventNode, PostNode},
};
use crate::{
    application::queries::{FindPost, ListPublished},
    domain::PostEvent,
    presentation::BlogApi,
};

/// Pushed updates (`graphql-transport-ws`). Two kinds:
///
/// - **live queries** (`post`, `publishedPosts`): the same shape as the
///   queries, now and on each change. What most clients want.
/// - **the event log** (`postEvents`, `postPublished`): raw committed
///   events, filtered on the server by the field's arguments.
///
/// Where NestJS pipes RxJS operators, each field here returns a `Stream` and
/// filters with `StreamExt`/`TryStreamExt` combinators: arguments become a
/// `try_filter`, applied before anything is converted or sent.
pub struct SubscriptionRoot<A>(PhantomData<fn() -> A>);

impl<A> Default for SubscriptionRoot<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[Subscription(name = "Subscription")]
impl<A: BlogApi> SubscriptionRoot<A> {
    /// The post now (`null` if it does not exist yet), then again each time
    /// it changes. Same shape as `Query.post`.
    async fn post(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> Result<impl Stream<Item = Result<Option<PostNode>>> + use<A>> {
        let query = FindPost { id: post_id(&id)? };
        let updates = ctx.data_unchecked::<A>().queries().subscribe(query);
        Ok(updates.map(|post| post.map(|post| post.map(PostNode::from)).map_err(from_blog)))
    }

    /// The published posts now, then again each time the list changes.
    /// Same shape as `Query.publishedPosts`.
    async fn published_posts(
        &self,
        ctx: &Context<'_>,
    ) -> impl Stream<Item = Result<Vec<PostNode>>> + use<A> {
        let updates = ctx.data_unchecked::<A>().queries().subscribe(ListPublished);
        updates.map(|posts| {
            posts
                .map(|posts| posts.into_iter().map(PostNode::from).collect())
                .map_err(from_blog)
        })
    }

    /// Every post event after `after` (a `position` already seen; omit it
    /// for the whole history), then each new one, filtered on the server:
    /// only `postId`'s events and only these `kinds`, when given. Nothing is
    /// skipped or repeated, so a client that reconnects with its last
    /// position misses nothing.
    async fn post_events(
        &self,
        ctx: &Context<'_>,
        after: Option<ID>,
        post_id: Option<ID>,
        kinds: Option<Vec<PostEventKind>>,
    ) -> Result<impl Stream<Item = Result<PostEventNode>> + use<A>> {
        let after = after.as_ref().map(position).transpose()?.unwrap_or(0);
        // Validated up front: a bad argument fails the subscription at once.
        let stream = post_id.as_ref().map(self::post_id).transpose()?.map(|id| id.to_string());
        let events = ctx.data_unchecked::<A>().events().subscribe(after);
        Ok(events
            .try_filter(move |event| {
                let in_post = stream.as_ref().is_none_or(|id| event.stream.id() == id);
                let of_kind = kinds
                    .as_ref()
                    .is_none_or(|kinds| kinds.contains(&PostEventKind::of(&event.payload)));
                ready(in_post && of_kind)
            })
            .map_ok(|event| PostEventNode::from(&*event))
            .map_err(from_store))
    }

    /// The id of each post published from now on.
    async fn post_published(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<ID>> + use<A>> {
        let events = ctx.data_unchecked::<A>().events();
        let now = events.last_position().await.map_err(from_store)?;
        Ok(events.subscribe(now).filter_map(|event| {
            ready(match event {
                Ok(event) => matches!(event.payload, PostEvent::Published)
                    .then(|| Ok(ID(event.stream.id().to_owned()))),
                Err(error) => Some(Err(from_store(error))),
            })
        }))
    }
}

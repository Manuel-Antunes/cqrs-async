use std::sync::Arc;

use cqrs::{Appender, CommandHandler, EventPublisher, NotFound};

use crate::{
    application::{BlogError, commands::PostRepository},
    domain::{PostEvent, PostId},
};

/// Publishes a draft; from then on the post is frozen.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(error = BlogError)]
pub struct PublishPost {
    /// The post to publish.
    pub id: PostId,
}

/// Handles [`PublishPost`]: loads the post, lets the domain publish it and
/// commits the result; nothing is committed if the domain refuses.
pub struct PublishPostHandler<R, P> {
    /// Where the post is found.
    posts: Arc<R>,
    /// Where the commit goes.
    events: P,
}

impl<R, P> PublishPostHandler<R, P> {
    /// A handler finding posts in `posts` and committing through `events`.
    pub fn new(posts: Arc<R>, events: P) -> Self {
        Self { posts, events }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository, P: EventPublisher<PostEvent>> CommandHandler for PublishPostHandler<R, P> {
    async fn handle(&self, command: &PublishPost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.publish(events)?;
        events.commit(post, &self.events).await?;
        Ok(())
    }
}

use std::sync::Arc;

use cqrs::{Appender, CommandHandler, NotFound};

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
pub struct PublishPostHandler<R> {
    /// Where the post is found.
    posts: Arc<R>,
}

impl<R> PublishPostHandler<R> {
    /// A handler finding posts in `posts`.
    pub fn new(posts: Arc<R>) -> Self {
        Self { posts }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository> CommandHandler for PublishPostHandler<R> {
    async fn handle(&self, command: &PublishPost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.publish(events)?;
        events.commit(post).await?;
        Ok(())
    }
}

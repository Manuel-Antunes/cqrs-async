use std::sync::Arc;

use cqrs::{Appender, CommandHandler, NotFound};

use crate::{
    application::{BlogError, commands::PostRepository},
    domain::{PostEvent, PostId},
};

/// Changes the title of a draft.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(error = BlogError)]
pub struct RetitlePost {
    /// The post to rename.
    pub id: PostId,
    /// The new title; trimmed, and refused if blank or unchanged.
    pub title: String,
}

/// Handles [`RetitlePost`]: loads the post, lets the domain rename it and
/// commits the result; nothing is committed if the domain refuses.
pub struct RetitlePostHandler<R> {
    /// Where the post is found.
    posts: Arc<R>,
}

impl<R> RetitlePostHandler<R> {
    /// A handler finding posts in `posts`.
    pub fn new(posts: Arc<R>) -> Self {
        Self { posts }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository> CommandHandler for RetitlePostHandler<R> {
    async fn handle(&self, command: &RetitlePost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), events)?;
        events.commit(post).await?;
        Ok(())
    }
}

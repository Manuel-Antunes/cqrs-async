use std::sync::Arc;

use cqrs::{Appender, CommandHandler, NotFound};

use crate::{
    application::{BlogError, commands::PostRepository},
    domain::{PostEvent, PostId},
};

/// Changes the title of a draft.
#[cqrs::command(error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RetitlePost {
    /// The post to rename.
    pub id: PostId,
    /// The new title; trimmed, and refused if blank or unchanged.
    pub title: String,
}

/// Handles [`RetitlePost`]: loads the post, lets the domain rename it and
/// commits the result; nothing is committed if the domain refuses.
pub struct RetitlePostHandler<R> {
    /// Where posts are found and kept.
    posts: Arc<R>,
}

impl<R> RetitlePostHandler<R> {
    /// A handler over `posts`.
    pub fn new(posts: Arc<R>) -> Self {
        Self { posts }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository> CommandHandler for RetitlePostHandler<R> {
    async fn handle(&self, command: &RetitlePost, appender: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), appender)?;
        self.posts.save(post, appender).await?;
        Ok(())
    }
}

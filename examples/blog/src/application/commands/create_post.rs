use std::sync::Arc;

use cqrs::{Appender, CommandHandler, New};

use crate::{
    application::{BlogError, commands::PostRepository},
    domain::{Post, PostEvent, PostId},
};

/// Creates a draft post and returns its id.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(output = PostId, error = BlogError)]
pub struct CreatePost {
    /// The title; trimmed, and refused if blank.
    pub title: String,
}

/// Handles [`CreatePost`]: takes the next id from the repository, lets the
/// domain create the post and saves it, expecting it not to exist yet.
pub struct CreatePostHandler<R> {
    /// Where posts are kept.
    posts: Arc<R>,
}

impl<R> CreatePostHandler<R> {
    /// A handler saving to `posts`.
    pub fn new(posts: Arc<R>) -> Self {
        Self { posts }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository> CommandHandler for CreatePostHandler<R> {
    async fn handle(&self, command: &CreatePost, appender: &mut Appender<PostEvent>) -> Result<PostId, BlogError> {
        let id = self.posts.next_id().await?;
        Post::create(id, command.title.as_str(), appender)?;
        self.posts.save(New::<Post>(id).into(), appender).await?;
        Ok(id)
    }
}

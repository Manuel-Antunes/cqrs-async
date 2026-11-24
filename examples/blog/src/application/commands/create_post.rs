use std::sync::Arc;

use cqrs::{Appender, CommandHandler, New};

use crate::{
    application::{BlogError, commands::PostRepository},
    domain::{Post, PostEvent, PostId},
};

/// Creates a draft post and returns its id.
#[cqrs::command(output = PostId, error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CreatePost {
    /// The title; trimmed, and refused if blank.
    pub title: String,
}

/// Handles [`CreatePost`]: takes the next id from the repository, lets the
/// domain create the post, records it (expecting it not to exist yet) and
/// saves it.
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
    async fn handle(
        &self,
        command: &CreatePost,
        events: &mut Appender<PostEvent>,
        tx: &mut R::Tx,
    ) -> Result<PostId, BlogError> {
        let id = self.posts.next_id(tx).await?;
        Post::create(id, command.title.as_str(), events)?;
        let post = events.record(New::<Post>(id));
        self.posts.save(&post, tx).await?;
        Ok(id)
    }
}

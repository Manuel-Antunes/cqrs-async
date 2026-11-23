use std::sync::atomic::{AtomicU64, Ordering};

use cqrs::{Aggregate, Appender, CommandHandler, EventPublisher, Loaded};

use crate::{
    application::BlogError,
    domain::{Post, PostEvent, PostId},
};

/// Creates a draft post and returns its id.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(output = PostId, error = BlogError)]
pub struct CreatePost {
    /// The title; trimmed, and refused if blank.
    pub title: String,
}

/// Handles [`CreatePost`]: picks the next id, lets the domain create the post
/// and commits it, expecting it not to exist yet.
pub struct CreatePostHandler<P> {
    /// Ids are sequential per process: enough for the example, as a
    /// collision would surface as a [`BlogError::Conflict`], never as an
    /// overwrite.
    next_id: AtomicU64,
    /// Where the commit goes.
    events: P,
}

impl<P> CreatePostHandler<P> {
    /// A handler whose ids start at 1, committing through `events`.
    pub fn new(events: P) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            events,
        }
    }
}

#[cqrs::command_handler]
impl<P: EventPublisher<PostEvent>> CommandHandler for CreatePostHandler<P> {
    async fn handle(&self, command: &CreatePost, events: &mut Appender<PostEvent>) -> Result<PostId, BlogError> {
        let id = PostId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let post = Loaded::new(Post::initial(&id));
        post.create(command.title.as_str(), events)?;
        events.commit(post, &self.events).await?;
        Ok(id)
    }
}

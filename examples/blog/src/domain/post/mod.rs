//! The `Post` aggregate: a blog post that is drafted, possibly retitled, and
//! then published, after which it can no longer change.

/// Why the post refused an operation.
mod error;
/// What can happen to a post.
mod event;
/// The post's identity.
mod id;

use cqrs::EventAppender;
use serde::{Deserialize, Serialize};

pub use error::PostError;
pub use event::PostEvent;
pub use id::PostId;

/// A blog post: plain state. It is only ever changed by its events (see
/// its private `apply`); the public methods are decisions: they check the
/// business rules and append the events to the appender they are given
/// (Axon 5's `EventAppender`). [`create`](Self::create) has no post to decide
/// on yet, so it is an associated function; the others take `&self`. The
/// struct holds no buffer, so derives, literals and serialization work as for
/// any struct.
///
/// The derive writes the [`Aggregate`](cqrs::Aggregate) impl: stream `post`,
/// id from the `id` field, initial state with the other fields at their
/// default, and `evolve` calling `apply`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::AggregateRoot)]
#[aggregate(name = "post", event = PostEvent)]
pub struct Post {
    /// Which post this is.
    id: PostId,
    /// Current title, already trimmed and never empty once created.
    title: String,
    /// Whether the post is public. Once `true`, the post is frozen.
    published: bool,
}

impl Post {
    /// How an event changes the post, both when it is recorded and when it
    /// is replayed: no validation, no failure.
    fn apply(&mut self, event: &PostEvent) {
        match event {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => {
                self.title.clone_from(title);
            }
            PostEvent::Published => self.published = true,
        }
    }

    /// Drafts the post `id`, unpublished. The title is trimmed. There is no
    /// post yet, so this takes none: commit it to `cqrs::New`, which expects
    /// the post's stream not to exist.
    ///
    /// # Errors
    ///
    /// [`PostError::EmptyTitle`] if the title is blank.
    pub fn create(
        id: PostId,
        title: impl Into<String>,
        events: &mut impl EventAppender<PostEvent>,
    ) -> Result<(), PostError> {
        let title = valid_title(title.into())?;
        events.append(PostEvent::Created { id, title });
        Ok(())
    }

    /// Changes the title of a draft. The title is trimmed.
    ///
    /// # Errors
    ///
    /// - [`PostError::PublishedPostsAreFrozen`] if the post is published;
    /// - [`PostError::EmptyTitle`] if the title is blank;
    /// - [`PostError::SameTitle`] if nothing would change (so no empty event
    ///   is recorded).
    pub fn retitle(
        &self,
        title: impl Into<String>,
        events: &mut impl EventAppender<PostEvent>,
    ) -> Result<(), PostError> {
        if self.published {
            return Err(PostError::PublishedPostsAreFrozen);
        }
        let title = valid_title(title.into())?;
        if title == self.title {
            return Err(PostError::SameTitle);
        }
        events.append(PostEvent::Retitled { title });
        Ok(())
    }

    /// Makes the post public. From then on it cannot change.
    ///
    /// # Errors
    ///
    /// [`PostError::AlreadyPublished`] if it already is.
    pub fn publish(&self, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
        if self.published {
            return Err(PostError::AlreadyPublished);
        }
        events.append(PostEvent::Published);
        Ok(())
    }

    /// The current title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Whether the post is public (and therefore frozen).
    #[must_use]
    pub fn is_published(&self) -> bool {
        self.published
    }
}

/// The title as stored: trimmed, and refused if nothing is left.
fn valid_title(title: String) -> Result<String, PostError> {
    let title = title.trim();
    if title.is_empty() {
        Err(PostError::EmptyTitle)
    } else {
        Ok(title.to_owned())
    }
}

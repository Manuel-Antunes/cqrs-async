use std::fmt;

/// A business rule the post refused to break. Nothing is recorded when an
/// operation returns one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostError {
    /// The title is empty or only whitespace.
    EmptyTitle,
    /// Retitling to the current title, which would record a no-op event.
    SameTitle,
    /// Publishing a post that is already public.
    AlreadyPublished,
    /// Changing a published post.
    PublishedPostsAreFrozen,
}

impl fmt::Display for PostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyTitle => "a post needs a title",
            Self::SameTitle => "the post already has this title",
            Self::AlreadyPublished => "the post is already published",
            Self::PublishedPostsAreFrozen => "a published post cannot change",
        })
    }
}

impl std::error::Error for PostError {}

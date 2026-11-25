use std::fmt;

use cqrs::{aggregate::NotFound, store::StoreError};

use crate::domain::{PostError, PostId};

/// Why a use case failed, classified by origin, so an HTTP adapter maps it
/// without parsing strings.
#[derive(Debug, PartialEq)]
pub enum BlogError {
    /// The domain refused (a 4xx).
    Domain(PostError),
    /// No post has this id (a 404).
    NotFound(PostId),
    /// Someone else changed the post in between: retry.
    Conflict,
    /// Storage failed (a 5xx).
    Store(String),
}

impl From<PostError> for BlogError {
    fn from(error: PostError) -> Self {
        Self::Domain(error)
    }
}

impl From<NotFound<PostId>> for BlogError {
    fn from(NotFound(id): NotFound<PostId>) -> Self {
        Self::NotFound(id)
    }
}

impl From<StoreError> for BlogError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } | StoreError::ConditionFailed => Self::Conflict,
            // `StoreError` may gain variants: anything else is the store's.
            other => Self::Store(other.to_string()),
        }
    }
}

impl fmt::Display for BlogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(error) => error.fmt(f),
            Self::NotFound(id) => write!(f, "post {id} not found"),
            Self::Conflict => f.write_str("the post changed concurrently, retry"),
            Self::Store(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for BlogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            _ => None,
        }
    }
}

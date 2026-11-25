//! Application errors as GraphQL errors, with a machine-readable
//! `extensions.code` so clients branch on the code, never on the message.

use async_graphql::{Error, ErrorExtensions, ID};
use cqrs::store::StoreError;

use crate::{application::BlogError, domain::PostId};

/// `BlogError` → GraphQL error. Internal failures keep their details in the
/// logs, not in the response.
pub(super) fn from_blog(error: BlogError) -> Error {
    let code = match &error {
        BlogError::Domain(_) => "DOMAIN_RULE",
        BlogError::NotFound(_) => "NOT_FOUND",
        BlogError::Conflict => "CONFLICT",
        BlogError::Store(_) => return internal(&error),
    };
    Error::new(error.to_string()).extend_with(|_, extensions| extensions.set("code", code))
}

/// A failure reading the events, during a subscription.
pub(super) fn from_store(error: StoreError) -> Error {
    internal(&error)
}

/// Logs `error` and answers with a generic internal error.
fn internal(error: &impl std::fmt::Display) -> Error {
    tracing::error!(%error, "request failed");
    Error::new("internal error").extend_with(|_, extensions| extensions.set("code", "INTERNAL"))
}

/// Parses a post id argument.
pub(super) fn post_id(id: &ID) -> Result<PostId, Error> {
    id.parse::<u64>().map(PostId).map_err(|_| {
        Error::new(format!("`{}` is not a post id", id.as_str()))
            .extend_with(|_, extensions| extensions.set("code", "BAD_USER_INPUT"))
    })
}

/// Parses a log position argument.
pub(super) fn position(position: &ID) -> Result<u64, Error> {
    position.parse::<u64>().map_err(|_| {
        Error::new(format!("`{}` is not an event position", position.as_str()))
            .extend_with(|_, extensions| extensions.set("code", "BAD_USER_INPUT"))
    })
}

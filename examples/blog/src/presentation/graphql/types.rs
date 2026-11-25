//! GraphQL output types: the API's own shapes, converted from the
//! application's, so renaming a domain field never breaks clients.

use async_graphql::{Enum, ID, SimpleObject};
use cqrs::event::EventEnvelope;

use crate::{application::queries::PostSummary, domain::PostEvent};

/// A post as the read model shows it.
#[derive(SimpleObject)]
#[graphql(name = "Post")]
pub struct PostNode {
    /// The post's id.
    pub id: ID,
    /// Its current title.
    pub title: String,
    /// Whether it is published.
    pub published: bool,
}

impl From<PostSummary> for PostNode {
    fn from(summary: PostSummary) -> Self {
        Self {
            id: ID(summary.id.to_string()),
            title: summary.title,
            published: summary.published,
        }
    }
}

/// What happened to a post.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostEventKind {
    /// The post was written.
    Created,
    /// Its title changed.
    Retitled,
    /// It went public.
    Published,
}

/// A committed post event.
#[derive(SimpleObject)]
#[graphql(name = "PostEvent")]
pub struct PostEventNode {
    /// Position in the whole event log: resume a subscription after it with
    /// `postEvents(after: position)`. A string, as it can exceed GraphQL's
    /// 32-bit `Int`.
    pub position: ID,
    /// The post it happened to.
    pub post_id: ID,
    /// What happened.
    pub kind: PostEventKind,
    /// The new title, for `CREATED` and `RETITLED`.
    pub title: Option<String>,
}

impl PostEventKind {
    /// The kind of `event`.
    pub fn of(event: &PostEvent) -> Self {
        match event {
            PostEvent::Created { .. } => Self::Created,
            PostEvent::Retitled { .. } => Self::Retitled,
            PostEvent::Published => Self::Published,
        }
    }
}

impl From<&EventEnvelope<PostEvent>> for PostEventNode {
    fn from(event: &EventEnvelope<PostEvent>) -> Self {
        let title = match &event.payload {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => Some(title.clone()),
            PostEvent::Published => None,
        };
        Self {
            position: ID(event.position.to_string()),
            post_id: ID(event.stream.id().to_owned()),
            kind: PostEventKind::of(&event.payload),
            title,
        }
    }
}

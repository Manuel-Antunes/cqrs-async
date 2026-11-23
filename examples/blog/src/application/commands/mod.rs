//! Commands: messages that change state. One file per use case, each with
//! its message and its handler.

/// Drafting a new post.
mod create_post;
/// Where the handlers find posts (a port).
mod post_repository;
/// Making a draft public.
mod publish_post;
/// Renaming a draft.
mod retitle_post;

use std::sync::Arc;

pub use create_post::{CreatePost, CreatePostHandler};
pub use post_repository::PostRepository;
pub use publish_post::{PublishPost, PublishPostHandler};
pub use retitle_post::{RetitlePost, RetitlePostHandler};

use crate::application::BlogError;

/// Every command of the write side, as one message: what a transport (a
/// queue, a JSON request) deserializes and sends. Each command converts
/// into it with `From`. Also generates [`PostCommands`], the port of
/// whatever routes them all.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Commands)]
#[commands(error = BlogError, port = PostCommands, derive(Debug, PartialEq))]
pub enum PostCommand {
    /// See [`CreatePost`].
    CreatePost(CreatePost),
    /// See [`RetitlePost`].
    RetitlePost(RetitlePost),
    /// See [`PublishPost`].
    PublishPost(PublishPost),
}

/// Write side: routes each command to the field whose handler declares it, at
/// compile time; no registry, no `dyn`, and no message named here. It also
/// takes a [`PostCommand`], each variant going to its handler; a command of
/// the enum that no field handles is a compile error.
#[derive(cqrs::Gateway)]
#[gateway(commands = PostCommand)]
pub struct PostCommandGateway<R, P> {
    /// Handles [`CreatePost`].
    create: CreatePostHandler<P>,
    /// Handles [`RetitlePost`].
    retitle: RetitlePostHandler<R, P>,
    /// Handles [`PublishPost`].
    publish: PublishPostHandler<R, P>,
}

impl<R, P: Clone> PostCommandGateway<R, P> {
    /// Builds every command handler over the posts' repository and the
    /// publisher their commits go through (shared: `P` is cloned into each
    /// handler, so an `Arc` or a cheap handle).
    pub fn new(posts: Arc<R>, events: P) -> Self {
        Self {
            create: CreatePostHandler::new(events.clone()),
            retitle: RetitlePostHandler::new(Arc::clone(&posts), events.clone()),
            publish: PublishPostHandler::new(posts, events),
        }
    }
}

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
    create: CreatePostHandler<R>,
    /// Handles [`RetitlePost`].
    retitle: RetitlePostHandler<R>,
    /// Handles [`PublishPost`].
    publish: PublishPostHandler<R>,
    /// The unit of work every command runs in: it opens the transaction
    /// the handlers and the repository write in, appends what they
    /// recorded, and commits.
    #[gateway(unit)]
    events: P,
}

impl<R, P> PostCommandGateway<R, P> {
    /// The gateway, its handlers sharing the posts' repository, its
    /// commands running in units of work of `events`.
    pub fn new(posts: Arc<R>, events: P) -> Self {
        Self {
            create: CreatePostHandler::new(Arc::clone(&posts)),
            retitle: RetitlePostHandler::new(Arc::clone(&posts)),
            publish: PublishPostHandler::new(posts),
            events,
        }
    }
}

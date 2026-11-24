//! End-to-end example for the `cqrs` PoC, in clean architecture layers.
//!
//! Dependencies only point inwards:
//!
//! - [`domain`]: the `Post` aggregate.
//! - [`application`]: commands, queries and projections, written against ports.
//! - [`infrastructure`]: adapters for those ports.
//! - [`presentation`]: the GraphQL API (axum), written against ports too.
//! - [`config`]: the composition root, the only place that names concrete
//!   adapters, wiring the blog's [`Cqrs`](cqrs::Cqrs) unit two ways:
//!   - [`in_memory`]: everything synchronous, projections run inside each
//!     commit. No runtime needed; what the domain and flow tests use.
//!   - [`start`]: the asynchronous one the server runs. Commands return once
//!     committed, projections follow on their own Tokio tasks
//!     ([`TrackingProcessor`](cqrs::TrackingProcessor)), and
//!     subscriptions stream the committed events.
//!
//! The doctests below are the negative half of the study: each one must
//! **fail to compile**, proving a guarantee is enforced by the type system.
//!
//! A decision changing state directly instead of appending an event (it
//! gets `&self`, so only `evolve` can change state):
//!
//! ```compile_fail,E0594
//! use blog::domain::{PostError, PostEvent};
//! use cqrs::EventAppender;
//!
//! struct Draft { published: bool }
//!
//! impl Draft {
//!     fn publish(&self, _: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
//!         self.published = true; // not an event: refused
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Changing an aggregate loaded from the repository behind its back (the
//! loaded aggregate is read-only; only saved events change it):
//!
//! ```compile_fail,E0596
//! # async fn doc(posts: &impl blog::application::commands::PostRepository) {
//! use blog::domain::PostId;
//! let mut post = posts.find(PostId(1)).await.unwrap().unwrap();
//! let state: &mut blog::domain::Post = &mut post;
//! # }
//! ```
//!
//! Sending a command no field of the gateway handles:
//!
//! ```compile_fail,E0277
//! use blog::application::BlogError;
//! use cqrs::CommandGateway;
//!
//! #[derive(cqrs::Command)]
//! #[command(error = BlogError)]
//! struct DeletePost;
//!
//! let (blog, _, _) = blog::in_memory();
//! let _ = blog.commands().send(DeletePost);
//! ```
//!
//! Sending a query as if it were a command:
//!
//! ```compile_fail,E0277
//! use blog::{application::queries::FindPost, domain::PostId};
//! use cqrs::CommandGateway;
//!
//! let (blog, _, _) = blog::in_memory();
//! let _ = blog.commands().send(FindPost { id: PostId(1) });
//! ```
//!
//! The gateway checks below define their handlers in the doctest itself: a
//! derived gateway must live in the crate of its handlers (Rust's coherence
//! check cannot see through another crate's `Handler::Message`).
//!
//! Two handlers for the same command in one gateway:
//!
//! ```compile_fail,E0119
//! use blog::{application::{BlogError, commands::CreatePost}, domain::PostId};
//!
//! struct Creates;
//! impl cqrs::Handler for Creates {
//!     type Message = CreatePost;
//!     async fn handle(&self, _: CreatePost) -> Result<PostId, BlogError> { Ok(PostId(1)) }
//! }
//! struct AlsoCreates;
//! impl cqrs::Handler for AlsoCreates {
//!     type Message = CreatePost;
//!     async fn handle(&self, _: CreatePost) -> Result<PostId, BlogError> { Ok(PostId(2)) }
//! }
//!
//! #[derive(cqrs::Gateway)]
//! struct Twice { create: Creates, again: AlsoCreates }
//! ```
//!
//! A gateway taking an enum of commands one of which it does not handle
//! (rejected where the gateway is defined):
//!
//! ```compile_fail,E0277
//! use blog::{application::{BlogError, commands::{CreatePost, PublishPost}}, domain::PostId};
//!
//! struct Creates;
//! impl cqrs::Handler for Creates {
//!     type Message = CreatePost;
//!     async fn handle(&self, _: CreatePost) -> Result<PostId, BlogError> { Ok(PostId(1)) }
//! }
//!
//! #[derive(cqrs::Commands)]
//! #[commands(error = BlogError)]
//! enum Writes { CreatePost(CreatePost), PublishPost(PublishPost) }
//!
//! #[derive(cqrs::Gateway)]
//! #[gateway(commands = Writes)]
//! struct OnlyCreates { create: Creates }
//! ```
//!
//! A field that is not a handler, without `#[gateway(skip)]`:
//!
//! ```compile_fail,E0277
//! #[derive(cqrs::Gateway)]
//! struct Broken {
//!     find: blog::application::queries::FindPostHandler<blog::infrastructure::InMemoryPostSummaryRepository>,
//!     label: String,
//! }
//! ```
//!

#![warn(missing_docs)]

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod presentation;

pub use config::{PgPostEvents, PostEvents, PostStore, Running, in_memory, in_memory_with, start, start_in_memory};

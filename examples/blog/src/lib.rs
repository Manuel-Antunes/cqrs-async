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
//!     ([`TrackingProcessor`](cqrs::processing::TrackingProcessor)), and
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
//! use cqrs::aggregate::EventAppender;
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
//! # async fn doc(posts: &impl cqrs::aggregate::Repository<blog::domain::Post, Tx = ()>) {
//! use blog::domain::PostId;
//! let mut post = posts.find(&PostId(1), &mut ()).await.unwrap().unwrap();
//! let state: &mut blog::domain::Post = &mut post;
//! # }
//! ```
//!
//! Sending a command no field of the gateway handles:
//!
//! ```compile_fail,E0277
//! use blog::application::BlogError;
//! use cqrs::command::CommandGateway;
//!
//! #[cqrs::command(error = BlogError)]
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
//! use cqrs::command::CommandGateway;
//!
//! let (blog, _, _) = blog::in_memory();
//! let _ = blog.commands().send(FindPost { id: PostId(1) });
//! ```
//!
//! A gateway listing a command it has no handler for (rejected where the
//! gateway is declared):
//!
//! ```compile_fail,E0277
//! use blog::{application::{BlogError, commands::{CreatePost, PublishPost}}, domain::{PostEvent, PostId}};
//! use cqrs::command::{CommandContext, CommandHandler};
//!
//! #[cqrs::command_gateway(handles(CreatePost, PublishPost))]
//! struct OnlyCreates<P> {
//!     #[gateway(unit)]
//!     events: P,
//! }
//!
//! #[cqrs::command_handler]
//! impl<P> CommandHandler<CreatePost> for OnlyCreates<P> {
//!     async fn handle(&self, _: &CreatePost, _: &mut CommandContext<PostEvent>) -> Result<PostId, BlogError> { Ok(PostId(1)) }
//! }
//! ```
//!
//! A query listed by a command gateway:
//!
//! ```compile_fail,E0277
//! use blog::application::queries::FindPost;
//!
//! #[cqrs::command_gateway(handles(FindPost))]
//! struct Confused<P> {
//!     #[gateway(unit)]
//!     events: P,
//! }
//! # impl<P> cqrs::Handles<FindPost> for Confused<P> {}
//! ```
//!
//! The same command listed twice:
//!
//! ```compile_fail
//! use blog::application::commands::CreatePost;
//!
//! #[cqrs::command_gateway(handles(CreatePost, CreatePost))]
//! struct Twice<P> {
//!     #[gateway(unit)]
//!     events: P,
//! }
//! ```

#![warn(missing_docs)]

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod presentation;

pub use config::{PgPostEvents, PostEvents, PostStore, Running, in_memory, in_memory_with, start, start_in_memory};

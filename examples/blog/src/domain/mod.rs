//! Domain layer: the business rules, and nothing else.
//!
//! No store, no bus, no runtime. The only framework items are the
//! [`Aggregate`](cqrs::Aggregate) contract (how events change the state) and
//! the [`EventAppender`](cqrs::EventAppender) the decisions append to; a
//! domain can even take `impl Extend<Event>` from std instead. Persistence
//! is the framework's: command handlers get the post loaded and their
//! appended events committed.

pub mod post;

pub use post::{Post, PostError, PostEvent, PostId};

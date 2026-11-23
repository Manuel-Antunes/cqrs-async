//! The GraphQL schema.
//!
//! Each root is generic over the [`BlogApi`] it serves, which the schema
//! carries as context data: resolvers reach it with
//! `ctx.data_unchecked::<A>()`, typed, without `dyn`.
//!
//! Consistency, as seen by clients:
//!
//! - a mutation returns once its events are **committed**;
//! - queries read a projection updated **asynchronously** right after, so a
//!   query just after a mutation may not see it yet;
//! - subscriptions see every committed event, in order: subscribe to know
//!   when a change landed instead of polling.

mod error;
mod mutation;
mod query;
mod subscription;
mod types;

use async_graphql::Schema;

pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use subscription::SubscriptionRoot;
pub use types::{PostEventKind, PostEventNode, PostNode};

use super::BlogApi;

/// The schema serving `A`.
pub type BlogSchema<A> = Schema<QueryRoot<A>, MutationRoot<A>, SubscriptionRoot<A>>;

/// Builds the schema around `api`.
pub fn schema<A: BlogApi>(api: A) -> BlogSchema<A> {
    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .data(api)
    .finish()
}

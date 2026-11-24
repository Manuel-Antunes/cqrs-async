//! Application layer: the use cases, written against ports only.
//!
//! - [`commands`] are command handlers: each gets an appender from the
//!   gateway, finds the post in the application's
//!   [`PostRepository`](commands::PostRepository) port, lets the domain append
//!   and saves it through the same port; routed by
//!   [`PostCommandGateway`](commands::PostCommandGateway).
//! - [`queries`] only read, from the [`PostSummaryRepository`](queries::PostSummaryRepository)
//!   read-model port, and are routed by
//!   [`PostQueryGateway`](queries::PostQueryGateway). They never touch the
//!   event store.
//! - [`projections`] handle committed events: one keeps the read model up
//!   to date and notifies, one namespace each
//!   ([`PostProjection`](projections::PostProjection)).
//!
//! Every handler is one struct per message, generic over the ports it needs;
//! the concrete adapters are chosen by the composition root
//! ([`config`](crate::config)), which wires them into a
//! [`Cqrs`](cqrs::Cqrs) unit.

pub mod commands;
/// The error every use case returns.
mod error;
pub mod projections;
pub mod queries;

pub use error::BlogError;

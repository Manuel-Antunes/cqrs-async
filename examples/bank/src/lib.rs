//! A bank and a compliance service, two applications talking through
//! RabbitMQ.
//!
//! - [`banking`]: customers and accounts, each aggregate root on its own
//!   log, behind a REST API ([`banking::api`]). A portfolio projection
//!   reads both logs as one (a fan-in), in the order they were committed.
//!   The accounts' log is forwarded to RabbitMQ by a tracking processor:
//!   the log is the outbox.
//! - [`alerts`]: another service, with its own log, gateway and REST API
//!   ([`alerts::api`]). It consumes the bank's account events from its
//!   queue and raises an alert for each large withdrawal, once, however
//!   many times the message is delivered, keeping the correlation id of
//!   the bank request that caused it.
//!
//! ```text
//! docker run -d --name rabbit -p 5672:5672 rabbitmq:4-alpine
//! cargo run -p bank        # the bank on :8080, compliance on :8081
//! ```
//!
//! The binary's documentation (`src/main.rs`) has a session with `curl`.

pub mod alerts;
pub mod banking;

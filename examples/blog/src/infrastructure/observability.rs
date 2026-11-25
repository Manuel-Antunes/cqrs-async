//! Cross-cutting concerns as interceptors, kept out of the handlers:
//!
//! - [`Audit`] around the command gateway: every command, who asked
//!   (its correlation id) and how it ended, in an [`AuditLog`];
//! - [`Measured`] around the projection: how many events it applied and
//!   how long they took, in [`ProjectionMetrics`].

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use cqrs::{
    Interceptor, Message,
    event::{DomainEvent, EventEnvelope, Metadata},
    processing::{EventChain, EventInterceptor, HandlerError, ProcessingContext},
};

/// One audited command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    /// The command's type (`CreatePost`).
    pub command: &'static str,
    /// The `correlation_id` it was sent with, if any.
    pub correlation_id: Option<String>,
    /// Whether it succeeded.
    pub succeeded: bool,
}

/// Where audited commands are kept (in memory: an adapter would write them
/// somewhere durable).
#[derive(Debug, Default)]
pub struct AuditLog(Mutex<Vec<AuditEntry>>);

impl AuditLog {
    /// Every entry so far, oldest first.
    pub fn entries(&self) -> Vec<AuditEntry> {
        self.0.lock().expect("audit log poisoned").clone()
    }
}

/// Records every command the gateway handles, after it is handled.
#[derive(Clone)]
pub struct Audit(pub Arc<AuditLog>);

impl<M: Message> Interceptor<M> for Audit {
    async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send,
    {
        let correlation_id = metadata.get("correlation_id").cloned();
        let result = next(message, metadata).await;
        let command = std::any::type_name::<M>().rsplit("::").next().unwrap_or_default();
        let entry = AuditEntry {
            command,
            correlation_id,
            succeeded: result.is_ok(),
        };
        self.0.0.lock().expect("audit log poisoned").push(entry);
        result
    }
}

/// What [`Measured`] counted.
#[derive(Debug, Default)]
pub struct ProjectionMetrics {
    /// Events handled.
    handled: AtomicU64,
    /// Time spent handling them, in microseconds.
    busy_us: AtomicU64,
}

impl ProjectionMetrics {
    /// Events handled so far.
    pub fn handled(&self) -> u64 {
        self.handled.load(Ordering::Relaxed)
    }

    /// Time spent handling them.
    pub fn busy(&self) -> Duration {
        Duration::from_micros(self.busy_us.load(Ordering::Relaxed))
    }
}

/// Counts and times every event handled, and traces the ones that fail.
#[derive(Clone)]
pub struct Measured(pub Arc<ProjectionMetrics>);

impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Measured {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        let started = Instant::now();
        let result = next.proceed(context).await;
        let busy = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.0.busy_us.fetch_add(busy, Ordering::Relaxed);
        match &result {
            Ok(()) => {
                self.0.handled.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => tracing::warn!(position = event.position, %error, "projection failed"),
        }
        result
    }
}

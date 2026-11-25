//! Commits relayed between instances, with PostgreSQL's `LISTEN`/`NOTIFY`.
//!
//! An instance's live feed carries only its own commits: the others' reach
//! its subscriptions and processors at their next poll
//! ([`SubscriptionOptions::poll_interval`](cqrs::store::SubscriptionOptions),
//! 1 s by default). [`Postgres::listen`](crate::Postgres::listen) closes that
//! gap: each instance tells the others when it committed, and wakes its own
//! readers when another did.
//!
//! The notification is sent after the commit, from a connection of its
//! own, never inside the append's transaction: a transaction that notifies
//! takes a lock at commit that every other notifying transaction waits for,
//! which would serialize the appends. Commits close together share one
//! notification. A lost notification costs only latency: the poll is still
//! there, and the store stays the source of truth.

use std::time::Duration;

use cqrs::{event::DomainEvent, store::StoreError};
use sqlx::postgres::PgListener;
use tokio::{
    sync::broadcast::error::{RecvError, TryRecvError},
    task::JoinHandle,
};

use crate::{PgEvents, Postgres, backend};

/// The channel every log's notifications go through (the payload says
/// which log, and which instance).
pub const CHANNEL: &str = "cqrs_events";

/// How long commits are gathered into one notification.
const GATHER: Duration = Duration::from_millis(5);

/// Wait before listening again after a failure.
const RETRY: Duration = Duration::from_millis(500);

/// Relays a log's commits between instances, until stopped or dropped.
#[must_use = "dropping it stops relaying the commits"]
pub struct Listening {
    /// The task telling the others, and the one listening to them.
    tasks: [JoinHandle<()>; 2],
}

impl Listening {
    /// Stops relaying.
    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl ::std::fmt::Debug for Listening {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("Listening").finish_non_exhaustive()
    }
}

impl Postgres {
    /// Relays the commits to `events`' log between the instances of the
    /// application: tells the others (`NOTIFY`) once this one committed,
    /// and wakes `events`' subscriptions and processors
    /// ([`Events::committed_elsewhere`](cqrs::processing::Events::committed_elsewhere))
    /// when another did (`LISTEN`). Every instance calls it, once per log;
    /// a single instance has no need of it.
    ///
    /// # Errors
    ///
    /// If the listening connection cannot be opened.
    pub async fn listen<E, U>(&self, events: &PgEvents<E, U>) -> Result<Listening, StoreError>
    where
        E: DomainEvent,
        U: Send + Sync + 'static,
    {
        let log: std::sync::Arc<str> = events.store().log().into();
        let instance = instance_id();
        let mut listener = PgListener::connect_with(self.pool()).await.map_err(backend)?;
        listener.listen(CHANNEL).await.map_err(backend)?;

        let telling = {
            let mut committed = events.live().subscribe();
            let pool = self.pool().clone();
            let payload = format!("{log}\n{instance}");
            tokio::spawn(async move {
                loop {
                    match committed.recv().await {
                        Ok(_) | Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => return,
                    }
                    // The commits close together go in one notification.
                    tokio::time::sleep(GATHER).await;
                    while !matches!(committed.try_recv(), Err(TryRecvError::Empty | TryRecvError::Closed)) {}
                    let sent = sqlx::query!("SELECT pg_notify($1, $2)", CHANNEL, payload)
                        .execute(&pool)
                        .await;
                    if let Err(error) = sent {
                        tracing::warn!(%error, "cannot tell the other instances about a commit");
                    }
                }
            })
        };

        let listening = {
            let events = events.clone();
            tokio::spawn(async move {
                loop {
                    match listener.try_recv().await {
                        Ok(Some(notification)) => {
                            let mut from = notification.payload().split('\n');
                            if from.next() == Some(&*log) && from.next() != Some(&*instance) {
                                events.committed_elsewhere();
                            }
                        }
                        // The connection dropped (it reconnects on the next
                        // call): a notification may have been lost meanwhile.
                        Ok(None) => events.committed_elsewhere(),
                        Err(error) => {
                            tracing::warn!(%error, "cannot listen to the other instances' commits");
                            tokio::time::sleep(RETRY).await;
                        }
                    }
                }
            })
        };

        Ok(Listening {
            tasks: [telling, listening],
        })
    }
}

/// An id telling this instance's notifications apart from the others'.
fn instance_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    // A fresh `RandomState` is randomly keyed: enough to tell instances
    // apart, without a dependency.
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    format!("{:016x}", hasher.finish())
}

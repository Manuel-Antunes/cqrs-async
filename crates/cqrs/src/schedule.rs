//! Commands sent later: the [`CommandSchedule`] port (Axon 4's
//! `CommandScheduler`, which Axon 5 dropped for external schedulers).
//!
//! A schedule keeps one command type `C`: what is due is sent through a
//! [`CommandGateway<C>`](crate::CommandGateway) by [`deliver_scheduled`],
//! and the command's handler decides again on the state it finds then. A
//! command handler schedules within its unit of work
//! ([`schedule_in`](CommandSchedule::schedule_in)): the schedule commits with
//! its events, or not at all, and a handler run again after a lost race
//! schedules once.
//!
//! Delivery is at least once: a command sent just before a crash, and not
//! yet marked delivered, is sent again. Its decision should hold when it
//! comes twice (publishing a published post refuses, and the refusal is
//! the end of it: see [`DeliveryOptions::max_attempts`]).

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use crate::{Metadata, StoreError, sync::Locked};

/// What names a scheduled command, to cancel or move it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScheduleToken(pub u64);

/// A command due, claimed for delivery.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Scheduled<C> {
    /// Its token.
    pub token: ScheduleToken,
    /// When it was due.
    pub due: SystemTime,
    /// The command.
    pub command: C,
    /// Sent with it.
    pub metadata: Metadata,
    /// Failed deliveries so far.
    pub attempts: u32,
}

impl<C> Scheduled<C> {
    /// `command`, due at `due`, never tried.
    pub fn new(token: ScheduleToken, due: SystemTime, command: C, metadata: Metadata) -> Self {
        Self {
            token,
            due,
            command,
            metadata,
            attempts: 0,
        }
    }
}

/// Commands of type `C` to send at a given time, kept until delivered.
///
/// `Tx` is the unit of work [`schedule_in`](Self::schedule_in) joins: a
/// command handler's transaction (`()` in memory).
pub trait CommandSchedule<C, Tx = ()>: Send + Sync {
    /// Schedules `command` for `due`, sent with `metadata`, within `tx`:
    /// it exists once `tx` commits.
    fn schedule_in(
        &self,
        tx: &mut Tx,
        due: SystemTime,
        command: &C,
        metadata: &Metadata,
    ) -> impl Future<Output = Result<ScheduleToken, StoreError>> + Send;

    /// Cancels the command `token` names; `false` if it was delivered,
    /// given up or cancelled already.
    fn cancel(&self, token: ScheduleToken) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Moves the command `token` names to `due`; `false` if it is gone.
    fn reschedule(
        &self,
        token: ScheduleToken,
        due: SystemTime,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Claims up to `limit` commands due now, oldest first, for `lease`:
    /// other deliverers skip them until it expires (a deliverer that died
    /// meanwhile leaves them to the next one).
    fn claim_due(
        &self,
        lease: Duration,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Scheduled<C>>, StoreError>> + Send;

    /// Removes the command `token` names: it was delivered.
    fn delivered(&self, token: ScheduleToken) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Records a failed delivery of the command `token` names, with
    /// `error`: tried again at `retry_at`, or, with `None`, given up (kept,
    /// marked failed, for whoever looks into it).
    fn failed(
        &self,
        token: ScheduleToken,
        retry_at: Option<SystemTime>,
        error: &str,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// One scheduled command in memory.
#[derive(Debug)]
struct Entry<C> {
    /// When it is due.
    due: SystemTime,
    /// The command and its metadata.
    command: C,
    /// Sent with it.
    metadata: Metadata,
    /// Failed deliveries.
    attempts: u32,
    /// Claimed by a deliverer until then.
    claimed_until: Option<SystemTime>,
    /// Given up, with the last error.
    failed: Option<String>,
}

/// The scheduled commands by token, and the last token given.
type Entries<C> = (BTreeMap<u64, Entry<C>>, u64);

/// Scheduled commands in memory: lost when the process ends. A handle:
/// clones share the same schedule.
pub struct InMemorySchedule<C> {
    /// By token, and the next token.
    entries: Arc<Mutex<Entries<C>>>,
}

impl<C> Default for InMemorySchedule<C> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new((BTreeMap::new(), 0))),
        }
    }
}

impl<C> Clone for InMemorySchedule<C> {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
        }
    }
}

impl<C> ::std::fmt::Debug for InMemorySchedule<C> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("InMemorySchedule").finish_non_exhaustive()
    }
}

impl<C: Clone> InMemorySchedule<C> {
    /// Schedules `command` for `due` (no unit of work to join in memory).
    pub fn schedule(&self, due: SystemTime, command: C, metadata: Metadata) -> ScheduleToken {
        let mut guard = self.entries.locked();
        let (entries, next) = &mut *guard;
        *next += 1;
        entries.insert(
            *next,
            Entry {
                due,
                command,
                metadata,
                attempts: 0,
                claimed_until: None,
                failed: None,
            },
        );
        ScheduleToken(*next)
    }

    /// The commands not delivered yet: pending, and given up (with their
    /// last error).
    pub fn pending(&self) -> Vec<(ScheduleToken, C, Option<String>)> {
        self.entries
            .locked()
            .0
            .iter()
            .map(|(token, entry)| (ScheduleToken(*token), entry.command.clone(), entry.failed.clone()))
            .collect()
    }
}

impl<C: Clone + Send + Sync> CommandSchedule<C> for InMemorySchedule<C> {
    async fn schedule_in(
        &self,
        (): &mut (),
        due: SystemTime,
        command: &C,
        metadata: &Metadata,
    ) -> Result<ScheduleToken, StoreError> {
        Ok(self.schedule(due, command.clone(), metadata.clone()))
    }

    async fn cancel(&self, token: ScheduleToken) -> Result<bool, StoreError> {
        let mut guard = self.entries.locked();
        Ok(guard.0.remove(&token.0).is_some_and(|entry| entry.failed.is_none()))
    }

    async fn reschedule(&self, token: ScheduleToken, due: SystemTime) -> Result<bool, StoreError> {
        let mut guard = self.entries.locked();
        let Some(entry) = guard.0.get_mut(&token.0).filter(|entry| entry.failed.is_none()) else {
            return Ok(false);
        };
        entry.due = due;
        entry.claimed_until = None;
        Ok(true)
    }

    async fn claim_due(&self, lease: Duration, limit: usize) -> Result<Vec<Scheduled<C>>, StoreError> {
        let now = SystemTime::now();
        let mut guard = self.entries.locked();
        let mut due: Vec<_> = guard
            .0
            .iter_mut()
            .filter(|(_, entry)| {
                entry.failed.is_none() && entry.due <= now && entry.claimed_until.is_none_or(|until| until < now)
            })
            .collect();
        due.sort_by_key(|(token, entry)| (entry.due, **token));
        Ok(due
            .into_iter()
            .take(limit)
            .map(|(token, entry)| {
                entry.claimed_until = Some(now + lease);
                Scheduled {
                    token: ScheduleToken(*token),
                    due: entry.due,
                    command: entry.command.clone(),
                    metadata: entry.metadata.clone(),
                    attempts: entry.attempts,
                }
            })
            .collect())
    }

    async fn delivered(&self, token: ScheduleToken) -> Result<(), StoreError> {
        self.entries.locked().0.remove(&token.0);
        Ok(())
    }

    async fn failed(&self, token: ScheduleToken, retry_at: Option<SystemTime>, error: &str) -> Result<(), StoreError> {
        let mut guard = self.entries.locked();
        if let Some(entry) = guard.0.get_mut(&token.0) {
            entry.attempts += 1;
            entry.claimed_until = None;
            match retry_at {
                Some(retry_at) => entry.due = retry_at,
                None => entry.failed = Some(error.to_owned()),
            }
        }
        Ok(())
    }
}

/// How [`deliver_scheduled`] runs.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct DeliveryOptions {
    /// How often the schedule is asked for what is due, when nothing was.
    pub poll_interval: Duration,
    /// How long a claimed command is kept from other deliverers.
    pub lease: Duration,
    /// Commands claimed at a time.
    pub batch_size: usize,
    /// Failed deliveries before a command is given up (kept, marked
    /// failed). A refusal by the command's decision counts as a failure.
    pub max_attempts: u32,
    /// Wait before the first retry of a failed delivery; doubles each time.
    pub retry_initial: Duration,
    /// Upper bound for that wait.
    pub retry_max: Duration,
}

impl Default for DeliveryOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            lease: Duration::from_secs(30),
            batch_size: 32,
            max_attempts: 5,
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(300),
        }
    }
}

/// Built from [`Default`], one setter per field: `DeliveryOptions::default().poll_interval(..)`.
impl DeliveryOptions {
    /// Sets [`poll_interval`](Self::poll_interval).
    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets [`lease`](Self::lease).
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Sets [`batch_size`](Self::batch_size).
    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets [`max_attempts`](Self::max_attempts).
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets [`retry_initial`](Self::retry_initial).
    #[must_use]
    pub fn retry_initial(mut self, retry_initial: Duration) -> Self {
        self.retry_initial = retry_initial;
        self
    }

    /// Sets [`retry_max`](Self::retry_max).
    #[must_use]
    pub fn retry_max(mut self, retry_max: Duration) -> Self {
        self.retry_max = retry_max;
        self
    }
}

/// Sends the commands of `schedule` as they fall due, through `gateway`,
/// until `shutdown` completes: claims what is due, sends each with its
/// metadata, removes what was handled, and gives a failure a later try
/// (backoff) or, after [`max_attempts`](DeliveryOptions::max_attempts), up.
/// Several deliverers (one per instance) share a schedule: each claims
/// different commands. The caller runs it (awaited, or spawned).
#[cfg(feature = "tokio")]
pub async fn deliver_scheduled<C, Tx, S, G>(
    schedule: S,
    gateway: G,
    options: DeliveryOptions,
    shutdown: impl Future<Output = ()> + Send,
) where
    C: crate::Command + Sync,
    C::Error: std::fmt::Display,
    S: CommandSchedule<C, Tx>,
    G: crate::CommandGateway<C>,
{
    let deliver = async {
        loop {
            let due = match schedule.claim_due(options.lease, options.batch_size.max(1)).await {
                Ok(due) => due,
                Err(error) => {
                    tracing::warn!(%error, "cannot read the schedule");
                    Vec::new()
                }
            };
            let full = due.len() >= options.batch_size.max(1);
            for scheduled in due {
                let token = scheduled.token;
                let outcome = match gateway.send_with(scheduled.command, scheduled.metadata).await {
                    Ok(_) => schedule.delivered(token).await,
                    Err(error) => {
                        let attempts = scheduled.attempts + 1;
                        let error = error.to_string();
                        let retry_at = (attempts < options.max_attempts).then(|| {
                            let wait = options
                                .retry_initial
                                .saturating_mul(2_u32.saturating_pow(attempts - 1))
                                .min(options.retry_max);
                            SystemTime::now() + wait
                        });
                        tracing::warn!(token = token.0, attempts, %error, given_up = retry_at.is_none(), "a scheduled command failed");
                        schedule.failed(token, retry_at, &error).await
                    }
                };
                if let Err(error) = outcome {
                    // Its lease expires and it is claimed again: at least once.
                    tracing::warn!(token = token.0, %error, "cannot record a delivery");
                }
            }
            if !full {
                tokio::time::sleep(options.poll_interval).await;
            }
        }
    };
    tokio::select! {
        () = deliver => {}
        () = shutdown => {}
    }
}

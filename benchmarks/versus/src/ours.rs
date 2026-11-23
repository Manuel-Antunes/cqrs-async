//! The bank on this crate: a command handler finding accounts through its
//! own port (loaded by their events) and committing through the event
//! publisher, the read model as a `Projection` committing with its token,
//! and the transfer policy as a tracking processor. Both processors run in
//! 8 segments (accounts in parallel, each account in order), handling the
//! events waiting for a segment in batches of up to 100 per transaction.

use std::{future::Future, sync::Arc};

use cqrs::{
    Aggregate, Appender, CommandGateway, CommandHandler, EventAppender, EventEnvelope, EventHandler,     EventSourced, EventStore, Events, ExpectedVersion, HandlerError, Loaded, Metadata, ProcessingContext,
    ProcessorHandle, Published, SnapshotStore, StoreError, StreamId, TrackingOptions, WithPublisher,
};
use cqrs_postgres::{PgTransaction, PostgresEventStore, PostgresSnapshotStore, PostgresTokenStore};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::{
    bank::{Account, AccountEvent, Balances, BankCommand, BankError, fold},
    system::System,
};

#[derive(Debug, Serialize, Deserialize, cqrs::AggregateRoot)]
#[aggregate(name = "account", event = AccountEvent)]
pub struct OurAccount {
    id: String,
    state: Account,
}

impl OurAccount {
    fn apply(&mut self, event: &AccountEvent) {
        self.state.evolve(event);
    }
}

impl From<StoreError> for BankError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } | StoreError::ConditionFailed => Self::Conflict,
            other => Self::Store(other.to_string()),
        }
    }
}

#[derive(Clone, Debug, cqrs::Command)]
#[command(error = BankError)]
pub struct Execute {
    pub account: String,
    pub command: BankCommand,
}

/// The bank's port to its accounts: the application's repository.
pub trait Accounts: Send + Sync {
    /// The account, at the version it was read at; `None` if it has no events.
    fn find(&self, id: &str) -> impl Future<Output = Result<Option<Loaded<OurAccount>>, StoreError>> + Send;
}

/// Its adapter: accounts rebuilt from their events.
pub struct EventSourcedAccounts<N>(Arc<Repo<N>>);

impl<N: SnapshotStore<OurAccount>> Accounts for EventSourcedAccounts<N> {
    async fn find(&self, id: &str) -> Result<Option<Loaded<OurAccount>>, StoreError> {
        self.0.load(&id.to_owned()).await
    }
}

pub struct ExecuteHandler<R> {
    accounts: R,
}

#[cqrs::command_handler]
impl<R: Accounts> CommandHandler for ExecuteHandler<R> {
    async fn handle(&self, command: &Execute, events: &mut Appender<AccountEvent>) -> Result<(), BankError> {
        let account = match self.accounts.find(&command.account).await? {
            Some(account) => account,
            None => Loaded::new(OurAccount::initial(&command.account)),
        };
        for event in account.state.decide(&command.command)? {
            events.append(event);
        }
        events.commit(account).await?;
        Ok(())
    }

    /// The harness retries, as it does for cqrs-es: equal retry policies.
    fn conflict_retries(&self) -> u32 {
        0
    }
}

#[derive(cqrs::Gateway)]
#[gateway(commands)]
pub struct Bank<R> {
    execute: ExecuteHandler<R>,
}

/// `account_views`, one row per account, written in the processor's
/// transaction.
struct AccountViews;

#[cqrs::projection(namespace = "views")]
impl EventHandler for AccountViews {
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query(
            "INSERT INTO account_views (id, balance, events) VALUES ($1, $2, 1) ON CONFLICT (id) DO UPDATE \
             SET balance = account_views.balance + EXCLUDED.balance, events = account_views.events + 1",
        )
        .bind(event.stream.id())
        .bind(event.payload.delta())
        .execute(&mut **context.transaction())
        .await?;
        Ok(())
    }

    async fn reset(&self, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query("TRUNCATE account_views").execute(&mut **context.transaction()).await?;
        Ok(())
    }
}

/// Credits the target of each transfer.
struct TransferPolicy<B> {
    bank: Arc<B>,
}

#[cqrs::projection(namespace = "transfers")]
impl<B: CommandGateway<Execute> + 'static> EventHandler for TransferPolicy<B> {
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if let AccountEvent::TransferredOut { to, amount } = &event.payload {
            let deposit = Execute { account: to.clone(), command: BankCommand::Deposit { amount: *amount } };
            while self.bank.send(deposit.clone()).await == Err(BankError::Conflict) {}
        }
        Ok(())
    }
}

type Store = PostgresEventStore<AccountEvent>;
type OurEvents = Events<AccountEvent, Store, PostgresTokenStore>;
type Repo<N> = EventSourced<OurAccount, Store, OurEvents, N>;

pub struct Ours<N> {
    pool: PgPool,
    events: OurEvents,
    bank: Arc<Published<Bank<EventSourcedAccounts<N>>, OurEvents>>,
    views: Mutex<Option<ProcessorHandle>>,
    transfers: Mutex<Option<ProcessorHandle>>,
}

/// 8 segments; events per unit of work from `CQRS_VERSUS_BATCH` (default
/// 100; 1 handles each event in its own transaction).
fn options() -> TrackingOptions {
    let batch_size = std::env::var("CQRS_VERSUS_BATCH").ok().and_then(|value| value.parse().ok()).unwrap_or(100);
    TrackingOptions { segments: 8, batch_size, ..TrackingOptions::default() }
}

/// Starts `processor` and waits until it owns all its segments.
async fn started(handle: ProcessorHandle) -> ProcessorHandle {
    while handle.status().segments.len() < options().segments as usize {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    handle
}

impl Ours<()> {
    /// Every load replays the account's events.
    pub async fn new(pool: PgPool) -> Self {
        Self::build(pool, |repository| repository).await
    }
}

impl Ours<PostgresSnapshotStore<OurAccount>> {
    /// A snapshot every `every` events.
    pub async fn with_snapshots(pool: PgPool, every: u64) -> Self {
        let snapshots = PostgresSnapshotStore::new(pool.clone());
        Self::build(pool, move |repository| repository.with_snapshots(snapshots, every)).await
    }
}

impl<N> Ours<N>
where
    N: SnapshotStore<OurAccount> + 'static,
{
    async fn build(pool: PgPool, snapshots: impl FnOnce(Repo<()>) -> Repo<N>) -> Self {
        cqrs_postgres::migrate(&pool).await.expect("migrating");
        sqlx::query("CREATE TABLE account_views (id text PRIMARY KEY, balance bigint NOT NULL, events bigint NOT NULL)")
            .execute(&pool)
            .await
            .expect("creating the view");
        let events = Events::new(Store::new(pool.clone())).with_tokens(PostgresTokenStore::new(pool.clone()));
        let accounts = Arc::new(snapshots(events.event_sourced()));
        let execute = ExecuteHandler { accounts: EventSourcedAccounts(accounts) };
        let bank = Arc::new(Bank { execute }.with_publisher(events.clone()));
        let views = started(events.projection(AccountViews).with_options(options()).spawn()).await;
        let policy = TransferPolicy { bank: Arc::clone(&bank) };
        let transfers = started(events.projection(policy).with_options(options()).spawn()).await;
        Self { pool, events, bank, views: Mutex::new(Some(views)), transfers: Mutex::new(Some(transfers)) }
    }
}

impl<N> System for Ours<N>
where
    N: Send + Sync + 'static,
    N: SnapshotStore<OurAccount> + 'static,
{
    async fn execute(&self, account: &str, command: BankCommand) -> Result<(), BankError> {
        self.bank.send(Execute { account: account.to_owned(), command }).await
    }

    async fn settle(&self) {
        let (views, transfers) = (self.views.lock().await, self.transfers.lock().await);
        let (views, transfers) = (views.as_ref().expect("running"), transfers.as_ref().expect("running"));
        loop {
            let head = self.events.store().last_position().await.expect("reading the head");
            views.caught_up(head).await;
            transfers.caught_up(head).await;
            // The policy's deposits are events too.
            if self.events.store().last_position().await.expect("reading the head") == head {
                return;
            }
        }
    }

    async fn views(&self) -> Balances {
        let rows: Vec<(String, i64, i64)> =
            sqlx::query_as("SELECT id, balance, events FROM account_views").fetch_all(&self.pool).await.expect("views");
        rows.into_iter().map(|(id, balance, events)| (id, (balance, events))).collect()
    }

    async fn truth(&self) -> Balances {
        let rows: Vec<(String, serde_json::Value)> =
            sqlx::query_as("SELECT stream_id, payload FROM cqrs_events WHERE stream_type = 'account' ORDER BY position")
                .fetch_all(&self.pool)
                .await
                .expect("events");
        fold(rows)
    }

    async fn rebuild(&self) -> u64 {
        let mut views = self.views.lock().await;
        views.take().expect("running").stop().await;
        let projection = self.events.projection(AccountViews).with_options(options());
        projection.reset().await.expect("resetting");
        let running = projection.spawn();
        let head = self.events.store().last_position().await.expect("reading the head");
        running.caught_up(head).await;
        *views = Some(running);
        head
    }

    async fn seed(&self, account: &str, deposits: usize) {
        let stream = StreamId::of::<OurAccount>(&account.to_owned());
        for chunk in (0..deposits).collect::<Vec<_>>().chunks(1_000) {
            let batch = chunk.iter().map(|_| AccountEvent::Deposited { amount: 1 }).collect();
            self.events.store().append(&stream, ExpectedVersion::Any, batch, &Metadata::new()).await.expect("seeding");
        }
    }

    async fn close(&self) {
        for processor in [&self.views, &self.transfers] {
            if let Some(running) = processor.lock().await.take() {
                running.stop().await;
            }
        }
        self.pool.close().await;
    }
}

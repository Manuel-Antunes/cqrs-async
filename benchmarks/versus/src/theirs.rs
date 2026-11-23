//! The bank on cqrs-es 0.5 + postgres-es 0.5, as their docs and demo wire
//! it: `CqrsFramework` over a `PersistedEventStore`, the read model as a
//! `GenericQuery` over a `PostgresViewRepository`, and the transfer policy
//! as a `Query` sending commands. Queries run inline, after each commit.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use cqrs_es::{
    Aggregate, AggregateError, CqrsFramework, EventEnvelope, Query, View,
    event_sink::EventSink,
    persist::{GenericQuery, PersistedEventStore, QueryReplay},
};
use postgres_es::{PostgresEventRepository, PostgresViewRepository};
use serde::{Deserialize, Serialize};
use sqlx08::{PgPool, Row};

use crate::{
    bank::{Account, AccountEvent, Balances, BankCommand, BankError, fold},
    system::System,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TheirAccount {
    state: Account,
}

impl Aggregate for TheirAccount {
    const TYPE: &'static str = "account";
    type Command = BankCommand;
    type Event = AccountEvent;
    type Error = BankError;
    type Services = ();

    async fn handle(&mut self, command: BankCommand, _: &(), sink: &EventSink<Self>) -> Result<(), BankError> {
        for event in self.state.decide(&command)? {
            sink.write(event, self).await;
        }
        Ok(())
    }

    fn apply(&mut self, event: AccountEvent) {
        self.state.evolve(&event);
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AccountView {
    balance: i64,
    events: i64,
}

impl View<TheirAccount> for AccountView {
    fn update(&mut self, event: &EventEnvelope<TheirAccount>) {
        self.balance += event.payload.delta();
        self.events += 1;
    }
}

type Framework = CqrsFramework<TheirAccount, PersistedEventStore<PostgresEventRepository, TheirAccount>>;
type Views = GenericQuery<PostgresViewRepository<AccountView, TheirAccount>, AccountView, TheirAccount>;

/// Credits the target of each transfer, inline, from the source command.
struct TransferPolicy {
    cqrs: Arc<OnceLock<Arc<Framework>>>,
}

#[async_trait]
impl Query<TheirAccount> for TransferPolicy {
    async fn dispatch(&self, _: &str, events: &[EventEnvelope<TheirAccount>]) {
        let cqrs = self.cqrs.get().expect("wired");
        for event in events {
            if let AccountEvent::TransferredOut { to, amount } = &event.payload {
                let deposit = || BankCommand::Deposit { amount: *amount };
                while let Err(AggregateError::AggregateConflict) = cqrs.execute(to, deposit()).await {}
            }
        }
    }
}

pub struct Theirs {
    pool: PgPool,
    cqrs: Arc<Framework>,
}

const SCHEMA: &str = "
CREATE TABLE events (
    aggregate_type text NOT NULL, aggregate_id text NOT NULL, sequence bigint CHECK (sequence >= 0) NOT NULL,
    event_type text NOT NULL, event_version text NOT NULL, payload json NOT NULL, metadata json NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
CREATE TABLE snapshots (
    aggregate_type text NOT NULL, aggregate_id text NOT NULL, last_sequence bigint CHECK (last_sequence >= 0) NOT NULL,
    current_snapshot bigint CHECK (current_snapshot >= 0) NOT NULL, payload json NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, last_sequence)
);
CREATE TABLE account_view (
    view_id text NOT NULL, version bigint CHECK (version >= 0) NOT NULL, payload json NOT NULL,
    PRIMARY KEY (view_id)
);";

impl Theirs {
    /// postgres-es' `db/init.sql` (events, snapshots, one view table), then
    /// the framework: every load replays, or a snapshot every `snapshots`.
    pub async fn new(pool: PgPool, snapshots: Option<usize>) -> Self {
        sqlx08::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .expect("creating the tables");
        let repository = PostgresEventRepository::new(pool.clone());
        let store = match snapshots {
            Some(size) => PersistedEventStore::new_snapshot_store(repository, size),
            None => PersistedEventStore::new_event_store(repository),
        };
        let cqrs = Arc::new(OnceLock::new());
        let queries: Vec<Box<dyn Query<TheirAccount>>> = vec![
            Box::new(views(&pool)),
            Box::new(TransferPolicy {
                cqrs: Arc::clone(&cqrs),
            }),
        ];
        let framework = Arc::new(CqrsFramework::new(store, queries, ()));
        cqrs.set(Arc::clone(&framework)).ok().expect("set once");
        Self { pool, cqrs: framework }
    }
}

fn views(pool: &PgPool) -> Views {
    GenericQuery::new(Arc::new(PostgresViewRepository::new("account_view", pool.clone())))
}

impl System for Theirs {
    async fn execute(&self, account: &str, command: BankCommand) -> Result<(), BankError> {
        self.cqrs.execute(account, command).await.map_err(|error| match error {
            AggregateError::UserError(error) => error,
            AggregateError::AggregateConflict => BankError::Conflict,
            other => BankError::Store(other.to_string()),
        })
    }

    /// Everything already ran inline, within `execute`.
    async fn settle(&self) {}

    async fn views(&self) -> Balances {
        let rows = sqlx08::query("SELECT view_id, payload FROM account_view")
            .fetch_all(&self.pool)
            .await
            .expect("views");
        rows.iter()
            .map(|row| {
                let view: AccountView = serde_json::from_value(row.get("payload")).expect("a view");
                (row.get("view_id"), (view.balance, view.events))
            })
            .collect()
    }

    async fn truth(&self) -> Balances {
        let rows = sqlx08::query(
            "SELECT aggregate_id, payload FROM events WHERE aggregate_type = 'account' ORDER BY aggregate_id, sequence",
        )
        .fetch_all(&self.pool)
        .await
        .expect("events");
        fold(rows.iter().map(|row| (row.get("aggregate_id"), row.get("payload"))))
    }

    /// Their replay: every event of the aggregate type through the query.
    async fn rebuild(&self) -> u64 {
        sqlx08::query("TRUNCATE account_view")
            .execute(&self.pool)
            .await
            .expect("truncating");
        let replay = QueryReplay::new(PostgresEventRepository::new(self.pool.clone()), views(&self.pool));
        replay.replay_all().await.expect("replaying");
        let count: i64 = sqlx08::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&self.pool)
            .await
            .expect("count");
        count as u64
    }

    async fn seed(&self, account: &str, deposits: usize) {
        sqlx08::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             SELECT 'account', $1, n, 'account.deposited', '1.0', '{\"Deposited\":{\"amount\":1}}', '{}' \
             FROM generate_series((SELECT max(sequence) + 1 FROM events WHERE aggregate_id = $1), \
                                  (SELECT max(sequence) FROM events WHERE aggregate_id = $1) + $2) AS n",
        )
        .bind(account)
        .bind(deposits as i64)
        .execute(&self.pool)
        .await
        .expect("seeding");
    }

    /// The framework and its policy hold each other, so the pool is closed
    /// explicitly.
    async fn close(&self) {
        self.pool.close().await;
    }
}

//! The bank: customers and accounts, each aggregate root on its own log;
//! the portfolios projected from both (a fan-in); the accounts' log
//! forwarded to RabbitMQ, for the services that follow the money.

pub mod api;
pub mod commands;
pub mod domain;
pub mod portfolio;

use cqrs::{
    processing::{Events, Forward, ProcessorHandle, ProjectionHandle},
    store::{FanIn, InMemoryEventStore, Json, ReadLog},
};
use cqrs_amqp::{Amqp, AmqpError, AmqpPublisher};

use self::{
    commands::{AccountGateway, CustomerGateway},
    domain::{Account, AccountEvent, Customer, CustomerEvent},
    portfolio::{BankEvent, PortfolioProjection, PortfolioQueries, SharedPortfolios},
};

/// Where the accounts' events are published: a fanout exchange, so every
/// service that binds a queue to it gets all of them.
pub const ACCOUNTS_TOPIC: &str = "bank.accounts";

/// The customers' events service: their log, and its processors.
pub type Customers = Events<CustomerEvent, InMemoryEventStore<CustomerEvent>>;
/// The accounts' events service.
pub type Accounts = Events<AccountEvent, InMemoryEventStore<AccountEvent>>;
/// Both logs, read as one.
pub type Ledger =
    Events<BankEvent, FanIn<BankEvent, (InMemoryEventStore<CustomerEvent>, InMemoryEventStore<AccountEvent>)>>;

/// The bank, running: its gateways, its queries, and the processors
/// following its logs.
///
/// The composition root: the logs are in memory here. On PostgreSQL, the
/// same wiring is `pg.events::<CustomerEvent>("customers")`,
/// `pg.events::<AccountEvent>("accounts")` and
/// `pg.fan_in((&customers, &accounts))`; nothing else changes.
pub struct Bank {
    /// The customers' commands.
    pub customers: CustomerGateway<Customers>,
    /// The accounts' commands.
    pub accounts: AccountGateway<Accounts>,
    /// The portfolios' queries.
    pub queries: PortfolioQueries,
    /// The accounts' log, as events.
    account_events: Accounts,
    /// Both, as one.
    ledger: Ledger,
    /// The portfolio projection's processor.
    portfolio: ProjectionHandle,
    /// The outbox's processor, once [`forward_accounts`](Self::forward_accounts) started it.
    outbox: Option<ProcessorHandle>,
}

impl std::fmt::Debug for Bank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bank").finish_non_exhaustive()
    }
}

impl Bank {
    /// Starts the bank, in memory: its logs, its gateways, and the
    /// portfolio projection over both logs. Must run inside a Tokio
    /// runtime.
    pub fn start() -> Self {
        // The accounts' store is made beside the customers': one numbering,
        // so the two logs are in one order, which the fan-in reads.
        let customer_store = InMemoryEventStore::<CustomerEvent>::new();
        let account_store = customer_store.beside::<AccountEvent>();
        let customer_events = Customers::new(customer_store);
        let account_events = Accounts::new(account_store);
        let ledger: Ledger = Events::fan_in((&customer_events, &account_events));

        let portfolios = SharedPortfolios::default();
        let portfolio = ledger
            .projection(PortfolioProjection {
                portfolios: portfolios.clone(),
            })
            .spawn();
        Self {
            customers: CustomerGateway {
                customers: customer_events.repository::<Customer>(),
                events: customer_events.clone(),
            },
            accounts: AccountGateway {
                customers: customer_events.repository::<Customer>(),
                accounts: account_events.repository::<Account>(),
                events: account_events.clone(),
            },
            queries: PortfolioQueries { portfolios },
            account_events,
            ledger,
            portfolio,
            outbox: None,
        }
    }

    /// Forwards the accounts' log to RabbitMQ, to `topic` (a fanout
    /// exchange; [`ACCOUNTS_TOPIC`] in production): a
    /// tracking processor publishes each committed event, from its token,
    /// retrying while the broker is down. The log is the outbox: nothing is
    /// published that was not committed, nothing committed is lost.
    ///
    /// # Errors
    ///
    /// If the publisher's channel cannot be opened.
    pub async fn forward_accounts(&mut self, amqp: &Amqp, topic: &str) -> Result<(), AmqpError> {
        let publisher = AmqpPublisher::fanout(amqp).await?;
        let forward = Forward::new(publisher, Json, topic);
        self.outbox = Some(self.account_events.track("outbox", forward));
        Ok(())
    }

    /// Waits until the portfolios (and the outbox, if any) have everything
    /// committed so far.
    pub async fn caught_up(&self) {
        let last = self.ledger.store().last_position().await.unwrap_or(0);
        self.portfolio.caught_up(last).await;
        if let Some(outbox) = &self.outbox {
            let last = self.account_events.store().last_position().await.unwrap_or(0);
            outbox.caught_up(last).await;
        }
    }

    /// Stops the processors, letting the events in hand finish.
    pub async fn stop(self) {
        self.portfolio.stop().await;
        if let Some(outbox) = self.outbox {
            outbox.stop().await;
        }
    }
}

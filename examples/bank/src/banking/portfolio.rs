//! Each customer's portfolio: their name and their accounts' balances.
//!
//! The name is in the customers' log, the balances in the accounts' log:
//! the projection reads both as one (a fan-in), in the order they were
//! committed, so a customer is always registered before their first
//! account shows up, and a rename lands between the deposits it came
//! between.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, PoisonError},
};

use cqrs::{
    event::EventEnvelope,
    processing::{EventHandler, HandlerError, ProcessingContext},
    query::QueryHandler,
};

use super::domain::{Account, AccountEvent, BankError, Customer, CustomerEvent};

/// What the portfolio projection reads: both logs, in one order. Only this
/// projection joins them; the write side never sees this type.
#[derive(Clone, Debug, PartialEq, Eq, cqrs::DomainEvent)]
#[event(fan_in)]
pub enum BankEvent {
    /// From the customers' log.
    Customer(CustomerEvent),
    /// From the accounts' log.
    Account(AccountEvent),
}

/// A customer's portfolio, as the read model keeps it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Portfolio {
    /// The customer's current name.
    pub name: String,
    /// Each account's balance, in cents, by number.
    pub accounts: BTreeMap<u32, u64>,
}

impl Portfolio {
    /// Everything the customer has, in cents.
    pub fn total(&self) -> u64 {
        self.accounts.values().sum()
    }
}

/// The read model: portfolios by customer, and whose each account is.
#[derive(Debug, Default)]
pub struct Portfolios {
    /// By customer number.
    customers: HashMap<u32, Portfolio>,
    /// Each account's owner.
    owners: HashMap<u32, u32>,
}

/// The portfolios, shared by the projection writing them and the queries
/// reading them.
pub type SharedPortfolios = Arc<Mutex<Portfolios>>;

/// Projects both logs into the portfolios.
#[cqrs::projection(name = "portfolio", events = BankEvent, namespaces("portfolios"))]
pub struct PortfolioProjection {
    /// The read model.
    pub portfolios: SharedPortfolios,
}

#[cqrs::event_handler(namespace = "portfolios")]
impl<Tx: Send> EventHandler for PortfolioProjection {
    async fn handle(
        &self,
        event: &EventEnvelope<BankEvent>,
        _: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        let mut portfolios = self.portfolios.lock().unwrap_or_else(PoisonError::into_inner);
        match &event.payload {
            BankEvent::Customer(CustomerEvent::Registered { name } | CustomerEvent::Renamed { name }) => {
                let customer = id_of::<Customer>(event)?;
                portfolios.customers.entry(customer).or_default().name.clone_from(name);
            }
            BankEvent::Account(AccountEvent::Opened { customer }) => {
                let account = id_of::<Account>(event)?;
                // The fan-in's order: the customer was registered before.
                let portfolio = portfolios
                    .customers
                    .get_mut(customer)
                    .ok_or_else(|| HandlerError::msg(format!("account {account} of unknown customer {customer}")))?;
                portfolio.accounts.insert(account, 0);
                portfolios.owners.insert(account, *customer);
            }
            BankEvent::Account(AccountEvent::Deposited { amount }) => {
                *portfolios.balance(id_of::<Account>(event)?)? += amount;
            }
            BankEvent::Account(AccountEvent::Withdrawn { amount }) => {
                *portfolios.balance(id_of::<Account>(event)?)? -= amount;
            }
        }
        Ok(())
    }

    /// Before a replay rebuilds it.
    async fn reset(&self, _: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        *self.portfolios.lock().unwrap_or_else(PoisonError::into_inner) = Portfolios::default();
        Ok(())
    }
}

impl Portfolios {
    /// The balance of account `account`, in its owner's portfolio.
    fn balance(&mut self, account: u32) -> Result<&mut u64, HandlerError> {
        self.owners
            .get(&account)
            .and_then(|owner| self.customers.get_mut(owner))
            .and_then(|portfolio| portfolio.accounts.get_mut(&account))
            .ok_or_else(|| HandlerError::msg(format!("account {account} was not opened")))
    }
}

/// The id of the aggregate `A` an event is about.
fn id_of<A: cqrs::aggregate::Aggregate<Id = u32>>(event: &EventEnvelope<BankEvent>) -> Result<u32, HandlerError> {
    event
        .aggregate_id::<A>()
        .ok_or_else(|| HandlerError::msg(format!("{} is not a {}", event.stream, A::TYPE)))
}

/// A customer's portfolio, if they are a customer.
#[cqrs::query(output = Option<Portfolio>, error = BankError)]
#[derive(Clone, Debug)]
pub struct PortfolioOf {
    /// The customer.
    pub customer: u32,
}

/// Answers from the portfolios.
#[cqrs::query_gateway(queries = BankQueries, handles(PortfolioOf))]
pub struct PortfolioQueries {
    /// The read model.
    pub portfolios: SharedPortfolios,
}

#[cqrs::query_handler]
impl QueryHandler<PortfolioOf> for PortfolioQueries {
    async fn handle(&self, query: PortfolioOf) -> Result<Option<Portfolio>, BankError> {
        let portfolios = self.portfolios.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(portfolios.customers.get(&query.customer).cloned())
    }
}

//! The bank's domain: customers and their accounts, two aggregate roots,
//! each with its own events, so each with its own log.

use cqrs::aggregate::EventAppender;
use serde::{Deserialize, Serialize};

/// What happened to a customer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, cqrs::DomainEvent)]
pub enum CustomerEvent {
    /// The customer became one.
    #[event(name = "customer.registered")]
    Registered {
        /// Their name.
        name: String,
    },
    /// The customer changed their name.
    #[event(name = "customer.renamed")]
    Renamed {
        /// Their new name.
        name: String,
    },
}

/// A customer of the bank.
#[cqrs::aggregate(name = "customer", event = CustomerEvent)]
#[derive(Debug)]
pub struct Customer {
    /// Their number.
    pub id: u32,
    /// Their current name.
    pub name: String,
}

impl Customer {
    fn apply(&mut self, event: &CustomerEvent) {
        match event {
            CustomerEvent::Registered { name } | CustomerEvent::Renamed { name } => self.name.clone_from(name),
        }
    }

    /// Registers a customer named `name`.
    ///
    /// # Errors
    ///
    /// If the name is blank.
    pub fn register(name: &str, events: &mut impl EventAppender<CustomerEvent>) -> Result<(), BankError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(BankError::BlankName);
        }
        events.append(CustomerEvent::Registered { name: name.to_owned() });
        Ok(())
    }

    /// Renames the customer; nothing happens if the name is the same.
    ///
    /// # Errors
    ///
    /// If the name is blank.
    pub fn rename(&self, name: &str, events: &mut impl EventAppender<CustomerEvent>) -> Result<(), BankError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(BankError::BlankName);
        }
        if name != self.name {
            events.append(CustomerEvent::Renamed { name: name.to_owned() });
        }
        Ok(())
    }
}

/// What happened to an account. Serialized as JSON: it is what the bank
/// publishes to RabbitMQ, for other services to read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, cqrs::DomainEvent)]
pub enum AccountEvent {
    /// The account was opened for a customer.
    #[event(name = "account.opened")]
    Opened {
        /// Its owner.
        #[tag]
        customer: u32,
    },
    /// Money came in.
    #[event(name = "account.deposited")]
    Deposited {
        /// How much, in cents.
        amount: u64,
    },
    /// Money went out.
    #[event(name = "account.withdrawn")]
    Withdrawn {
        /// How much, in cents.
        amount: u64,
    },
}

/// A customer's account.
#[cqrs::aggregate(name = "account", event = AccountEvent)]
#[derive(Debug)]
pub struct Account {
    /// Its number.
    pub id: u32,
    /// Its owner.
    pub customer: u32,
    /// What is in it, in cents.
    pub balance: u64,
}

impl Account {
    fn apply(&mut self, event: &AccountEvent) {
        match *event {
            AccountEvent::Opened { customer } => self.customer = customer,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }

    /// Opens an account for `customer`.
    pub fn open(customer: u32, events: &mut impl EventAppender<AccountEvent>) {
        events.append(AccountEvent::Opened { customer });
    }

    /// Puts `amount` in.
    ///
    /// # Errors
    ///
    /// If `amount` is zero.
    pub fn deposit(&self, amount: u64, events: &mut impl EventAppender<AccountEvent>) -> Result<(), BankError> {
        if amount == 0 {
            return Err(BankError::NothingToMove);
        }
        events.append(AccountEvent::Deposited { amount });
        Ok(())
    }

    /// Takes `amount` out.
    ///
    /// # Errors
    ///
    /// If `amount` is zero, or more than the balance.
    pub fn withdraw(&self, amount: u64, events: &mut impl EventAppender<AccountEvent>) -> Result<(), BankError> {
        if amount == 0 {
            return Err(BankError::NothingToMove);
        }
        if amount > self.balance {
            return Err(BankError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }
        events.append(AccountEvent::Withdrawn { amount });
        Ok(())
    }
}

/// Why the bank refused a command.
#[derive(Debug, PartialEq, Eq)]
pub enum BankError {
    /// A customer's name is blank.
    BlankName,
    /// No such customer.
    NoSuchCustomer(u32),
    /// No such account.
    NoSuchAccount(u32),
    /// The customer or account number is taken.
    AlreadyExists,
    /// An amount of zero.
    NothingToMove,
    /// More than the balance.
    InsufficientFunds {
        /// What the account has.
        balance: u64,
        /// What was asked for.
        requested: u64,
    },
    /// The store failed.
    Store(String),
}

impl std::fmt::Display for BankError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankName => f.write_str("a name cannot be blank"),
            Self::NoSuchCustomer(id) => write!(f, "no customer {id}"),
            Self::NoSuchAccount(id) => write!(f, "no account {id}"),
            Self::AlreadyExists => f.write_str("that number is taken"),
            Self::NothingToMove => f.write_str("the amount is zero"),
            Self::InsufficientFunds { balance, requested } => {
                write!(f, "{requested} requested, {balance} available")
            }
            Self::Store(error) => write!(f, "store: {error}"),
        }
    }
}

impl std::error::Error for BankError {}

impl From<cqrs::store::StoreError> for BankError {
    fn from(error: cqrs::store::StoreError) -> Self {
        match error {
            // A new stream that exists already: the number is taken.
            cqrs::store::StoreError::Conflict {
                actual: Some(_),
                expected: cqrs::store::ExpectedVersion::NoStream,
                ..
            } => Self::AlreadyExists,
            error => Self::Store(error.to_string()),
        }
    }
}

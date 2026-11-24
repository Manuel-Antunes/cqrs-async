//! The bank, shared by both systems: commands, events, and the decisions.
//! Each framework only wraps [`Account`] in its own aggregate type, so both
//! run exactly the same rules.

use std::fmt;

use serde::{Deserialize, Serialize};

/// What an account is asked to do.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BankCommand {
    Open,
    Deposit {
        amount: i64,
    },
    Withdraw {
        amount: i64,
    },
    /// Debits this account; a policy then credits `to`.
    Transfer {
        to: String,
        amount: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent)]
pub enum AccountEvent {
    #[event(name = "account.opened")]
    Opened,
    #[event(name = "account.deposited")]
    Deposited { amount: i64 },
    #[event(name = "account.withdrew")]
    Withdrew { amount: i64 },
    #[event(name = "account.transferred_out")]
    TransferredOut { to: String, amount: i64 },
}

impl AccountEvent {
    /// How the event moves the balance.
    pub fn delta(&self) -> i64 {
        match self {
            Self::Opened => 0,
            Self::Deposited { amount } => *amount,
            Self::Withdrew { amount } | Self::TransferredOut { amount, .. } => -amount,
        }
    }
}

/// cqrs-es names events with `String`s.
impl cqrs_es::DomainEvent for AccountEvent {
    fn event_type(&self) -> String {
        cqrs::DomainEvent::event_type(self).to_owned()
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BankError {
    AlreadyOpen,
    NotOpen,
    InsufficientFunds,
    /// Another command changed the account first: decide again.
    Conflict,
    Store(String),
}

impl fmt::Display for BankError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for BankError {}

/// The state both aggregates wrap.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub open: bool,
    pub balance: i64,
}

impl Account {
    pub fn decide(&self, command: &BankCommand) -> Result<Vec<AccountEvent>, BankError> {
        match command {
            BankCommand::Open if self.open => Err(BankError::AlreadyOpen),
            BankCommand::Open => Ok(vec![AccountEvent::Opened]),
            _ if !self.open => Err(BankError::NotOpen),
            BankCommand::Deposit { amount } => Ok(vec![AccountEvent::Deposited { amount: *amount }]),
            BankCommand::Withdraw { amount } | BankCommand::Transfer { amount, .. } if *amount > self.balance => {
                Err(BankError::InsufficientFunds)
            }
            BankCommand::Withdraw { amount } => Ok(vec![AccountEvent::Withdrew { amount: *amount }]),
            BankCommand::Transfer { to, amount } => Ok(vec![AccountEvent::TransferredOut {
                to: to.clone(),
                amount: *amount,
            }]),
        }
    }

    pub fn evolve(&mut self, event: &AccountEvent) {
        if let AccountEvent::Opened = event {
            self.open = true;
        }
        self.balance += event.delta();
    }
}

/// `(balance, events applied)` per account.
pub type Balances = std::collections::BTreeMap<String, (i64, i64)>;

/// Folds `(account, payload)` rows into balances: the truth a read model is
/// compared with.
pub fn fold(rows: impl IntoIterator<Item = (String, serde_json::Value)>) -> Balances {
    let mut balances = Balances::new();
    for (account, payload) in rows {
        let event: AccountEvent = serde_json::from_value(payload).expect("an account event");
        let entry = balances.entry(account).or_default();
        entry.0 += event.delta();
        entry.1 += 1;
    }
    balances
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rules() {
        let mut account = Account::default();
        assert_eq!(
            account.decide(&BankCommand::Deposit { amount: 1 }),
            Err(BankError::NotOpen)
        );
        for event in account.decide(&BankCommand::Open).unwrap() {
            account.evolve(&event);
        }
        assert_eq!(account.decide(&BankCommand::Open), Err(BankError::AlreadyOpen));
        assert_eq!(
            account.decide(&BankCommand::Withdraw { amount: 1 }),
            Err(BankError::InsufficientFunds)
        );
        account.evolve(&AccountEvent::Deposited { amount: 10 });
        let to = "b".to_owned();
        let out = account
            .decide(&BankCommand::Transfer {
                to: to.clone(),
                amount: 4,
            })
            .unwrap();
        assert_eq!(out, [AccountEvent::TransferredOut { to, amount: 4 }]);
        account.evolve(&out[0]);
        assert_eq!(account.balance, 6);
    }
}

//! The bank on its own (no broker): commands on two logs, and the
//! portfolios projected from both, in commit order.

use bank::banking::{
    Bank,
    commands::{Deposit, OpenAccount, RegisterCustomer, RenameCustomer, Withdraw},
    domain::BankError,
    portfolio::{Portfolio, PortfolioOf},
};
use cqrs::{command::CommandGateway, query::QueryGateway};

async fn portfolio(bank: &Bank, customer: u32) -> Option<Portfolio> {
    bank.queries.query(PortfolioOf { customer }).await.unwrap()
}

#[tokio::test]
async fn portfolios_join_the_customers_and_the_accounts_logs() {
    let bank = Bank::start();
    let (customers, accounts) = (&bank.customers, &bank.accounts);
    customers
        .send(RegisterCustomer {
            id: 1,
            name: "Ana".into(),
        })
        .await
        .unwrap();
    accounts.send(OpenAccount { id: 10, customer: 1 }).await.unwrap();
    accounts
        .send(Deposit {
            account: 10,
            amount: 500,
        })
        .await
        .unwrap();
    customers
        .send(RenameCustomer {
            id: 1,
            name: "Ana Maria".into(),
        })
        .await
        .unwrap();
    accounts.send(OpenAccount { id: 11, customer: 1 }).await.unwrap();
    accounts
        .send(Deposit {
            account: 11,
            amount: 250,
        })
        .await
        .unwrap();
    accounts
        .send(Withdraw {
            account: 10,
            amount: 100,
        })
        .await
        .unwrap();
    bank.caught_up().await;

    let ana = portfolio(&bank, 1).await.unwrap();
    assert_eq!(ana.name, "Ana Maria", "the rename, from the other log");
    assert_eq!(ana.accounts.into_iter().collect::<Vec<_>>(), [(10, 400), (11, 250)]);
    assert_eq!(portfolio(&bank, 2).await, None);
    bank.stop().await;
}

#[tokio::test]
async fn the_rules_refuse_and_nothing_is_committed() {
    let bank = Bank::start();
    let (customers, accounts) = (&bank.customers, &bank.accounts);
    assert_eq!(
        accounts.send(OpenAccount { id: 10, customer: 1 }).await,
        Err(BankError::NoSuchCustomer(1))
    );
    customers
        .send(RegisterCustomer {
            id: 1,
            name: "Ana".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        customers
            .send(RegisterCustomer {
                id: 1,
                name: "Bia".into()
            })
            .await,
        Err(BankError::AlreadyExists)
    );
    assert_eq!(
        customers
            .send(RegisterCustomer {
                id: 2,
                name: "  ".into()
            })
            .await,
        Err(BankError::BlankName)
    );
    accounts.send(OpenAccount { id: 10, customer: 1 }).await.unwrap();
    accounts
        .send(Deposit {
            account: 10,
            amount: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        accounts
            .send(Withdraw {
                account: 10,
                amount: 101
            })
            .await,
        Err(BankError::InsufficientFunds {
            balance: 100,
            requested: 101
        })
    );
    assert_eq!(
        accounts.send(Deposit { account: 99, amount: 1 }).await,
        Err(BankError::NoSuchAccount(99))
    );
    bank.caught_up().await;
    let ana = portfolio(&bank, 1).await.unwrap();
    assert_eq!((ana.name.as_str(), ana.total()), ("Ana", 100));
    bank.stop().await;
}

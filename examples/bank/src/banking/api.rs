//! The bank's REST API, over axum.
//!
//! | Route | Body | Command or query |
//! |---|---|---|
//! | `POST /customers/{id}` | `{"name": "Ana"}` | [`RegisterCustomer`] |
//! | `PUT /customers/{id}/name` | `{"name": "Ana Maria"}` | [`RenameCustomer`] |
//! | `GET /customers/{id}/portfolio` | | [`PortfolioOf`] |
//! | `POST /accounts/{id}` | `{"customer": 1}` | [`OpenAccount`] |
//! | `POST /accounts/{id}/deposits` | `{"amount": 25000}` | [`Deposit`] |
//! | `POST /accounts/{id}/withdrawals` | `{"amount": 15000}` | [`Withdraw`] |
//! | `GET /health` | | |
//!
//! One route per command, each handler sending its command through the
//! gateway: no command enum for transport. Amounts are in cents. An
//! `x-correlation-id` header is stored with the events a command causes,
//! and travels with them to RabbitMQ.
//!
//! A command answers `201 Created` (a new customer or account) or `204 No
//! Content`; a refusal, `404` (no such customer or account), `409` (the
//! number is taken) or `422` (the rules), with `{"error": "..."}`.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use cqrs::{command::CommandGateway, event::Metadata, query::QueryGateway};
use serde::{Deserialize, Serialize};

use super::{
    Bank,
    commands::{Deposit, OpenAccount, RegisterCustomer, RenameCustomer, Withdraw},
    domain::BankError,
    portfolio::PortfolioOf,
};

/// The request header whose value becomes the events' `correlation_id`.
pub const CORRELATION_HEADER: &str = "x-correlation-id";

/// The bank's routes, over `bank`.
pub fn router(bank: Arc<Bank>) -> Router {
    Router::new()
        .route("/customers/{id}", post(register_customer))
        .route("/customers/{id}/name", put(rename_customer))
        .route("/customers/{id}/portfolio", get(portfolio))
        .route("/accounts/{id}", post(open_account))
        .route("/accounts/{id}/deposits", post(deposit))
        .route("/accounts/{id}/withdrawals", post(withdraw))
        .route("/health", get(|| async { "ok" }))
        .with_state(bank)
}

/// A customer's name.
#[derive(Debug, Deserialize)]
pub struct Name {
    /// The name.
    pub name: String,
}

/// An account's owner.
#[derive(Debug, Deserialize)]
pub struct Owner {
    /// The customer.
    pub customer: u32,
}

/// An amount of money.
#[derive(Debug, Deserialize)]
pub struct Amount {
    /// In cents.
    pub amount: u64,
}

/// A portfolio, as the API returns it.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortfolioView {
    /// The customer.
    pub customer: u32,
    /// Their current name.
    pub name: String,
    /// Their accounts.
    pub accounts: Vec<AccountView>,
    /// Everything they have, in cents.
    pub total: u64,
}

/// An account in a portfolio.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountView {
    /// Its number.
    pub id: u32,
    /// Its balance, in cents.
    pub balance: u64,
}

async fn register_customer(
    State(bank): State<Arc<Bank>>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    Json(body): Json<Name>,
) -> Result<StatusCode, ApiError> {
    let command = RegisterCustomer { id, name: body.name };
    bank.customers.send_with(command, metadata(&headers)).await?;
    Ok(StatusCode::CREATED)
}

async fn rename_customer(
    State(bank): State<Arc<Bank>>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    Json(body): Json<Name>,
) -> Result<StatusCode, ApiError> {
    let command = RenameCustomer { id, name: body.name };
    bank.customers.send_with(command, metadata(&headers)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn open_account(
    State(bank): State<Arc<Bank>>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    Json(body): Json<Owner>,
) -> Result<StatusCode, ApiError> {
    let command = OpenAccount {
        id,
        customer: body.customer,
    };
    bank.accounts.send_with(command, metadata(&headers)).await?;
    Ok(StatusCode::CREATED)
}

async fn deposit(
    State(bank): State<Arc<Bank>>,
    Path(account): Path<u32>,
    headers: HeaderMap,
    Json(body): Json<Amount>,
) -> Result<StatusCode, ApiError> {
    let command = Deposit {
        account,
        amount: body.amount,
    };
    bank.accounts.send_with(command, metadata(&headers)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn withdraw(
    State(bank): State<Arc<Bank>>,
    Path(account): Path<u32>,
    headers: HeaderMap,
    Json(body): Json<Amount>,
) -> Result<StatusCode, ApiError> {
    let command = Withdraw {
        account,
        amount: body.amount,
    };
    bank.accounts.send_with(command, metadata(&headers)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// The customer's portfolio. It is projected asynchronously: right after a
/// command, it may not show it yet.
async fn portfolio(State(bank): State<Arc<Bank>>, Path(customer): Path<u32>) -> Result<Json<PortfolioView>, ApiError> {
    let portfolio = bank
        .queries
        .query(PortfolioOf { customer })
        .await?
        .ok_or(BankError::NoSuchCustomer(customer))?;
    Ok(Json(PortfolioView {
        customer,
        total: portfolio.total(),
        name: portfolio.name,
        accounts: portfolio
            .accounts
            .into_iter()
            .map(|(id, balance)| AccountView { id, balance })
            .collect(),
    }))
}

/// The metadata stored with the events a request causes.
fn metadata(headers: &HeaderMap) -> Metadata {
    headers
        .get(CORRELATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|id| Metadata::from([("correlation_id", id)]))
        .unwrap_or_default()
}

/// A refusal, as an HTTP response.
#[derive(Debug)]
pub struct ApiError(BankError);

impl From<BankError> for ApiError {
    fn from(error: BankError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            BankError::NoSuchCustomer(_) | BankError::NoSuchAccount(_) => StatusCode::NOT_FOUND,
            BankError::AlreadyExists => StatusCode::CONFLICT,
            BankError::BlankName | BankError::NothingToMove | BankError::InsufficientFunds { .. } => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            BankError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({ "error": self.0.to_string() }))).into_response()
    }
}

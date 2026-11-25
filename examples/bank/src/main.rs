//! The bank and the compliance service, each a REST API of its own, on one
//! broker.
//!
//! ```text
//! docker run -d --name rabbit -p 5672:5672 rabbitmq:4-alpine
//! cargo run -p bank        # BANK_AMQP_URL, BANK_ADDR, COMPLIANCE_ADDR to change
//!
//! curl -X POST localhost:8080/customers/1 -H 'content-type: application/json' -d '{"name": "Ana"}'
//! curl -X POST localhost:8080/accounts/10 -H 'content-type: application/json' -d '{"customer": 1}'
//! curl -X POST localhost:8080/accounts/10/deposits -H 'content-type: application/json' -d '{"amount": 250000}'
//! curl -X POST localhost:8080/accounts/10/withdrawals -H 'content-type: application/json' \
//!      -H 'x-correlation-id: req-42' -d '{"amount": 150000}'
//! curl localhost:8080/customers/1/portfolio
//! curl localhost:8081/alerts
//! ```

use std::{future::Future, sync::Arc};

use bank::{
    alerts::{self, Compliance},
    banking::{self, Bank},
};
use cqrs_amqp::Amqp;
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;

/// From how much a withdrawal is reported, in cents (1000.00).
const LARGE: u64 = 100_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let url = std::env::var("BANK_AMQP_URL").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".to_owned());
    let amqp = Amqp::connect(&url).await?;

    // The compliance service first, so its queue is bound before the bank
    // publishes.
    let compliance = Arc::new(Compliance::start(&amqp, banking::ACCOUNTS_TOPIC, alerts::QUEUE, LARGE).await?);
    let mut bank = Bank::start();
    bank.forward_accounts(&amqp, banking::ACCOUNTS_TOPIC).await?;
    let bank = Arc::new(bank);

    let bank_listener = TcpListener::bind(address("BANK_ADDR", "127.0.0.1:8080")).await?;
    let compliance_listener = TcpListener::bind(address("COMPLIANCE_ADDR", "127.0.0.1:8081")).await?;
    tracing::info!("bank on http://{}", bank_listener.local_addr()?);
    tracing::info!("compliance on http://{}", compliance_listener.local_addr()?);

    // Ctrl-C stops both servers.
    let (stop, stopping) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = stop.send(true);
    });
    let until_stopped = |mut stopping: watch::Receiver<bool>| async move {
        let _ = stopping.wait_for(|stop| *stop).await;
    };
    let (served_bank, served_compliance) = tokio::join!(
        serve(
            bank_listener,
            banking::api::router(Arc::clone(&bank)),
            until_stopped(stopping.clone())
        ),
        serve(
            compliance_listener,
            alerts::api::router(Arc::clone(&compliance)),
            until_stopped(stopping)
        ),
    );
    served_bank?;
    served_compliance?;

    tracing::info!("shutting down");
    // The routers are gone with their servers: the processors and the
    // subscription can be stopped, finishing what they hold.
    if let Some(bank) = Arc::into_inner(bank) {
        bank.stop().await;
    }
    if let Some(compliance) = Arc::into_inner(compliance) {
        compliance.stop().await?;
    }
    Ok(())
}

/// Serves `router` on `listener` until `shutdown`.
async fn serve(
    listener: TcpListener,
    router: axum::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router).with_graceful_shutdown(shutdown).await
}

/// The address in the environment variable `variable`, or `default`.
fn address(variable: &str, default: &str) -> String {
    std::env::var(variable).unwrap_or_else(|_| default.to_owned())
}

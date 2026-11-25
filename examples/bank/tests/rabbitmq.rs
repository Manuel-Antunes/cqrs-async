//! The two applications through a real RabbitMQ, over their REST APIs: set
//! `CQRS_AMQP_URL` (skipped otherwise).
//!
//! ```text
//! docker run -d --name cqrs-rabbit -p 55672:5672 rabbitmq:4-alpine
//! CQRS_AMQP_URL=amqp://127.0.0.1:55672/%2f cargo test -p bank
//! ```

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bank::{
    alerts::{self, AlertError, AlertView, Compliance, RaiseAlert},
    banking::{self, Bank},
};
use cqrs::command::CommandGateway;
use cqrs_amqp::Amqp;
use serde_json::{Value, json};
use tower::ServiceExt;

/// Sends `method uri` with `body`, and `correlation` as the correlation id:
/// the status and the response's JSON (`null` when empty).
async fn call(router: &Router, method: &str, uri: &str, body: Option<Value>, correlation: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header(banking::api::CORRELATION_HEADER, correlation)
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, json)
}

#[tokio::test]
async fn large_withdrawals_reach_the_compliance_service_once() {
    let Ok(url) = std::env::var("CQRS_AMQP_URL") else {
        eprintln!("CQRS_AMQP_URL not set: skipped");
        return;
    };
    let amqp = Amqp::connect(&url).await.unwrap();
    // Names of this run's own, deleted at the end.
    let run = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
    let (topic, queue) = (
        format!("bank-test-{run}.accounts"),
        format!("bank-test-{run}.compliance"),
    );

    let compliance = Arc::new(Compliance::start(&amqp, &topic, &queue, 1_000).await.unwrap());
    let mut bank = Bank::start();
    bank.forward_accounts(&amqp, &topic).await.unwrap();
    let bank = Arc::new(bank);
    let (bank_api, compliance_api) = (
        banking::api::router(Arc::clone(&bank)),
        alerts::api::router(Arc::clone(&compliance)),
    );

    let post = async |uri: &str, body: Value, correlation: &str| {
        let (status, _) = call(&bank_api, "POST", uri, Some(body), correlation).await;
        assert!(status.is_success(), "{uri}: {status}");
    };
    post("/customers/1", json!({"name": "Ana"}), "c-1").await;
    post("/accounts/10", json!({"customer": 1}), "c-2").await;
    post("/accounts/10/deposits", json!({"amount": 5000}), "c-3").await;
    post("/accounts/10/withdrawals", json!({"amount": 999}), "c-4").await;
    post("/accounts/10/withdrawals", json!({"amount": 1000}), "c-5").await;
    post("/accounts/10/withdrawals", json!({"amount": 2500}), "c-6").await;
    bank.caught_up().await;

    let mut alerts = Vec::new();
    for _ in 0..100 {
        let (_, body) = call(&compliance_api, "GET", "/alerts", None, "").await;
        alerts = serde_json::from_value::<Vec<AlertView>>(body).unwrap();
        if alerts.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let alert = |amount, correlation: &str| AlertView {
        account: 10,
        amount,
        correlation_id: Some(correlation.to_owned()),
    };
    assert_eq!(
        alerts,
        [alert(1000, "c-5"), alert(2500, "c-6")],
        "the two large ones, in order, with the bank request's correlation id"
    );

    // Delivered again (at least once): the alert is not raised twice. The
    // withdrawal of 1000 is the log's fifth event.
    let again = compliance
        .gateway
        .send(RaiseAlert {
            source: 5,
            account: 10,
            amount: 1000,
        })
        .await;
    assert_eq!(again, Err(AlertError::AlreadyRaised));
    assert_eq!(compliance.alerts().await.len(), 2);

    drop((bank_api, compliance_api));
    Arc::into_inner(bank).unwrap().stop().await;
    Arc::into_inner(compliance).unwrap().stop().await.unwrap();
    let channel = amqp.connection().create_channel().await.unwrap();
    let _ = channel
        .queue_delete(
            queue.as_str().into(),
            cqrs_amqp::lapin::options::QueueDeleteOptions::default(),
        )
        .await;
    let _ = channel
        .exchange_delete(
            topic.as_str().into(),
            cqrs_amqp::lapin::options::ExchangeDeleteOptions::default(),
        )
        .await;
}

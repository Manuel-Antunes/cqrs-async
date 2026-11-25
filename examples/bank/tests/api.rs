//! The bank's REST API, request by request (no socket, no broker).

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bank::banking::{
    Bank,
    api::{self, AccountView, PortfolioView},
};
use serde_json::{Value, json};
use tower::ServiceExt;

/// Sends `method uri` with `body` (JSON) to `router`: the status and the
/// response's JSON (`null` when empty).
async fn call(router: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header(api::CORRELATION_HEADER, "test-1")
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

/// The portfolio once the projection shows `total` (it is asynchronous).
async fn portfolio_with(router: &Router, customer: u32, total: u64) -> PortfolioView {
    for _ in 0..100 {
        let (status, body) = call(router, "GET", &format!("/customers/{customer}/portfolio"), None).await;
        if status == StatusCode::OK {
            let portfolio: PortfolioView = serde_json::from_value(body).unwrap();
            if portfolio.total == total {
                return portfolio;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the portfolio of {customer} never reached {total}");
}

#[tokio::test]
async fn commands_and_the_portfolio_over_http() {
    let bank = Arc::new(Bank::start());
    let router = api::router(Arc::clone(&bank));

    assert_eq!(call(&router, "GET", "/health", None).await.0, StatusCode::OK);
    let created = |status: (StatusCode, Value)| assert_eq!(status, (StatusCode::CREATED, Value::Null));
    let done = |status: (StatusCode, Value)| assert_eq!(status, (StatusCode::NO_CONTENT, Value::Null));
    created(call(&router, "POST", "/customers/1", Some(json!({"name": "Ana"}))).await);
    created(call(&router, "POST", "/accounts/10", Some(json!({"customer": 1}))).await);
    done(call(&router, "POST", "/accounts/10/deposits", Some(json!({"amount": 2500}))).await);
    done(
        call(
            &router,
            "POST",
            "/accounts/10/withdrawals",
            Some(json!({"amount": 500})),
        )
        .await,
    );
    done(call(&router, "PUT", "/customers/1/name", Some(json!({"name": "Ana Maria"}))).await);

    let portfolio = portfolio_with(&router, 1, 2000).await;
    assert_eq!(
        portfolio,
        PortfolioView {
            customer: 1,
            name: "Ana Maria".into(),
            accounts: vec![AccountView { id: 10, balance: 2000 }],
            total: 2000,
        }
    );
}

#[tokio::test]
async fn refusals_have_their_status_and_reason() {
    let bank = Arc::new(Bank::start());
    let router = api::router(Arc::clone(&bank));
    call(&router, "POST", "/customers/1", Some(json!({"name": "Ana"}))).await;
    call(&router, "POST", "/accounts/10", Some(json!({"customer": 1}))).await;

    let refused = |status, error: &str| (status, json!({ "error": error }));
    assert_eq!(
        call(&router, "POST", "/customers/1", Some(json!({"name": "Bia"}))).await,
        refused(StatusCode::CONFLICT, "that number is taken")
    );
    assert_eq!(
        call(&router, "POST", "/accounts/11", Some(json!({"customer": 9}))).await,
        refused(StatusCode::NOT_FOUND, "no customer 9")
    );
    assert_eq!(
        call(&router, "POST", "/accounts/10/withdrawals", Some(json!({"amount": 1}))).await,
        refused(StatusCode::UNPROCESSABLE_ENTITY, "1 requested, 0 available")
    );
    assert_eq!(
        call(&router, "POST", "/customers/2", Some(json!({"name": " "}))).await,
        refused(StatusCode::UNPROCESSABLE_ENTITY, "a name cannot be blank")
    );
    assert_eq!(
        call(&router, "GET", "/customers/7/portfolio", None).await,
        refused(StatusCode::NOT_FOUND, "no customer 7")
    );
    // A body that is not the command's is axum's to refuse.
    assert_eq!(
        call(&router, "POST", "/accounts/10/deposits", Some(json!({"cents": 5})))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

//! The compliance service's REST API, over axum: what it raised.
//!
//! | Route | |
//! |---|---|
//! | `GET /alerts` | every alert raised, in order: `[{"account": 20, "amount": 150000, "correlation_id": "..."}]` |
//! | `GET /health` | |

use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};

use super::{AlertView, Compliance};

/// The service's routes, over `compliance`.
pub fn router(compliance: Arc<Compliance>) -> Router {
    Router::new()
        .route("/alerts", get(alerts))
        .route("/health", get(|| async { "ok" }))
        .with_state(compliance)
}

async fn alerts(State(compliance): State<Arc<Compliance>>) -> Json<Vec<AlertView>> {
    Json(compliance.alerts().await)
}

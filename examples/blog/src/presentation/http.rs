//! The HTTP surface:
//!
//! | Route | |
//! |---|---|
//! | `POST /graphql` | queries and mutations; an `x-correlation-id` header is stored with the events they cause |
//! | `GET /graphql` | GraphiQL, to explore the API in a browser |
//! | `GET /graphql/ws` | subscriptions (`graphql-transport-ws`, also `graphql-ws`) |
//! | `GET /health` | liveness |

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    Router,
    extract::State,
    http::HeaderMap,
    response::{Html, IntoResponse},
    routing::get,
};
use cqrs::Metadata;

use super::{BlogApi, graphql::BlogSchema};

/// Where queries and mutations are posted.
pub const GRAPHQL_PATH: &str = "/graphql";

/// Where subscriptions connect.
pub const SUBSCRIPTIONS_PATH: &str = "/graphql/ws";

/// The request header whose value becomes the events' `correlation_id`.
pub const CORRELATION_HEADER: &str = "x-correlation-id";

/// The router serving `schema`.
pub fn router<A: BlogApi>(schema: BlogSchema<A>) -> Router {
    Router::new()
        .route(GRAPHQL_PATH, get(graphiql).post(execute::<A>))
        .route_service(SUBSCRIPTIONS_PATH, GraphQLSubscription::new(schema.clone()))
        .route("/health", get(|| async { "ok" }))
        .with_state(schema)
}

/// Runs a query or mutation, handing the request's metadata to the
/// resolvers, which send it along with their commands.
async fn execute<A: BlogApi>(
    State(schema): State<BlogSchema<A>>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let mut metadata = Metadata::new();
    if let Some(id) = headers.get(CORRELATION_HEADER).and_then(|value| value.to_str().ok()) {
        metadata.insert("correlation_id".to_owned(), id.to_owned());
    }
    schema.execute(request.into_inner().data(metadata)).await.into()
}

/// The GraphiQL page, wired to both endpoints.
async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint(GRAPHQL_PATH)
            .subscription_endpoint(SUBSCRIPTIONS_PATH)
            .finish(),
    )
}

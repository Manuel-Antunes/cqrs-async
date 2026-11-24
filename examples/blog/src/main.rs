//! The blog as a GraphQL server.
//!
//! ```text
//! cargo run -p blog                 # in memory, on 127.0.0.1:8000 (BLOG_ADDR to change)
//! BLOG_DATABASE_URL=postgres://... cargo run -p blog   # on PostgreSQL, with SeaORM
//! cargo run -p blog -- --schema     # prints the schema (SDL) and exits
//! ```
//!
//! Then open <http://127.0.0.1:8000/graphql> for GraphiQL.

use std::sync::Arc;

use blog::{
    Running,
    presentation::{BlogApi, graphql, http},
};
use cqrs_postgres::sqlx::PgPool;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    match std::env::var("BLOG_DATABASE_URL") {
        Ok(url) => {
            tracing::info!("on PostgreSQL");
            serve(blog::start(PgPool::connect(&url).await?).await?).await
        }
        Err(_) => {
            tracing::info!("in memory (set BLOG_DATABASE_URL for PostgreSQL)");
            serve(blog::start_in_memory()).await
        }
    }
}

/// Serves `running` until Ctrl-C, then stops its processors.
async fn serve<C, Q, R, E>(running: Running<C, Q, R, E>) -> Result<(), Box<dyn std::error::Error>>
where
    Arc<cqrs::Cqrs<C, Q, E>>: BlogApi,
{
    let schema = graphql::schema(Arc::clone(&running.app));
    if std::env::args().any(|arg| arg == "--schema") {
        println!("{}", schema.sdl());
        running.stop().await;
        return Ok(());
    }

    let address = std::env::var("BLOG_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_owned());
    let listener = TcpListener::bind(&address).await?;
    tracing::info!("GraphiQL on http://{}{}", listener.local_addr()?, http::GRAPHQL_PATH);

    axum::serve(listener, http::router(schema))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    tracing::info!("shutting down");
    running.stop().await;
    Ok(())
}

//! The blog as a GraphQL server.
//!
//! ```text
//! cargo run -p blog                 # serves on 127.0.0.1:8000 (BLOG_ADDR to change)
//! cargo run -p blog -- --schema     # prints the schema (SDL) and exits
//! ```
//!
//! Then open <http://127.0.0.1:8000/graphql> for GraphiQL.

use blog::presentation::{graphql, http};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let running = blog::start();
    let schema = graphql::schema(std::sync::Arc::clone(&running.app));

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

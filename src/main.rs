use clap::Parser;
use std::sync::Arc;
use tokio::signal;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod api;
mod config;
mod db;
mod error;
mod metrics;
mod watchdog;
mod worker;

use crate::config::{AppConfig, Cli};

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: AppConfig,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let cfg = AppConfig::load(&cli.config_path)
        .expect("Failed to load configuration");

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(fmt::layer().json())
        .init();

    tracing::info!(config_path = %cli.config_path, "RustyQueue starting");

    let pool = db::init_pool(&cfg.database).await
        .expect("Failed to initialize database pool");

    db::run_migrations(&pool).await
        .expect("Failed to run database migrations");

    metrics::register_metrics();

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let state = Arc::new(AppState {
        pool: pool.clone(),
        config: cfg.clone(),
        shutdown_tx: shutdown_tx.clone(),
    });

    let worker_handle = worker::start(state.clone()).await;
    let watchdog_handle = watchdog::start(state.clone()).await;

    let addr = format!("{}:{}", cfg.server.host, cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(address = %addr, "HTTP server listening");

    let router = api::build_router(state.clone());

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx.clone()))
        .await?;

    tracing::info!("HTTP server stopped; waiting for workers (30s grace period)");
    let _ = shutdown_tx.send(());

    let grace = tokio::time::Duration::from_secs(30);
    let _ = tokio::time::timeout(grace, worker_handle).await;
    let _ = tokio::time::timeout(grace, watchdog_handle).await;

    tracing::info!("RustyQueue shutdown complete");
    Ok(())
}

async fn shutdown_signal(tx: tokio::sync::broadcast::Sender<()>) {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received");
    let _ = tx.send(());
}

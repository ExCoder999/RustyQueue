use clap::Parser;
use rustyqueue::{api, config::{AppConfig, Cli}, db, metrics, watchdog, worker, AppState};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{signal, time::Duration};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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
    let circuit_open = Arc::new(AtomicBool::new(false));

    let state = Arc::new(AppState {
        pool: pool.clone(),
        config: cfg.clone(),
        shutdown_tx: shutdown_tx.clone(),
        circuit_open: circuit_open.clone(),
        task_cancel_tokens: Default::default(),
    });

    start_circuit_breaker_monitor(pool.clone(), circuit_open, shutdown_tx.clone());

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

    let grace = Duration::from_secs(30);
    let _ = tokio::time::timeout(grace, worker_handle).await;
    let _ = tokio::time::timeout(grace, watchdog_handle).await;

    tracing::info!("RustyQueue shutdown complete");
    Ok(())
}

fn start_circuit_breaker_monitor(
    pool: sqlx::PgPool,
    flag: Arc<AtomicBool>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
) {
    tokio::spawn(async move {
        let mut saturation_since: Option<std::time::Instant> = None;
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let mut shutdown_rx = shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown_rx.recv() => return,
            }

            let reachable = tokio::time::timeout(
                Duration::from_millis(200),
                pool.acquire(),
            )
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);

            if reachable {
                if saturation_since.take().is_some() && flag.load(Ordering::Relaxed) {
                    tracing::info!("DB pool recovered — closing circuit breaker");
                    flag.store(false, Ordering::Relaxed);
                }
            } else {
                let since = saturation_since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() > Duration::from_secs(5) && !flag.load(Ordering::Relaxed) {
                    tracing::warn!("DB pool unreachable >5 s — opening circuit breaker");
                    flag.store(true, Ordering::Relaxed);
                }
            }
        }
    });
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

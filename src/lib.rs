pub mod api;
pub mod config;
pub mod db;
pub mod error;
pub mod metrics;
pub mod middleware;
pub mod watchdog;
pub mod worker;

use std::sync::{atomic::AtomicBool, Arc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: config::AppConfig,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Flipped by the circuit-breaker monitor after >5 s of DB pool saturation.
    pub circuit_open: Arc<AtomicBool>,
    /// Per-task cancellation tokens. The executor registers a token on start
    /// and the cancel endpoint cancels it for best-effort in-process abort.
    pub task_cancel_tokens: dashmap::DashMap<Uuid, CancellationToken>,
}

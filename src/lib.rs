pub mod api;
pub mod config;
pub mod db;
pub mod error;
pub mod metrics;
pub mod middleware;
pub mod watchdog;
pub mod worker;

use std::sync::{atomic::AtomicBool, Arc};

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: config::AppConfig,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    pub circuit_open: Arc<AtomicBool>,
}

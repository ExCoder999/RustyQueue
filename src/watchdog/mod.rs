use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};

use crate::db::queries::{move_exceeded_retries_to_dlq, reset_stuck_leases};
use crate::AppState;

pub async fn start(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        let mut shutdown_rx = state.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown_rx.recv() => {
                    tracing::info!("Watchdog shutting down");
                    return;
                }
            }

            match reset_stuck_leases(&state.pool).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(count = n, "Watchdog reset stuck leases"),
                Err(e) => tracing::error!(error = %e, "Watchdog failed to reset stuck leases"),
            }

            match move_exceeded_retries_to_dlq(&state.pool).await {
                Ok(0) => {}
                Ok(n) => tracing::warn!(count = n, "Watchdog moved tasks to DLQ"),
                Err(e) => tracing::error!(error = %e, "Watchdog failed to move tasks to DLQ"),
            }
        }
    })
}

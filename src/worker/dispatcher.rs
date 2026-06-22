use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::db::{models::DbTask, queries::fetch_pending_task};
use crate::AppState;

pub async fn dispatch_loop(state: Arc<AppState>, queue: String, tx: mpsc::Sender<DbTask>) {
    let poll_interval = Duration::from_millis(state.config.queue.poll_interval_ms);
    let lease_secs = state.config.queue.default_lease_seconds as i64;
    let mut shutdown_rx = state.shutdown_tx.subscribe();
    let mut backoff = Duration::from_secs(1);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                tracing::info!(queue = %queue, "Dispatcher shutting down");
                return;
            }
            _ = sleep(poll_interval) => {}
        }

        match fetch_pending_task(&state.pool, &queue, lease_secs).await {
            Ok(Some(task)) => {
                backoff = Duration::from_secs(1); // reset on success
                tracing::debug!(task_id = %task.id, queue = %queue, "Dispatching task");
                if tx.send(task).await.is_err() {
                    tracing::warn!(queue = %queue, "Worker channel closed, stopping dispatcher");
                    return;
                }
            }
            Ok(None) => {
                // Queue empty; no backoff needed
            }
            Err(crate::error::AppError::Database(ref e))
                if matches!(e, sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed) =>
            {
                tracing::error!(
                    queue = %queue,
                    backoff_secs = backoff.as_secs(),
                    "DB pool exhausted, backing off"
                );
                tokio::select! {
                    _ = shutdown_rx.recv() => return,
                    _ = sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            Err(e) => {
                tracing::error!(queue = %queue, error = %e, backoff_secs = backoff.as_secs(), "Dispatcher DB error, backing off");
                tokio::select! {
                    _ = shutdown_rx.recv() => return,
                    _ = sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

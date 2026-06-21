pub mod dispatcher;
pub mod executor;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::db::models::DbTask;
use crate::db::queries::get_queue_length;
use crate::metrics::QUEUE_LENGTH;
use crate::AppState;

use dispatcher::dispatch_loop;
use executor::execute_task;

// For now we hardcode the queue name; in a multi-queue setup, this
// would be driven by config or DB discovery.
const DEFAULT_QUEUE: &str = "default";

pub async fn start(state: Arc<AppState>) -> JoinHandle<()> {
    let num_workers = state.config.worker.num_workers_per_queue;

    // Channel sized to num_workers so dispatch never gets too far ahead
    let (tx, rx) = mpsc::channel::<DbTask>(num_workers);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    // Spawn the dispatcher
    let disp_state = state.clone();
    tokio::spawn(dispatch_loop(disp_state, DEFAULT_QUEUE.to_string(), tx));

    // Spawn N workers
    for worker_id in 0..num_workers {
        let rx = rx.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let mut shutdown_rx = state.shutdown_tx.subscribe();
            loop {
                let task = {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        msg = guard.recv() => msg,
                        _ = shutdown_rx.recv() => {
                            tracing::info!(worker_id, "Worker shutting down");
                            return;
                        }
                    }
                };
                match task {
                    Some(t) => {
                        tracing::debug!(worker_id, task_id = %t.id, "Worker picked up task");
                        execute_task(state.clone(), t).await;
                    }
                    None => {
                        tracing::info!(worker_id, "Task channel closed, worker exiting");
                        return;
                    }
                }
            }
        });
    }

    // Background queue-length gauge poller (every 10s)
    let gauge_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        let mut shutdown_rx = gauge_state.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown_rx.recv() => return,
            }
            match get_queue_length(&gauge_state.pool).await {
                Ok(len) => QUEUE_LENGTH.set(len as f64),
                Err(e) => tracing::warn!(error = %e, "Failed to poll queue length"),
            }
        }
    });

    // Return a join handle that resolves when shutdown is signalled
    let state = state.clone();
    tokio::spawn(async move {
        let mut rx = state.shutdown_tx.subscribe();
        let _ = rx.recv().await;
    })
}

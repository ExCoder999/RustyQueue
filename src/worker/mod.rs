pub mod dispatcher;
pub mod executor;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::db::models::DbTask;
use crate::db::queries::get_queue_length_by_queue;
use crate::metrics::QUEUE_LENGTH;
use crate::AppState;

use dispatcher::dispatch_loop;
use executor::execute_task;

pub async fn start(state: Arc<AppState>) -> JoinHandle<()> {
    let queues = state.config.worker.queues.clone();
    let num_workers = state.config.worker.num_workers_per_queue;

    for queue in queues {
        spawn_queue_workers(state.clone(), queue, num_workers);
    }

    spawn_queue_length_poller(state.clone());

    // Return a handle that resolves on shutdown
    let mut rx = state.shutdown_tx.subscribe();
    tokio::spawn(async move {
        let _ = rx.recv().await;
    })
}

fn spawn_queue_workers(state: Arc<AppState>, queue: String, num_workers: usize) {
    // Channel depth = num_workers: dispatcher stays at most one batch ahead
    let (tx, rx) = mpsc::channel::<DbTask>(num_workers);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    // One dispatcher per queue
    tokio::spawn(dispatch_loop(state.clone(), queue.clone(), tx));

    // N workers sharing the same receiver
    for worker_id in 0..num_workers {
        let rx = rx.clone();
        let state = state.clone();
        let queue = queue.clone();
        tokio::spawn(async move {
            let mut shutdown_rx = state.shutdown_tx.subscribe();
            tracing::info!(worker_id, queue = %queue, "Worker started");
            loop {
                let task = {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        msg = guard.recv() => msg,
                        _ = shutdown_rx.recv() => {
                            tracing::info!(worker_id, queue = %queue, "Worker shutting down");
                            return;
                        }
                    }
                };
                match task {
                    Some(t) => execute_task(state.clone(), t).await,
                    None => {
                        tracing::info!(worker_id, queue = %queue, "Channel closed, worker exiting");
                        return;
                    }
                }
            }
        });
    }
}

fn spawn_queue_length_poller(state: Arc<AppState>) {
    let queues = state.config.worker.queues.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        let mut shutdown_rx = state.shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown_rx.recv() => return,
            }
            for queue in &queues {
                match get_queue_length_by_queue(&state.pool, queue).await {
                    Ok(len) => QUEUE_LENGTH.with_label_values(&[queue]).set(len as f64),
                    Err(e) => {
                        tracing::warn!(error = %e, queue = %queue, "Failed to poll queue length")
                    }
                }
            }
        }
    });
}

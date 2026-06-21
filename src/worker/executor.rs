use std::sync::Arc;
use tokio::process::Command;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use crate::db::{
    models::DbTask,
    queries::{increment_retry, is_task_cancelled, mark_complete, mark_failed, move_to_dlq, update_heartbeat},
};
use crate::metrics::{ACTIVE_WORKERS, TASKS_DURATION};
use crate::AppState;

pub async fn execute_task(state: Arc<AppState>, task: DbTask) {
    let task_id = task.id;
    let queue = task.queue.clone();
    let max_timeout = Duration::from_secs(state.config.queue.max_command_timeout_seconds);
    let pool = state.pool.clone();

    // Register a cancellation token so the cancel endpoint can abort this
    // specific task without broadcasting a global shutdown signal.
    let cancel_token = CancellationToken::new();
    state.task_cancel_tokens.insert(task_id, cancel_token.clone());

    ACTIVE_WORKERS.inc();
    let start = std::time::Instant::now();

    tracing::info!(task_id = %task_id, queue = %queue, event = "Leased", "Task execution started");

    let result = timeout(max_timeout, run_command(&state, &task, cancel_token)).await;

    // Always deregister — cancel endpoint may have already removed it, that's fine.
    state.task_cancel_tokens.remove(&task_id);
    ACTIVE_WORKERS.dec();
    let elapsed = start.elapsed().as_secs_f64();

    match result {
        Ok(Ok(())) => {
            TASKS_DURATION
                .with_label_values(&[&queue, "completed"])
                .observe(elapsed);
            if let Err(e) = mark_complete(&pool, task_id).await {
                tracing::error!(task_id = %task_id, error = %e, "Failed to mark task complete");
            } else {
                tracing::info!(task_id = %task_id, queue = %queue, event = "Completed", "Task completed");
            }
        }
        Ok(Err(err)) => {
            TASKS_DURATION
                .with_label_values(&[&queue, "failed"])
                .observe(elapsed);
            handle_failure(&state, &task, &err.to_string()).await;
        }
        Err(_timeout) => {
            TASKS_DURATION
                .with_label_values(&[&queue, "timeout"])
                .observe(elapsed);
            handle_failure(&state, &task, "Task execution timed out").await;
        }
    }
}

async fn run_command(
    state: &Arc<AppState>,
    task: &DbTask,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let command_value = task
        .payload
        .get("command")
        .ok_or_else(|| anyhow::anyhow!("payload missing 'command' field"))?;

    let args: Vec<String> = serde_json::from_value(command_value.clone())
        .map_err(|e| anyhow::anyhow!("'command' must be an array of strings: {}", e))?;

    if args.is_empty() {
        return Err(anyhow::anyhow!("'command' array is empty"));
    }

    let program = &args[0];
    let rest = &args[1..];

    let mut child = Command::new(program)
        .args(rest)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to spawn process '{}': {}", program, e))?;

    let pool = state.pool.clone();
    let task_id = task.id;
    let mut shutdown_rx = state.shutdown_tx.subscribe();

    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Err(e) = update_heartbeat(&pool, task_id).await {
                tracing::warn!(task_id = %task_id, error = %e, "Failed to update heartbeat");
            }
        }
    });

    let exit_status = tokio::select! {
        status = child.wait() => {
            heartbeat_handle.abort();
            status.map_err(|e| anyhow::anyhow!("Process wait error: {}", e))?
        }
        // Process-wide graceful shutdown (SIGINT / SIGTERM)
        _ = shutdown_rx.recv() => {
            heartbeat_handle.abort();
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(anyhow::anyhow!("Task aborted: process shutting down"));
        }
        // Per-task cancellation triggered by POST /tasks/:id/cancel
        _ = cancel_token.cancelled() => {
            heartbeat_handle.abort();
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(anyhow::anyhow!("Task cancelled by user request"));
        }
    };

    if exit_status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Process exited with status: {}", exit_status))
    }
}

async fn handle_failure(state: &Arc<AppState>, task: &DbTask, error: &str) {
    let task_id = task.id;
    let pool = &state.pool;

    if is_task_cancelled(pool, task_id).await.unwrap_or(false) {
        tracing::info!(task_id = %task_id, "Task was cancelled, skipping retry");
        return;
    }

    let next_retries = task.retries + 1;
    if next_retries >= task.max_retries {
        tracing::warn!(
            task_id = %task_id,
            queue = %task.queue,
            retries = next_retries,
            event = "Failed",
            error = %error,
            "Task exceeded max retries, moving to DLQ"
        );
        if let Err(e) = mark_failed(pool, task_id, error).await {
            tracing::error!(task_id = %task_id, error = %e, "Failed to mark task as failed");
        }
        if let Err(e) = move_to_dlq(pool, task_id).await {
            tracing::error!(task_id = %task_id, error = %e, "Failed to move task to DLQ");
        }
    } else {
        tracing::warn!(
            task_id = %task_id,
            queue = %task.queue,
            retries = next_retries,
            max_retries = task.max_retries,
            event = "Failed",
            error = %error,
            "Task failed, scheduling retry"
        );
        if let Err(e) = increment_retry(pool, task_id, error).await {
            tracing::error!(task_id = %task_id, error = %e, "Failed to increment retry");
        }
    }
}

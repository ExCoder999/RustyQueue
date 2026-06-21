use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

use crate::api::extractors::BoundedJson;
use crate::db::{
    models::NewTask,
    queries::{
        cancel_task, get_idempotency_task_id, get_task_status, store_idempotency_key,
        check_pool_health, insert_task,
    },
};
use crate::error::{AppError, AppResult};
use crate::metrics::{gather_metrics, TASKS_ENQUEUED};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    pub queue: String,
    pub payload: serde_json::Value,
    #[serde(default = "default_max_retries")]
    pub max_retries: u8,
    #[serde(default)]
    pub delay_seconds: u64,
    #[serde(default)]
    pub priority: i32,
}

fn default_max_retries() -> u8 {
    3
}

#[derive(Debug, Serialize)]
pub struct EnqueueResponse {
    pub task_id: Uuid,
}

pub async fn enqueue_task(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    BoundedJson(req): BoundedJson<EnqueueRequest>,
) -> AppResult<impl IntoResponse> {
    if req.queue.is_empty() {
        return Err(AppError::BadRequest("queue name cannot be empty".to_string()));
    }

    // Idempotency check
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if let Some(ref key) = idempotency_key {
        let hash = format!("{:x}", Sha256::digest(key.as_bytes()));
        if let Some(existing_id) = get_idempotency_task_id(&state.pool, &hash).await? {
            return Err(AppError::Conflict(format!(
                "Duplicate submission: task {} already exists for this idempotency key",
                existing_id
            )));
        }
    }

    let scheduled_at = Utc::now()
        + chrono::Duration::seconds(req.delay_seconds as i64);

    let idempotency_hash = idempotency_key.as_ref().map(|k| {
        format!("{:x}", Sha256::digest(k.as_bytes()))
    });

    let new_task = NewTask {
        queue: req.queue.clone(),
        payload: req.payload,
        max_retries: req.max_retries as i16,
        priority: req.priority,
        scheduled_at,
        idempotency_key: idempotency_hash.clone(),
    };

    let task_id = insert_task(&state.pool, new_task).await?;

    if let Some(hash) = idempotency_hash {
        store_idempotency_key(&state.pool, &hash, task_id).await?;
    }

    TASKS_ENQUEUED.with_label_values(&[&req.queue]).inc();

    tracing::info!(
        task_id = %task_id,
        queue = %req.queue,
        event = "Enqueued",
        "Task enqueued"
    );

    Ok((StatusCode::ACCEPTED, Json(EnqueueResponse { task_id })))
}

pub async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let status = get_task_status(&state.pool, task_id).await?;
    Ok(Json(status))
}

pub async fn cancel_task_handler(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    // Signal via broadcast so inflight worker can abort
    let _ = state.shutdown_tx.send(());

    let cancelled = cancel_task(&state.pool, task_id).await?;
    if cancelled {
        tracing::info!(task_id = %task_id, event = "Cancelled", "Task cancelled");
        Ok((StatusCode::OK, Json(json!({ "cancelled": true }))))
    } else {
        Err(AppError::NotFound(format!(
            "Task {} not found or already terminal",
            task_id
        )))
    }
}

pub async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if check_pool_health(&state.pool).await {
        (StatusCode::OK, Json(json!({ "status": "ok" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unhealthy", "reason": "database unreachable" })),
        )
    }
}

pub async fn metrics_handler() -> impl IntoResponse {
    let body = gather_metrics();
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

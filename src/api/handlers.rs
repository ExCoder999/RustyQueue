use axum::{
    extract::{Path, Query, State},
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
        cancel_task, check_pool_health, count_dlq, count_tasks, get_idempotency_task_id,
        get_queue_stats, get_task_status, insert_task, insert_task_batch, list_dlq, list_tasks,
        purge_queue, requeue_all_dlq, requeue_dlq_task, store_idempotency_key,
    },
};
use crate::error::{AppError, AppResult};
use crate::metrics::{gather_metrics, TASKS_ENQUEUED};
use crate::AppState;

// ── Enqueue ───────────────────────────────────────────────────────────────────

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
        return Err(AppError::BadRequest(
            "queue name cannot be empty".to_string(),
        ));
    }

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

    let scheduled_at = Utc::now() + chrono::Duration::seconds(req.delay_seconds as i64);
    let idempotency_hash = idempotency_key
        .as_ref()
        .map(|k| format!("{:x}", Sha256::digest(k.as_bytes())));

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
    tracing::info!(task_id = %task_id, queue = %req.queue, event = "Enqueued", "Task enqueued");

    Ok((StatusCode::ACCEPTED, Json(EnqueueResponse { task_id })))
}

// ── Task status / cancel ──────────────────────────────────────────────────────

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
    if let Some((_, token)) = state.task_cancel_tokens.remove(&task_id) {
        token.cancel();
    }

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

// ── Task list ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TaskListQuery {
    pub queue: Option<String>,
    pub status: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

#[derive(Debug, Serialize)]
pub struct PagedResponse<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

pub async fn list_tasks_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TaskListQuery>,
) -> AppResult<impl IntoResponse> {
    let limit = q.limit.clamp(1, 200);
    let queue = q.queue.as_deref();
    let status = q.status.as_deref();

    let (items, total) = tokio::try_join!(
        list_tasks(&state.pool, queue, status, limit, q.offset),
        count_tasks(&state.pool, queue, status),
    )?;

    Ok(Json(PagedResponse {
        items,
        total,
        limit,
        offset: q.offset,
    }))
}

// ── Queue stats ───────────────────────────────────────────────────────────────

pub async fn list_queues_handler(
    State(state): State<Arc<AppState>>,
) -> AppResult<impl IntoResponse> {
    let stats = get_queue_stats(&state.pool).await?;
    Ok(Json(stats))
}

// ── DLQ ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct DlqListQuery {
    pub queue: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

pub async fn list_dlq_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DlqListQuery>,
) -> AppResult<impl IntoResponse> {
    let limit = q.limit.clamp(1, 200);
    let queue = q.queue.as_deref();

    let (items, total) = tokio::try_join!(
        list_dlq(&state.pool, queue, limit, q.offset),
        count_dlq(&state.pool, queue),
    )?;

    Ok(Json(PagedResponse {
        items,
        total,
        limit,
        offset: q.offset,
    }))
}

pub async fn requeue_dlq_handler(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let requeued = requeue_dlq_task(&state.pool, task_id).await?;
    if requeued {
        tracing::info!(task_id = %task_id, event = "Requeued", "DLQ task requeued");
        Ok((
            StatusCode::OK,
            Json(json!({ "task_id": task_id, "requeued": true })),
        ))
    } else {
        Err(AppError::NotFound(format!(
            "Task {} not found in the dead-letter queue",
            task_id
        )))
    }
}

// ── Batch enqueue ─────────────────────────────────────────────────────────────

const MAX_BATCH_SIZE: usize = 100;

#[derive(Debug, Deserialize)]
pub struct BatchEnqueueRequest {
    pub tasks: Vec<EnqueueRequest>,
}

#[derive(Debug, Serialize)]
pub struct BatchEnqueueResponse {
    pub task_ids: Vec<Uuid>,
}

pub async fn batch_enqueue_handler(
    State(state): State<Arc<AppState>>,
    BoundedJson(req): BoundedJson<BatchEnqueueRequest>,
) -> AppResult<impl IntoResponse> {
    if req.tasks.is_empty() {
        return Err(AppError::BadRequest(
            "tasks array must not be empty".to_string(),
        ));
    }
    if req.tasks.len() > MAX_BATCH_SIZE {
        return Err(AppError::BadRequest(format!(
            "batch size {} exceeds maximum of {MAX_BATCH_SIZE}",
            req.tasks.len(),
        )));
    }
    for task in &req.tasks {
        if task.queue.is_empty() {
            return Err(AppError::BadRequest(
                "queue name cannot be empty".to_string(),
            ));
        }
    }

    let now = Utc::now();
    let new_tasks: Vec<NewTask> = req
        .tasks
        .iter()
        .map(|t| NewTask {
            queue: t.queue.clone(),
            payload: t.payload.clone(),
            max_retries: t.max_retries as i16,
            priority: t.priority,
            scheduled_at: now + chrono::Duration::seconds(t.delay_seconds as i64),
            idempotency_key: None,
        })
        .collect();

    let task_ids = insert_task_batch(&state.pool, new_tasks).await?;

    for (task, id) in req.tasks.iter().zip(task_ids.iter()) {
        TASKS_ENQUEUED.with_label_values(&[&task.queue]).inc();
        tracing::info!(task_id = %id, queue = %task.queue, event = "BatchEnqueued");
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(BatchEnqueueResponse { task_ids }),
    ))
}

// ── Queue admin ───────────────────────────────────────────────────────────────

pub async fn purge_queue_handler(
    State(state): State<Arc<AppState>>,
    Path(queue): Path<String>,
) -> AppResult<impl IntoResponse> {
    let deleted = purge_queue(&state.pool, &queue).await?;
    tracing::info!(queue = %queue, deleted = deleted, event = "QueuePurged");
    Ok(Json(json!({ "queue": queue, "deleted": deleted })))
}

// ── DLQ bulk requeue ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RequeueAllQuery {
    pub queue: Option<String>,
}

pub async fn requeue_all_dlq_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RequeueAllQuery>,
) -> AppResult<impl IntoResponse> {
    let requeued = requeue_all_dlq(&state.pool, q.queue.as_deref()).await?;
    tracing::info!(requeued = requeued, event = "DlqBulkRequeued");
    Ok(Json(json!({ "requeued": requeued })))
}

// ── Health / metrics ──────────────────────────────────────────────────────────

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
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

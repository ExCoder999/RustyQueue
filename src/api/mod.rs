pub mod extractors;
pub mod handlers;

use axum::{
    middleware::from_fn_with_state,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::middleware::circuit_breaker;
use crate::AppState;
use handlers::{
    batch_enqueue_handler, cancel_task_handler, enqueue_task, get_task, health_check,
    list_dlq_handler, list_queues_handler, list_tasks_handler, metrics_handler,
    purge_queue_handler, requeue_all_dlq_handler, requeue_dlq_handler,
};

pub fn build_router(state: Arc<AppState>) -> Router {
    let max_concurrent = state.config.server.max_concurrent_requests;

    Router::new()
        // Task lifecycle
        .route("/api/v1/tasks", post(enqueue_task))
        .route("/api/v1/tasks", get(list_tasks_handler))
        .route("/api/v1/tasks/batch", post(batch_enqueue_handler))
        .route("/api/v1/tasks/:task_id", get(get_task))
        .route("/api/v1/tasks/:task_id/cancel", post(cancel_task_handler))
        // Queue stats and admin
        .route("/api/v1/queues", get(list_queues_handler))
        .route("/api/v1/queues/:queue/tasks", delete(purge_queue_handler))
        // Dead-letter queue
        .route("/api/v1/dlq", get(list_dlq_handler))
        .route("/api/v1/dlq/requeue-all", post(requeue_all_dlq_handler))
        .route("/api/v1/dlq/:task_id/requeue", post(requeue_dlq_handler))
        // Ops
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_handler))
        .layer(from_fn_with_state(state.clone(), circuit_breaker))
        .layer(
            ServiceBuilder::new()
                .layer(ConcurrencyLimitLayer::new(max_concurrent))
                .layer(TraceLayer::new_for_http()),
        )
        .with_state(state)
}

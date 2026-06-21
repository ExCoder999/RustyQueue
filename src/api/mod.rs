pub mod extractors;
pub mod handlers;

use axum::{
    middleware::from_fn_with_state,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::middleware::circuit_breaker;
use crate::AppState;
use handlers::{cancel_task_handler, enqueue_task, get_task, health_check, metrics_handler};

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/v1/tasks", post(enqueue_task))
        .route("/api/v1/tasks/:task_id", get(get_task))
        .route("/api/v1/tasks/:task_id/cancel", post(cancel_task_handler))
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_handler))
        // Circuit breaker runs before handlers; read-only endpoints pass through
        .layer(from_fn_with_state(state.clone(), circuit_breaker))
        .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
        .with_state(state)
}

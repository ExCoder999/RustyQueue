use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::AppState;

/// Rejects POST/PUT/PATCH/DELETE with 503 while the DB circuit is open.
/// The circuit opens after the pool is unreachable for >5 s and closes
/// automatically once the pool recovers (managed by the background monitor
/// started in main).
pub async fn circuit_breaker(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let is_write = matches!(
        req.method(),
        &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
    );

    if is_write && state.circuit_open.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "Database connection pool saturated — try again later"
            })),
        )
            .into_response();
    }

    next.run(req).await
}

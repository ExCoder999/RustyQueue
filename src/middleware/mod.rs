use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
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

/// Validates `Authorization: Bearer <key>` against the configured api_keys list.
/// Skips auth when api_keys is empty (development/open mode) or for /health and /metrics.
pub async fn api_key_auth(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    if state.config.server.api_keys.is_empty() {
        return next.run(req).await;
    }

    let path = req.uri().path();
    if path == "/health" || path == "/metrics" {
        return next.run(req).await;
    }

    let authorized = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|key| state.config.server.api_keys.iter().any(|k| k == key))
        .unwrap_or(false);

    if authorized {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid API key" })),
        )
            .into_response()
    }
}

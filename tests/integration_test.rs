//! Integration tests for rustyqueue.
//!
//! Tests marked `#[ignore]` require a live PostgreSQL instance.
//! Run them with:
//!   DATABASE_URL=postgres://rustyqueue:rustyqueue@localhost:5432/rustyqueue \
//!   cargo test -- --include-ignored

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::Value;
use tower::ServiceExt; // for `oneshot`

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an AppState backed by a real pool for DB-integrated tests.
#[cfg(test)]
async fn test_state(pool: sqlx::PgPool) -> std::sync::Arc<rustyqueue::AppState> {
    use rustyqueue::{
        config::{AppConfig, DatabaseConfig, ObservabilityConfig, QueueConfig, ServerConfig, WorkerConfig},
        AppState,
    };
    use std::sync::{Arc, atomic::AtomicBool};

    let cfg = AppConfig {
        database: DatabaseConfig {
            url: String::new(),
            max_connections: 5,
        },
        queue: QueueConfig {
            default_lease_seconds: 60,
            poll_interval_ms: 500,
            max_command_timeout_seconds: 60,
        },
        worker: WorkerConfig {
            queues: vec!["default".into()],
            num_workers_per_queue: 1,
        },
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 8080,
        },
        observability: ObservabilityConfig { otel_endpoint: None },
    };

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    Arc::new(AppState {
        pool,
        config: cfg,
        shutdown_tx,
        circuit_open: Arc::new(AtomicBool::new(false)),
    })
}

// ---------------------------------------------------------------------------
// Extractor tests (no DB required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_payload_too_large_rejected() {
    use rustyqueue::api::extractors::BoundedJson;

    // Build a body that exceeds 512 KB
    let oversized = "x".repeat(600 * 1024);
    let body = Body::from(format!(r#"{{"data":"{}"}}"#, oversized));
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();

    // Use a minimal router that exercises the extractor
    use axum::{extract::State, response::IntoResponse, routing::post, Router};
    async fn handler(
        _state: State<()>,
        BoundedJson(_v): BoundedJson<Value>,
    ) -> impl IntoResponse {
        StatusCode::OK
    }

    let app = Router::new()
        .route("/", post(handler))
        .with_state(());

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn test_valid_small_payload_accepted_by_extractor() {
    use rustyqueue::api::extractors::BoundedJson;
    use axum::{extract::State, response::IntoResponse, routing::post, Router};

    async fn handler(
        _state: State<()>,
        BoundedJson(v): BoundedJson<Value>,
    ) -> impl IntoResponse {
        assert_eq!(v["hello"], "world");
        StatusCode::OK
    }

    let body = Body::from(r#"{"hello":"world"}"#);
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();

    let app = Router::new()
        .route("/", post(handler))
        .with_state(());

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_invalid_json_returns_422() {
    use rustyqueue::api::extractors::BoundedJson;
    use axum::{extract::State, response::IntoResponse, routing::post, Router};

    async fn handler(
        _state: State<()>,
        BoundedJson(_v): BoundedJson<Value>,
    ) -> impl IntoResponse {
        StatusCode::OK
    }

    let body = Body::from("not json at all");
    let req = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();

    let app = Router::new()
        .route("/", post(handler))
        .with_state(());

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// Circuit-breaker middleware test (no DB required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_circuit_breaker_blocks_writes_when_open() {
    use std::sync::{Arc, atomic::AtomicBool};

    // We need a minimal AppState; skip DB init by using a closed channel trick.
    // Because we never actually call DB queries in this test, pool unused.
    // Use sqlx PgPool that won't connect (URL is never resolved in this test).
    use rustyqueue::config::{
        AppConfig, DatabaseConfig, ObservabilityConfig, QueueConfig, ServerConfig, WorkerConfig,
    };
    use rustyqueue::AppState;

    let cfg = AppConfig {
        database: DatabaseConfig { url: "postgres://x".into(), max_connections: 1 },
        queue: QueueConfig { default_lease_seconds: 60, poll_interval_ms: 500, max_command_timeout_seconds: 60 },
        worker: WorkerConfig { queues: vec!["default".into()], num_workers_per_queue: 1 },
        server: ServerConfig { host: "127.0.0.1".into(), port: 8080 },
        observability: ObservabilityConfig { otel_endpoint: None },
    };

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let circuit_open = Arc::new(AtomicBool::new(true)); // circuit already open

    // Build a PgPool with a bogus URL — it won't be used in this test
    let pool = sqlx::PgPool::connect_lazy("postgres://user:pass@localhost/rustyqueue").unwrap();

    let state = Arc::new(AppState {
        pool,
        config: cfg,
        shutdown_tx,
        circuit_open,
    });

    let app = rustyqueue::api::build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tasks")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"queue":"default","payload":{}}"#))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ---------------------------------------------------------------------------
// DB-integrated tests (requires live PostgreSQL — run with --include-ignored)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_health_check_ok() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;
    let app = rustyqueue::api::build_router(state);

    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_enqueue_and_fetch_task() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;
    let app = rustyqueue::api::build_router(state.clone());

    // Enqueue
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tasks")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"queue":"default","payload":{"command":["echo","hello"]},"max_retries":1}"#,
        ))
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let task_id = json["task_id"].as_str().expect("task_id missing");

    // Fetch status
    let req = Request::builder()
        .uri(format!("/api/v1/tasks/{}", task_id))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"].as_str().unwrap(), "Pending");
}

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_idempotency_key_deduplication() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;

    let idem_key = format!("test-key-{}", uuid::Uuid::now_v7());

    for i in 0..2u8 {
        let app = rustyqueue::api::build_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/tasks")
            .header("content-type", "application/json")
            .header("idempotency-key", &idem_key)
            .body(Body::from(r#"{"queue":"default","payload":{}}"#))
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        if i == 0 {
            assert_eq!(response.status(), StatusCode::ACCEPTED, "first request should succeed");
        } else {
            assert_eq!(response.status(), StatusCode::CONFLICT, "duplicate should be rejected");
        }
    }
}

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_get_nonexistent_task_returns_404() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;
    let app = rustyqueue::api::build_router(state);

    let bogus_id = uuid::Uuid::now_v7();
    let req = Request::builder()
        .uri(format!("/api/v1/tasks/{}", bogus_id))
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

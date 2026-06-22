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
            retry_base_delay_seconds: 5,
            retry_max_delay_seconds: 300,
        },
        worker: WorkerConfig {
            queues: vec!["default".into()],
            num_workers_per_queue: 1,
        },
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 8080,
            max_concurrent_requests: 512,
        },
        observability: ObservabilityConfig { otel_endpoint: None },
    };

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    Arc::new(AppState {
        pool,
        config: cfg,
        shutdown_tx,
        circuit_open: Arc::new(AtomicBool::new(false)),
        task_cancel_tokens: Default::default(),
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
        queue: QueueConfig {
            default_lease_seconds: 60,
            poll_interval_ms: 500,
            max_command_timeout_seconds: 60,
            retry_base_delay_seconds: 5,
            retry_max_delay_seconds: 300,
        },
        worker: WorkerConfig { queues: vec!["default".into()], num_workers_per_queue: 1 },
        server: ServerConfig { host: "127.0.0.1".into(), port: 8080, max_concurrent_requests: 512 },
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
        task_cancel_tokens: Default::default(),
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

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_list_tasks_returns_paged_response() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;
    let app = rustyqueue::api::build_router(state.clone());

    // Enqueue a task so the list is non-empty.
    let enqueue_req = Request::builder()
        .method("POST")
        .uri("/api/v1/tasks")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"queue":"list-test","payload":{"command":["true"]},"max_retries":0}"#,
        ))
        .unwrap();
    let enqueue_resp = app.clone().oneshot(enqueue_req).await.unwrap();
    assert_eq!(enqueue_resp.status(), StatusCode::ACCEPTED);

    // List tasks filtered by the queue we just used.
    let req = Request::builder()
        .uri("/api/v1/tasks?queue=list-test&limit=10&offset=0")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["total"].as_i64().unwrap() >= 1, "total should be at least 1");
    assert!(json["items"].as_array().unwrap().len() >= 1);
    assert_eq!(json["items"][0]["queue"].as_str().unwrap(), "list-test");
}

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_list_queues_returns_stats() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool).await;
    let app = rustyqueue::api::build_router(state.clone());

    // Seed a task so at least one queue appears.
    let enqueue_req = Request::builder()
        .method("POST")
        .uri("/api/v1/tasks")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"queue":"stats-test","payload":{"command":["true"]},"max_retries":0}"#,
        ))
        .unwrap();
    app.clone().oneshot(enqueue_req).await.unwrap();

    let req = Request::builder()
        .uri("/api/v1/queues")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let queues = json.as_array().expect("expected JSON array");
    let stats_queue = queues.iter().find(|q| q["queue"] == "stats-test");
    assert!(stats_queue.is_some(), "stats-test queue should appear");

    let s = stats_queue.unwrap();
    // pending count must be a non-negative integer
    assert!(s["pending"].as_i64().unwrap() >= 1);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL at DATABASE_URL"]
async fn test_dlq_list_and_requeue() {
    use rustyqueue::db::queries::{mark_failed, move_to_dlq};

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustyqueue:rustyqueue@localhost/rustyqueue".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = test_state(pool.clone()).await;
    let app = rustyqueue::api::build_router(state.clone());

    // Enqueue a task, manually fail it and move it to DLQ via query helpers.
    let enqueue_req = Request::builder()
        .method("POST")
        .uri("/api/v1/tasks")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"queue":"dlq-test","payload":{"command":["false"]},"max_retries":0}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(enqueue_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let enqueue_json: Value = serde_json::from_slice(&body).unwrap();
    let task_id_str = enqueue_json["task_id"].as_str().unwrap().to_string();
    let task_id: uuid::Uuid = task_id_str.parse().unwrap();

    mark_failed(&pool, task_id, "forced failure").await.unwrap();
    move_to_dlq(&pool, task_id).await.unwrap();

    // GET /api/v1/dlq should include our task.
    let req = Request::builder()
        .uri("/api/v1/dlq?queue=dlq-test")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["total"].as_i64().unwrap() >= 1);
    let found = json["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["id"].as_str() == Some(&task_id_str));
    assert!(found, "DLQ should contain the moved task");

    // POST /api/v1/dlq/:id/requeue should move it back to tasks.
    let requeue_req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/dlq/{}/requeue", task_id))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(requeue_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["requeued"], true);

    // The task should now appear in GET /api/v1/tasks with status Pending.
    let req = Request::builder()
        .uri(format!("/api/v1/tasks/{}", task_id))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"].as_str().unwrap(), "Pending");
}

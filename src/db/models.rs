use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DbTask {
    pub id: Uuid,
    pub queue: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub priority: i32,
    pub max_retries: i16,
    pub retries: i16,
    pub scheduled_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTask {
    pub queue: String,
    pub payload: serde_json::Value,
    pub max_retries: i16,
    pub priority: i32,
    pub scheduled_at: DateTime<Utc>,
    pub idempotency_key: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DlqTask {
    pub id: Uuid,
    pub queue: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub priority: i32,
    pub max_retries: i16,
    pub retries: i16,
    pub scheduled_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TaskStatus {
    pub id: Uuid,
    pub status: String,
    pub retries: i16,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Lightweight task row for list endpoints (no payload blob).
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TaskSummary {
    pub id: Uuid,
    pub queue: String,
    pub status: String,
    pub priority: i32,
    pub retries: i16,
    pub max_retries: i16,
    pub last_error: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Per-queue status counts returned by GET /api/v1/queues.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct QueueStats {
    pub queue: String,
    pub pending: i64,
    pub processing: i64,
    pub failed: i64,
    pub completed: i64,
}

/// DLQ row for list/requeue endpoints (no payload blob).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DlqSummary {
    pub id: Uuid,
    pub queue: String,
    pub retries: i16,
    pub max_retries: i16,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
}

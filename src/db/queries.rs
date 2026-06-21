use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::models::{DbTask, NewTask, TaskStatus};
use crate::error::{AppError, AppResult};

pub async fn insert_task(pool: &PgPool, task: NewTask) -> AppResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO tasks (id, queue, payload, status, priority, max_retries, scheduled_at, idempotency_key)
        VALUES ($1, $2, $3, 'Pending', $4, $5, $6, $7)
        "#,
        id,
        task.queue,
        task.payload,
        task.priority,
        task.max_retries,
        task.scheduled_at,
        task.idempotency_key,
    )
    .execute(pool)
    .await?;

    Ok(id)
}

pub async fn get_task_status(pool: &PgPool, id: Uuid) -> AppResult<TaskStatus> {
    let row = sqlx::query_as!(
        TaskStatus,
        r#"
        SELECT id, status, retries, last_error, created_at
        FROM tasks
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool)
    .await?;

    if let Some(task) = row {
        return Ok(task);
    }

    // Check DLQ
    let dlq_row = sqlx::query_as!(
        TaskStatus,
        r#"
        SELECT id, status, retries, last_error, created_at
        FROM dead_letter_tasks
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool)
    .await?;

    dlq_row.ok_or_else(|| AppError::NotFound(format!("Task {} not found", id)))
}

pub async fn cancel_task(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let result = sqlx::query!(
        r#"
        UPDATE tasks
        SET status = 'Failed', last_error = 'Cancelled by user'
        WHERE id = $1 AND status IN ('Pending', 'Processing')
        "#,
        id
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn fetch_pending_task(pool: &PgPool, queue: &str, lease_seconds: i64) -> AppResult<Option<DbTask>> {
    let task = sqlx::query_as!(
        DbTask,
        r#"
        UPDATE tasks
        SET
            status = 'Processing',
            leased_until = NOW() + ($2::bigint * INTERVAL '1 second'),
            last_heartbeat_at = NOW()
        WHERE id = (
            SELECT id FROM tasks
            WHERE status = 'Pending'
              AND queue = $1
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, scheduled_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING
            id, queue, payload, status, priority, max_retries, retries,
            scheduled_at, leased_until, last_heartbeat_at, last_error,
            idempotency_key, created_at
        "#,
        queue,
        lease_seconds,
    )
    .fetch_optional(pool)
    .await?;

    Ok(task)
}

pub async fn mark_complete(pool: &PgPool, id: Uuid) -> AppResult<()> {
    sqlx::query!(
        r#"UPDATE tasks SET status = 'Completed', leased_until = NULL WHERE id = $1"#,
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: Uuid, error: &str) -> AppResult<()> {
    sqlx::query!(
        r#"
        UPDATE tasks
        SET status = 'Failed', last_error = $2, leased_until = NULL
        WHERE id = $1
        "#,
        id,
        error,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn increment_retry(pool: &PgPool, id: Uuid, error: &str) -> AppResult<()> {
    sqlx::query!(
        r#"
        UPDATE tasks
        SET
            status = 'Pending',
            retries = retries + 1,
            last_error = $2,
            leased_until = NULL,
            scheduled_at = NOW() + INTERVAL '5 seconds'
        WHERE id = $1
        "#,
        id,
        error,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn move_to_dlq(pool: &PgPool, id: Uuid) -> AppResult<()> {
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO dead_letter_tasks
            (id, queue, payload, status, priority, max_retries, retries,
             scheduled_at, leased_until, last_heartbeat_at, last_error,
             idempotency_key, created_at, failed_at)
        SELECT
            id, queue, payload, status, priority, max_retries, retries,
            scheduled_at, leased_until, last_heartbeat_at, last_error,
            idempotency_key, created_at, NOW()
        FROM tasks
        WHERE id = $1
        "#,
        id,
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!("DELETE FROM tasks WHERE id = $1", id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn update_heartbeat(pool: &PgPool, id: Uuid) -> AppResult<()> {
    sqlx::query!(
        r#"UPDATE tasks SET last_heartbeat_at = NOW() WHERE id = $1 AND status = 'Processing'"#,
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn reset_stuck_leases(pool: &PgPool) -> AppResult<u64> {
    let result = sqlx::query!(
        r#"
        UPDATE tasks
        SET
            status = 'Pending',
            leased_until = NULL,
            retries = retries + 1,
            last_error = 'Lease expired (worker crash or timeout)'
        WHERE status = 'Processing'
          AND leased_until < NOW()
        "#
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn move_exceeded_retries_to_dlq(pool: &PgPool) -> AppResult<u64> {
    let exceeded = sqlx::query!(
        r#"SELECT id FROM tasks WHERE retries >= max_retries AND status IN ('Pending', 'Failed')"#
    )
    .fetch_all(pool)
    .await?;

    let count = exceeded.len() as u64;
    for row in exceeded {
        move_to_dlq(pool, row.id).await?;
    }
    Ok(count)
}

pub async fn get_queue_length(pool: &PgPool) -> AppResult<i64> {
    let row = sqlx::query!("SELECT COUNT(*) as count FROM tasks WHERE status = 'Pending'")
        .fetch_one(pool)
        .await?;
    Ok(row.count.unwrap_or(0))
}

pub async fn get_idempotency_task_id(pool: &PgPool, key_hash: &str) -> AppResult<Option<Uuid>> {
    let row = sqlx::query!(
        "SELECT task_id FROM idempotency_keys WHERE key_hash = $1",
        key_hash
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.task_id))
}

pub async fn store_idempotency_key(pool: &PgPool, key_hash: &str, task_id: Uuid) -> AppResult<()> {
    sqlx::query!(
        r#"
        INSERT INTO idempotency_keys (key_hash, task_id)
        VALUES ($1, $2)
        ON CONFLICT (key_hash) DO NOTHING
        "#,
        key_hash,
        task_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn check_pool_health(pool: &PgPool) -> bool {
    tokio::time::timeout(
        tokio::time::Duration::from_secs(1),
        sqlx::query("SELECT 1").execute(pool),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

pub async fn get_cancelled_task_ids(pool: &PgPool) -> AppResult<Vec<Uuid>> {
    let rows = sqlx::query!(
        "SELECT id FROM tasks WHERE status = 'Failed' AND last_error = 'Cancelled by user'"
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.id).collect())
}

pub async fn is_task_cancelled(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let row = sqlx::query!(
        "SELECT status, last_error FROM tasks WHERE id = $1",
        id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        r.status == "Failed" && r.last_error.as_deref() == Some("Cancelled by user")
    }).unwrap_or(false))
}

pub fn utc_now() -> chrono::DateTime<Utc> {
    Utc::now()
}

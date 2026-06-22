use sqlx::PgPool;
use uuid::Uuid;

use crate::db::models::{DbTask, DlqSummary, NewTask, QueueStats, TaskStatus, TaskSummary};
use crate::error::{AppError, AppResult};

pub async fn insert_task(pool: &PgPool, task: NewTask) -> AppResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        r#"
        INSERT INTO tasks (id, queue, payload, status, priority, max_retries, scheduled_at, idempotency_key)
        VALUES ($1, $2, $3, 'Pending', $4, $5, $6, $7)
        "#,
    )
    .bind(id)
    .bind(&task.queue)
    .bind(&task.payload)
    .bind(task.priority)
    .bind(task.max_retries)
    .bind(task.scheduled_at)
    .bind(&task.idempotency_key)
    .execute(pool)
    .await?;

    Ok(id)
}

pub async fn get_task_status(pool: &PgPool, id: Uuid) -> AppResult<TaskStatus> {
    let row = sqlx::query_as::<_, TaskStatus>(
        "SELECT id, status, retries, last_error, created_at FROM tasks WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    if let Some(task) = row {
        return Ok(task);
    }

    let dlq_row = sqlx::query_as::<_, TaskStatus>(
        "SELECT id, status, retries, last_error, created_at FROM dead_letter_tasks WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    dlq_row.ok_or_else(|| AppError::NotFound(format!("Task {} not found", id)))
}

pub async fn cancel_task(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let result = sqlx::query(
        "UPDATE tasks SET status = 'Failed', last_error = 'Cancelled by user' \
         WHERE id = $1 AND status IN ('Pending', 'Processing')",
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn fetch_pending_task(
    pool: &PgPool,
    queue: &str,
    lease_seconds: i64,
) -> AppResult<Option<DbTask>> {
    let task = sqlx::query_as::<_, DbTask>(
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
    )
    .bind(queue)
    .bind(lease_seconds)
    .fetch_optional(pool)
    .await?;

    Ok(task)
}

pub async fn mark_complete(pool: &PgPool, id: Uuid) -> AppResult<()> {
    sqlx::query("UPDATE tasks SET status = 'Completed', leased_until = NULL WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: Uuid, error: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE tasks SET status = 'Failed', last_error = $2, leased_until = NULL WHERE id = $1",
    )
    .bind(id)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// Reschedule a failed task with exponential backoff.
/// Delay = min(2^current_retries * base_delay_secs, max_delay_secs)
pub async fn increment_retry(
    pool: &PgPool,
    id: Uuid,
    error: &str,
    base_delay_secs: i64,
    max_delay_secs: i64,
) -> AppResult<()> {
    sqlx::query(
        r#"
        UPDATE tasks
        SET
            status      = 'Pending',
            retries     = retries + 1,
            last_error  = $2,
            leased_until = NULL,
            scheduled_at = NOW() + make_interval(
                secs => LEAST(
                    POWER(2, retries::double precision) * $3::double precision,
                    $4::double precision
                )::int
            )
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(error)
    .bind(base_delay_secs)
    .bind(max_delay_secs)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn move_to_dlq(pool: &PgPool, id: Uuid) -> AppResult<()> {
    let mut tx = pool.begin().await?;

    sqlx::query(
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
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM tasks WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn update_heartbeat(pool: &PgPool, id: Uuid) -> AppResult<()> {
    sqlx::query(
        "UPDATE tasks SET last_heartbeat_at = NOW() WHERE id = $1 AND status = 'Processing'",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn reset_stuck_leases(pool: &PgPool) -> AppResult<u64> {
    let result = sqlx::query(
        r#"
        UPDATE tasks
        SET
            status = 'Pending',
            leased_until = NULL,
            retries = retries + 1,
            last_error = 'Lease expired (worker crash or timeout)'
        WHERE status = 'Processing'
          AND leased_until < NOW()
        "#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn move_exceeded_retries_to_dlq(pool: &PgPool) -> AppResult<u64> {
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM tasks WHERE retries >= max_retries AND status IN ('Pending', 'Failed')",
    )
    .fetch_all(pool)
    .await?;

    let count = rows.len() as u64;
    for (id,) in rows {
        move_to_dlq(pool, id).await?;
    }
    Ok(count)
}

pub async fn get_queue_length(pool: &PgPool) -> AppResult<i64> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE status = 'Pending'")
            .fetch_one(pool)
            .await?;
    Ok(count)
}

pub async fn get_queue_length_by_queue(pool: &PgPool, queue: &str) -> AppResult<i64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tasks WHERE status = 'Pending' AND queue = $1",
    )
    .bind(queue)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn get_idempotency_task_id(pool: &PgPool, key_hash: &str) -> AppResult<Option<Uuid>> {
    let task_id: Option<Uuid> =
        sqlx::query_scalar("SELECT task_id FROM idempotency_keys WHERE key_hash = $1")
            .bind(key_hash)
            .fetch_optional(pool)
            .await?;
    Ok(task_id)
}

pub async fn store_idempotency_key(pool: &PgPool, key_hash: &str, task_id: Uuid) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO idempotency_keys (key_hash, task_id) VALUES ($1, $2) ON CONFLICT (key_hash) DO NOTHING",
    )
    .bind(key_hash)
    .bind(task_id)
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

pub async fn is_task_cancelled(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT status, last_error FROM tasks WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;

    Ok(row
        .map(|(status, last_error)| {
            status == "Failed" && last_error.as_deref() == Some("Cancelled by user")
        })
        .unwrap_or(false))
}

// ── List / admin queries ──────────────────────────────────────────────────────

pub async fn list_tasks(
    pool: &PgPool,
    queue: Option<&str>,
    status: Option<&str>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<TaskSummary>> {
    let rows = sqlx::query_as::<_, TaskSummary>(
        r#"
        SELECT id, queue, status, priority, retries, max_retries, last_error, scheduled_at, created_at
        FROM tasks
        WHERE ($1::text IS NULL OR queue  = $1)
          AND ($2::text IS NULL OR status = $2)
        ORDER BY priority DESC, created_at ASC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(queue)
    .bind(status)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn count_tasks(
    pool: &PgPool,
    queue: Option<&str>,
    status: Option<&str>,
) -> AppResult<i64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tasks WHERE ($1::text IS NULL OR queue = $1) AND ($2::text IS NULL OR status = $2)",
    )
    .bind(queue)
    .bind(status)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn get_queue_stats(pool: &PgPool) -> AppResult<Vec<QueueStats>> {
    let rows = sqlx::query_as::<_, QueueStats>(
        r#"
        SELECT
            queue,
            COUNT(*) FILTER (WHERE status = 'Pending')::bigint    AS pending,
            COUNT(*) FILTER (WHERE status = 'Processing')::bigint AS processing,
            COUNT(*) FILTER (WHERE status = 'Failed')::bigint     AS failed,
            COUNT(*) FILTER (WHERE status = 'Completed')::bigint  AS completed
        FROM tasks
        GROUP BY queue
        ORDER BY queue
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn list_dlq(
    pool: &PgPool,
    queue: Option<&str>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<DlqSummary>> {
    let rows = sqlx::query_as::<_, DlqSummary>(
        r#"
        SELECT id, queue, retries, max_retries, last_error, created_at, failed_at
        FROM dead_letter_tasks
        WHERE ($1::text IS NULL OR queue = $1)
        ORDER BY failed_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(queue)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn count_dlq(pool: &PgPool, queue: Option<&str>) -> AppResult<i64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dead_letter_tasks WHERE ($1::text IS NULL OR queue = $1)",
    )
    .bind(queue)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

/// Moves a DLQ task back to the `tasks` table with retries reset to 0.
/// Returns `false` if the task wasn't found in the DLQ.
pub async fn requeue_dlq_task(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let mut tx = pool.begin().await?;

    let result = sqlx::query(
        r#"
        INSERT INTO tasks
            (id, queue, payload, status, priority, max_retries, retries, scheduled_at, created_at)
        SELECT id, queue, payload, 'Pending', priority, max_retries, 0, NOW(), NOW()
        FROM dead_letter_tasks
        WHERE id = $1
        "#,
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;

    if result.rows_affected() == 0 {
        return Ok(false);
    }

    sqlx::query("DELETE FROM dead_letter_tasks WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(true)
}

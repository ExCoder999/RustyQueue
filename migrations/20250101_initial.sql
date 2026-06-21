-- Main tasks table
CREATE TABLE IF NOT EXISTS tasks (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'Pending'
                         CHECK (status IN ('Pending', 'Processing', 'Completed', 'Failed')),
    priority         INTEGER NOT NULL DEFAULT 0,
    max_retries      SMALLINT NOT NULL DEFAULT 3,
    retries          SMALLINT NOT NULL DEFAULT 0,
    scheduled_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    leased_until     TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    last_error       TEXT,
    idempotency_key  TEXT UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for fast polling dispatcher
CREATE INDEX idx_tasks_status_queue_scheduled
    ON tasks (status, queue, scheduled_at)
    WHERE status = 'Pending';

CREATE INDEX idx_tasks_leased_until
    ON tasks (leased_until)
    WHERE status = 'Processing';

-- Dead letter queue table
CREATE TABLE IF NOT EXISTS dead_letter_tasks (
    id               UUID PRIMARY KEY,
    queue            TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL,
    priority         INTEGER NOT NULL DEFAULT 0,
    max_retries      SMALLINT NOT NULL,
    retries          SMALLINT NOT NULL,
    scheduled_at     TIMESTAMPTZ NOT NULL,
    leased_until     TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    last_error       TEXT,
    idempotency_key  TEXT,
    created_at       TIMESTAMPTZ NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency tracking (separate from UNIQUE on tasks to survive DLQ moves)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key_hash         TEXT PRIMARY KEY,
    task_id          UUID NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

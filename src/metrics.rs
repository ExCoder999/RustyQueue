use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    CounterVec, Gauge, GaugeVec, HistogramVec, TextEncoder,
};

pub static TASKS_ENQUEUED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "tasks_enqueued_total",
        "Total number of tasks enqueued",
        &["queue"]
    )
    .expect("Failed to register tasks_enqueued_total")
});

pub static TASKS_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "tasks_duration_seconds",
        "Task execution duration in seconds",
        &["queue", "status"],
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    )
    .expect("Failed to register tasks_duration_seconds")
});

/// Pending task count broken down per queue name.
pub static QUEUE_LENGTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "queue_length",
        "Number of pending tasks per queue",
        &["queue"]
    )
    .expect("Failed to register queue_length")
});

pub static ACTIVE_WORKERS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("active_workers", "Number of currently active workers")
        .expect("Failed to register active_workers")
});

pub fn register_metrics() {
    Lazy::force(&TASKS_ENQUEUED);
    Lazy::force(&TASKS_DURATION);
    Lazy::force(&QUEUE_LENGTH);
    Lazy::force(&ACTIVE_WORKERS);
}

pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    encoder.encode_to_string(&families).unwrap_or_default()
}

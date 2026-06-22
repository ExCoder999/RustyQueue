use clap::Parser;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "rustyqueue", version, about = "Mission-critical task queue")]
pub struct Cli {
    #[arg(long, default_value = "config.toml", env = "RUSTYQUEUE_CONFIG_PATH")]
    pub config_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueueConfig {
    pub default_lease_seconds: u64,
    pub poll_interval_ms: u64,
    pub max_command_timeout_seconds: u64,
    /// Base delay (seconds) for exponential retry backoff: delay = 2^attempt * base
    pub retry_base_delay_seconds: u64,
    /// Maximum retry backoff delay in seconds (caps the exponential growth)
    pub retry_max_delay_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WorkerConfig {
    pub queues: Vec<String>,
    pub num_workers_per_queue: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Maximum number of in-flight HTTP requests before the server starts
    /// returning 503 (backpressure via tower ConcurrencyLimitLayer).
    pub max_concurrent_requests: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    pub otel_endpoint: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub queue: QueueConfig,
    pub worker: WorkerConfig,
    pub server: ServerConfig,
    pub observability: ObservabilityConfig,
}

impl AppConfig {
    pub fn load(config_path: &str) -> anyhow::Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name(config_path).required(false))
            .add_source(
                Environment::with_prefix("RUSTYQUEUE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        Ok(cfg.try_deserialize::<AppConfig>()?)
    }
}

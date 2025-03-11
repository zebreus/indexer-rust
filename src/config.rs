use clap::Parser;
use std::sync::LazyLock;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(about)]
pub struct Args {
    /// Path to a certificate to check jetstream server against. By default the bundled ISRG Root X1 certificate is used.
    #[arg(short = 'c', long)]
    pub certificate: Option<String>,
    /// Set the tokio threadpool size. The default value is the number of cores available to the system.
    #[arg(long)]
    pub threads: Option<usize>,
    /// Postgres connection string
    #[arg(
        short = 'D',
        long,
        default_value = "postgres://user-name:strong-password@localhost/user-name"
    )]
    pub db: String,
    /// Size of the database connection pool
    #[arg(long, default_value = "10")]
    pub db_pool_size: u32,
    /// Username for the database server
    #[arg(short, long, default_value = "root")]
    pub username: String,
    /// Password for the database server
    #[arg(short, long, default_value = "root")]
    pub password: String,
    /// Enable backfilling of old repos
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub no_backfill: bool,
    /// Enable attaching to the jetstream for realtime updates
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub no_jetstream: bool,
    /// Capacity of the surrealdb connection. 0 means unbounded
    #[arg(long, default_value = "0")]
    pub surrealdb_capacity: usize,
    /// Enable tokio console support
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub tokio_console: bool,
    /// Enable opentelemetry tracing support
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub otel_tracing: bool,
    /// Disable opentelemetry metrics support
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub no_otel_metrics: bool,
    /// Disable opentelemetry logging support
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub no_otel_logs: bool,
    /// Dont write to the database when backfilling
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub no_write_when_backfilling: bool,
    /// Size of the buffer between each pipeline stage in elements
    #[arg(long, default_value = "200")]
    pub pipeline_buffer_size: usize,
    /// Number of concurrent elements in each pipeline stage
    #[arg(long, default_value = "50")]
    pub pipeline_concurrent_elements: usize,
    /// Multiply the number of concurrent download repo tasks by this factor
    #[arg(long, default_value = "8")]
    pub pipeline_download_concurrency_multiplier: usize,
    /// Timeout for a pipeline stage in seconds. No pipeline stage should take longer than this
    #[arg(long, default_value = "1100")]
    pub pipeline_stage_timeout: u64,
    /// Timeout for the repo downloading pipeline stage in seconds.
    /// If this is longer than the pipeline_stage_timeout, the pipeline_stage_timeout will be used
    #[arg(long, default_value = "1000")]
    pub download_repo_timeout: u64,
    /// The maximum number of times to attempt to download a repo before giving up
    #[arg(long, default_value = "5")]
    pub download_repo_attempts: u64,
    /// Timeout for downloading information from the directory in seconds.
    /// If this is longer than the pipeline_stage_timeout, the pipeline_stage_timeout will be used
    #[arg(long, default_value = "200")]
    pub directory_download_timeout: u64,
    /// Number of DIDs the RepoStream should prefetch
    #[arg(long, default_value = "5000")]
    pub repo_stream_buffer_size: usize,
    /// Maximum number of concurrent database transactions
    #[arg(long, default_value = "1")]
    pub max_concurrent_transactions: u32,
    /// Minimum number of concurrent database transactions
    #[arg(long, default_value = "1")]
    pub min_concurrent_transactions: u32,
    /// Minimum number of rows per database transaction
    #[arg(long, default_value = "1000")]
    pub min_rows_per_transaction: usize,
}

pub static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);

use clap::{ArgAction, Parser};
use std::sync::LazyLock;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(about)]
pub struct Args {
    /// Certificate to check jetstream server against
    #[arg(short = 'c', long, default_value = "/etc/ssl/certs/ISRG_Root_X1.pem")]
    pub certificate: String,
    /// Set the tokio threadpool size. The default value is the number of cores available to the system.
    #[arg(long)]
    pub threads: Option<usize>,
    /// Override parallel task count for full repo index operations
    #[arg(long)]
    pub max_tasks: Option<usize>,
    /// Endpoint of the database server (including port and protocol)
    #[arg(short = 'D', long, default_value = "rocksdb://path/to/surreal.db")]
    pub db: String,
    /// Username for the database server
    #[arg(short, long, default_value = "root")]
    pub username: String,
    /// Password for the database server
    #[arg(short, long, default_value = "root")]
    pub password: String,
    /// Debug verbosity level
    #[arg(short, action = ArgAction::Count)]
    pub verbosity: u8,
    /// Enable backfilling of old repos
    #[arg(long, default_value = "true", default_missing_value = "true", num_args=0..=1)]
    pub backfill: Option<bool>,
    /// Enable attaching to the jetstream for realtime updates
    #[arg(long, default_value = "true", default_missing_value = "true", num_args=0..=1)]
    pub jetstream: Option<bool>,
    /// Capacity of the surrealdb connection. 0 means unbounded
    #[arg(long, default_value = "0")]
    pub surrealdb_capacity: usize,
    /// Size of the buffer between each pipeline stage in elements
    #[arg(long, default_value = "10")]
    pub pipeline_buffer_size: usize,
    /// Multiply the size of the download buffer by this factor
    #[arg(long, default_value = "5")]
    pub download_buffer_multiplier: usize,
    /// Enable tokio console support
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub console: Option<bool>,
    /// Enable opentelemetry tracing support
    #[arg(long, default_value = "true", default_missing_value = "true", num_args=0..=1)]
    pub otel_tracing: Option<bool>,
    /// Enable opentelemetry
    #[arg(long, default_value = "true", default_missing_value = "true", num_args=0..=1)]
    pub otel: Option<bool>,
    /// Dont write to the database when backfilling
    #[arg(long, default_value = "false", default_missing_value = "true", num_args=0..=1)]
    pub dont_write_when_backfilling: Option<bool>,
}

pub const ARGS: LazyLock<Args> = LazyLock::new(|| Args::parse());

// impl Args {
//     /// Dump configuration to log
//     pub fn dump(self: &Self) {
//         // dump configuration
//         info!("{}", "Configuration:".bold().underline().blue());
//         info!("{}: {}", "Certificate".cyan(), self.certificate.green());
//         info!(
//             "{}: {}",
//             "Threads".cyan(),
//             self.threads.map_or_else(
//                 || "Not set, using CPU count".yellow(),
//                 |v| v.to_string().green()
//             )
//         );
//         info!(
//             "{}: {}",
//             "Max tasks".cyan(),
//             self.max_tasks.map_or_else(
//                 || "Not set, using CPU count times 32".yellow(),
//                 |v| v.to_string().green()
//             )
//         );
//         info!(
//             "{}: {}",
//             "Verbosity Level".cyan(),
//             self.log_level().to_string().green()
//         );
//     }

//     /// Verbosity to log level
//     pub fn log_level(self: &Self) -> LevelFilter {
//         match self.verbosity {
//             0 => LevelFilter::INFO,
//             1 => LevelFilter::DEBUG,
//             _ => LevelFilter::TRACE,
//         }
//     }
// }

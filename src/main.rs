use anyhow::Context;
use config::ARGS;
use database::{connect, repo_indexer::start_full_repo_indexer};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use jetstream_consumer::attach_jetstream;
use metrics_reporter::export_system_metrics;
use observability::init_observability;
use opentelemetry_sdk::metrics::data;
use std::{
    process::exit,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::runtime::Builder;
use tokio_rustls::rustls::crypto::aws_lc_rs::default_provider;
use tracing::error;

mod config;
mod database;
mod jetstream_consumer;
mod metrics_reporter;
mod observability;
mod websocket;

/// Override the global allocator with mimalloc
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Entry point for the application
fn main() {
    // Build async runtime
    let mut rt_builder = Builder::new_multi_thread();
    rt_builder
        .enable_all()
        .max_blocking_threads(512 * 512)
        .enable_time()
        .enable_io()
        .max_io_events_per_tick(1024 * 512)
        .global_queue_interval(40)
        .event_interval(20)
        .thread_name_fn(|| {
            static ATOMIC: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC.fetch_add(1, Ordering::Relaxed);
            format!("Thread {}", id)
        });
    if let Some(threads) = ARGS.threads {
        rt_builder.worker_threads(threads);
    }
    let rt = rt_builder.build().unwrap();

    // Launch the async main function
    default_provider().install_default().unwrap();
    let err = rt.block_on(application_main());
    rt.shutdown_timeout(Duration::from_secs(5));
    if let Err(e) = &err {
        error!(target: "indexer", "{:?}", e);
        exit(1);
    } else {
        eprintln!("A task exited successfully, shutting down");
        exit(0);
    }
}

/// Asynchronous main function
async fn application_main() -> anyhow::Result<()> {
    let _otel_guard = init_observability().await;

    // Connect to the database
    let db = database::connect_surreal(ARGS.db.first().unwrap())
        .await
        .context("Failed to connect to the database")?;
    let database = connect().await?;

    // Create tasks
    let metrics_task = export_system_metrics().boxed();
    let jetstream_task =
        attach_jetstream(db.to_owned(), database.clone(), ARGS.certificate.clone()).boxed();
    let indexer_task = start_full_repo_indexer(db.to_owned(), database.clone()).boxed_local();

    // Add all tasks to a list
    let mut tasks: FuturesUnordered<_> = FuturesUnordered::new();
    if !ARGS.no_backfill {
        tasks.push(indexer_task);
    }
    if !ARGS.no_jetstream {
        tasks.push(jetstream_task);
    }
    tasks.push(metrics_task);

    // Wait for the first task to exit
    let first_exited_task = tasks.next().await;
    let Some(task_result) = first_exited_task else {
        return Err(anyhow::anyhow!(
            "It seems like there were no tasks. This should never happen."
        ));
    };
    task_result
}

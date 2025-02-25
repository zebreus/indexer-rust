#![feature(type_changing_struct_update)]

use anyhow::Context;
use config::Args;
use database::repo_indexer::start_full_repo_indexer;
use jetstream_consumer::attach_jetstream;
use metrics_reporter::export_system_metrics;
use observability::init_observability;
use std::{
    process::exit,
    sync::atomic::{AtomicUsize, Ordering},
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
    let args = config::parse_args();
    args.dump();

    // build async runtime
    let rt = if let Some(threads) = args.worker_threads {
        Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads)
            .max_blocking_threads(512 * 512)
            .max_io_events_per_tick(1024 * 512)
            .thread_name_fn(|| {
                static ATOMIC: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC.fetch_add(1, Ordering::Relaxed);
                format!("Thread {}", id)
            })
            .build()
            .unwrap()
    } else {
        Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(512 * 512)
            .max_io_events_per_tick(1024 * 512)
            .thread_name_fn(|| {
                static ATOMIC: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC.fetch_add(1, Ordering::Relaxed);
                format!("Thread {}", id)
            })
            .build()
            .unwrap()
    };

    // launch the application
    default_provider().install_default().unwrap();
    let err = rt.block_on(application_main(args));
    if let Err(e) = &err {
        error!(target: "indexer", "{:?}", e);
        exit(1);
    }
}

/// Asynchronous main function
async fn application_main(args: Args) -> anyhow::Result<()> {
    let _otel_guard = init_observability().await;

    // connect to the database
    let db = database::connect(args.db, &args.username, &args.password)
        .await
        .context("Failed to connect to the database")?;

    let metrics_task = tokio::spawn(export_system_metrics());
    let jetstream_task = tokio::spawn(attach_jetstream(db.clone(), args.certificate.clone()));
    if args.mode == "full" {
        start_full_repo_indexer(db.clone()).await?;
    }

    // TODO: To something smart if one of the tasks exits
    metrics_task.await??;
    jetstream_task.await??;
    Ok(())
}

use anyhow::Context;
use config::Args;
use database::repo_indexer::start_full_repo_indexer;
use opentelemetry::global;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};
use surrealdb::{engine::any::Any, Surreal};
use tokio::runtime::Builder;
use tokio_rustls::rustls::crypto::aws_lc_rs::default_provider;
use tracing::{error, info};
use tracing_subscriber::{prelude::*, EnvFilter};

mod config;
mod database;
mod websocket;

/// Override the global allocator with mimalloc
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
    Resource::builder()
        .with_service_name("rust-indexer")
        .build()
});

fn init_logger() -> SdkLoggerProvider {
    let otlp_log_exporter = LogExporter::builder().with_tonic().build().unwrap();
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_batch_exporter(otlp_log_exporter)
        .build();
    let otel_filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_filter);

    let tokio_console_layer = console_subscriber::spawn();

    let stdout_filter = EnvFilter::new("info").add_directive("opentelemetry=info".parse().unwrap());
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(stdout_filter);

    tracing_subscriber::registry()
        .with(tokio_console_layer)
        .with(otel_layer)
        .with(stdout_layer)
        .init();
    logger_provider
}

fn init_metrics() -> SdkMeterProvider {
    let otlp_metric_exporter = MetricExporter::builder().with_tonic().build().unwrap();

    let meter_provider = SdkMeterProvider::builder()
        // .with_periodic_exporter(exporter)
        .with_periodic_exporter(otlp_metric_exporter)
        .with_resource(RESOURCE.clone())
        .build();
    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

fn init_tracer() -> SdkTracerProvider {
    let otlp_span_exporter = SpanExporter::builder().with_tonic().build().unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(otlp_span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    tracer_provider
}

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
    }

    // exit
    std::process::exit(if err.is_ok() { 0 } else { 1 });
}

/// Asynchronous main function
async fn application_main(args: Args) -> anyhow::Result<()> {
    let tracer_provider = init_tracer();
    let metrics_provider = init_metrics();
    let logger_provider = init_logger();

    // connect to the database
    let db = database::connect(args.db, &args.username, &args.password)
        .await
        .context("Failed to connect to the database")?;

    let jetstream_hosts = vec![
        "jetstream1.us-west.bsky.network",
        "jetstream2.us-east.bsky.network",
        "test-jetstream.skyfeed.moe",
        "jetstream2.us-west.bsky.network",
        "jetstream1.us-east.bsky.network",
    ];

    for host in jetstream_hosts {
        let db_clone = db.clone();
        let certificate = args.certificate.clone();
        let (name, _) = host.split_at(18);
        std::thread::Builder::new()
            .name(format!("{}", name))
            .spawn(move || {
                Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async {
                        start_jetstream_consumer(db_clone, host.to_string(), certificate)
                            .await
                            .context("jetstream consumer failed")
                            .unwrap();
                    });
            })
            .context("Failed to spawn jetstream consumer thread")?;
    }

    let db_clone = db.clone();
    tokio::spawn(async move {
        let mut last_count = 0;
        loop {
            let mut res = db_clone
                .query("SELECT count() as c FROM li_did GROUP ALL;")
                .await
                .unwrap();
            let count: Option<i64> = res.take((0, "c")).unwrap();
            info!(
                "fully indexed repo count: {} with {} repos/10s",
                count.unwrap_or(0),
                count.unwrap_or(0) - last_count
            );
            last_count = count.unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
        }
    });

    if args.mode == "full" {
        start_full_repo_indexer(db, args.max_tasks.unwrap_or(num_cpus::get() * 50)).await?;
    } else {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
    }

    // TODO: Also handle shutdown when an error occurs
    tracer_provider.shutdown().unwrap();
    metrics_provider.shutdown().unwrap();
    logger_provider.shutdown().unwrap();

    Ok(())
}

async fn start_jetstream_consumer(
    db: Surreal<Any>,
    host: String,
    certificate: String,
) -> anyhow::Result<()> {
    // fetch initial cursor
    let cursor = database::fetch_cursor(&db, &host)
        .await
        .context("Failed to fetch cursor from database")?
        .map_or(0, |e| e.time_us);

    // enter websocket event loop
    websocket::start(host, certificate, cursor, db)
        .await
        .context("WebSocket event loop failed")?;

    Ok(())
}

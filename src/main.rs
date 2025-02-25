#![feature(type_changing_struct_update)]

use anyhow::Context;
use config::Args;
use database::repo_indexer::start_full_repo_indexer;
use metrics_reporter::export_system_metrics;
use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_resource_detectors::{
    HostResourceDetector, OsResourceDetector, ProcessResourceDetector,
};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    resource::EnvResourceDetector,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_NAME, SERVICE_VERSION},
    resource::{HOST_NAME, OS_BUILD_ID, OS_DESCRIPTION, OS_NAME, OS_VERSION, SERVICE_INSTANCE_ID},
    SCHEMA_URL,
};
use std::{
    process::exit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};
use surrealdb::{engine::any::Any, Surreal, Uuid};
use tokio::{runtime::Builder, signal::ctrl_c, time::interval_at};
use tokio_rustls::rustls::crypto::aws_lc_rs::default_provider;
use tracing::error;
use tracing_subscriber::{prelude::*, EnvFilter};

mod config;
mod database;
mod metrics_reporter;
mod websocket;

/// Override the global allocator with mimalloc
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
    let instance_id = Uuid::new_v4();

    let mut attributes = vec![
        KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        KeyValue::new(SERVICE_INSTANCE_ID, instance_id.to_string()),
        KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
    ];

    if let Ok(linux_sys_info) = sys_info::linux_os_release() {
        if let Some(build_id) = linux_sys_info.build_id {
            attributes.push(KeyValue::new(OS_BUILD_ID, build_id));
        }
        if let Some(pretty_name) = linux_sys_info.pretty_name {
            attributes.push(KeyValue::new(OS_DESCRIPTION, pretty_name));
        }
        if let Some(name) = linux_sys_info.name {
            attributes.push(KeyValue::new(OS_NAME, name));
        }
        if let Some(version_id) = linux_sys_info.version_id {
            attributes.push(KeyValue::new(OS_VERSION, version_id));
        }
    } else {
        if let Ok(os_version) = sys_info::os_release() {
            attributes.push(KeyValue::new(OS_DESCRIPTION, os_version));
        }
        if let Ok(os_name) = sys_info::os_type() {
            attributes.push(KeyValue::new(OS_NAME, os_name));
        }
    }

    if let Ok(hostname) = sys_info::hostname() {
        attributes.push(KeyValue::new(HOST_NAME, hostname));
    }

    Resource::builder()
        .with_schema_url(
            [
                KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
            ],
            SCHEMA_URL,
        )
        .with_attributes(attributes)
        .with_detectors(&[
            Box::new(EnvResourceDetector::new()),
            Box::new(HostResourceDetector::default()),
            Box::new(ProcessResourceDetector),
            Box::new(OsResourceDetector),
            // Box::new(OsResourceDetector::new()),
        ])
        .build()
});

fn init_observability() -> OtelGuard {
    let tracer_provider = init_tracer();
    let meter_provider = init_meter();
    let logger_provider = init_logger();

    let otel_log_filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_log_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_log_filter);

    let tokio_console_layer = console_subscriber::spawn();

    let stdout_filter = EnvFilter::new("info").add_directive("opentelemetry=info".parse().unwrap());
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(stdout_filter);

    let tracer = tracer_provider.tracer("tracing-otel-subscriber");
    tracing_subscriber::registry()
        .with(tokio_console_layer)
        .with(otel_log_layer)
        .with(stdout_layer)
        .with(tracing_opentelemetry::MetricsLayer::new(
            meter_provider.clone(),
        ))
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(tracer))
        .init();
    OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    }
}

fn init_logger() -> SdkLoggerProvider {
    let otlp_log_exporter = LogExporter::builder().with_tonic().build().unwrap();
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_batch_exporter(otlp_log_exporter)
        .build();
    logger_provider
}

fn init_meter() -> SdkMeterProvider {
    let otlp_metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap();

    let periodic_reader = PeriodicReader::builder(otlp_metric_exporter)
        .with_interval(std::time::Duration::from_secs(5))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_reader(periodic_reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

fn init_tracer() -> SdkTracerProvider {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_span_exporter = SpanExporter::builder().with_tonic().build().unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        .with_id_generator(RandomIdGenerator::default())
        .with_batch_exporter(otlp_span_exporter)
        // .with_simple_exporter(otlp_span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    tracer_provider
}

struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        eprintln!("Shutting down observability");
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.logger_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
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
    let otel_guard = Arc::new(init_observability());

    let handler_otel_guard = otel_guard.clone();
    tokio::spawn(async move {
        ctrl_c().await.unwrap();
        eprintln!("Preparing for unclean exit");

        handler_otel_guard.logger_provider.shutdown().unwrap();
        handler_otel_guard.meter_provider.shutdown().unwrap();
        handler_otel_guard.tracer_provider.shutdown().unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        eprintln!("Exiting");
        exit(1);
    });

    // Start exporting system metrics
    tokio::task::spawn(export_system_metrics());

    // // Create a tracing layer with the configured tracer
    // let telemetry =
    //     tracing_opentelemetry::layer().with_tracer(otel_guard.tracer_provider.tracer("testingsss"));

    // // Use the tracing subscriber `Registry`, or any other subscriber
    // // that impls `LookupSpan`
    // let subscriber = Registry::default().with(telemetry);
    // // Trace executed code
    // tracing::subscriber::with_default(subscriber, || {
    //     // Spans will be sent to the configured OpenTelemetry exporter
    //     let root = span!(tracing::Level::TRACE, "app_start", work_units = 2);
    //     let _enter = root.enter();

    //     error!("This event will be logged in the root span.");
    // });

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
        let indexed_repos_counter = global::meter("indexer")
            .u64_counter("indexer.repos.indexed")
            .with_description("Total number of indexed repos")
            .with_unit("{repo}")
            .build();
        let mut last_count: u64 = 0;
        let mut interval = interval_at(
            tokio::time::Instant::now(),
            tokio::time::Duration::from_secs(2),
        );
        loop {
            let mut res = db_clone
                .query("SELECT count() as c FROM li_did GROUP ALL;")
                .await
                .unwrap();
            let count: Option<i64> = res.take((0, "c")).unwrap();
            let count = count.unwrap_or(last_count as i64) as u64;

            indexed_repos_counter.add(count - last_count, &[]);
            last_count = count;

            interval.tick().await;
        }
    });

    if args.mode == "full" {
        start_full_repo_indexer(&db).await?;
    } else {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
    }

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

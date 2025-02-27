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
    sync::{Arc, LazyLock},
};
use surrealdb::Uuid;
use tokio::signal::ctrl_c;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

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

pub async fn init_observability() -> Arc<OtelGuard> {
    let tracer_provider = init_tracer();
    let meter_provider = init_meter();
    let logger_provider = init_logger();

    // Exports logs to otel
    let otel_log_filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_log_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_log_filter);

    // Exports tokio stats for tokio-console
    let tokio_console_layer = console_subscriber::spawn();

    // Prints logs to stdout
    // let stdout_filter = EnvFilter::new("info").add_directive("opentelemetry=info".parse().unwrap());
    // let stdout_layer = tracing_subscriber::fmt::layer()
    //     .with_thread_names(true)
    //     .with_filter(stdout_filter);

    // Exports tracing traces to opentelemetry
    let tracing_filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let tracer = tracer_provider.tracer("tracing-otel-subscriber");
    let tracing_layer =
        tracing_opentelemetry::OpenTelemetryLayer::new(tracer).with_filter(tracing_filter);

    // Add all layers
    tracing_subscriber::registry()
        .with(tokio_console_layer)
        .with(otel_log_layer)
        // .with(stdout_layer)
        .with(tracing_opentelemetry::MetricsLayer::new(
            meter_provider.clone(),
        ))
        .with(tracing_layer)
        .init();

    // TODO: Replace this hacky mess with something less broken
    let guard = Arc::new(OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    });
    let handler_otel_guard = guard.clone();
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
    guard
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
        .with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
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

pub struct OtelGuard {
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

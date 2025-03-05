use crate::config::ARGS;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
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
    resource::{HOST_NAME, OS_BUILD_ID, OS_DESCRIPTION, OS_NAME, OS_VERSION},
    SCHEMA_URL,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    LazyLock, Mutex,
};
use tracing::Subscriber;
use tracing_subscriber::{registry::LookupSpan, EnvFilter, Layer};

const RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
    // let instance_id = Uuid::new_v4();

    let mut attributes = vec![
        KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        // KeyValue::new(SERVICE_INSTANCE_ID, instance_id.to_string()),
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

fn init_logger() -> Option<SdkLoggerProvider> {
    if !ARGS.otel_logs.unwrap_or(true) {
        return None;
    }
    let otlp_log_exporter = LogExporter::builder().with_tonic().build().unwrap();
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_batch_exporter(otlp_log_exporter)
        .build();

    Some(logger_provider)
}

fn init_meter() -> Option<SdkMeterProvider> {
    if !ARGS.otel_metrics.unwrap_or(true) {
        return None;
    }
    let otlp_metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::Cumulative)
        .build()
        .unwrap();

    let periodic_reader = PeriodicReader::builder(otlp_metric_exporter)
        .with_interval(std::time::Duration::from_secs(5))
        .build();

    let meter_provider_builder = SdkMeterProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_reader(periodic_reader);
    let meter_provider = meter_provider_builder.build();
    global::set_meter_provider(meter_provider.clone());
    Some(meter_provider)
}

fn init_tracer() -> Option<SdkTracerProvider> {
    if !ARGS.otel_tracing.unwrap_or(true) {
        return None;
    }
    global::set_text_map_propagator(TraceContextPropagator::new());
    let otlp_span_exporter = SpanExporter::builder().with_tonic().build().unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        .with_id_generator(RandomIdGenerator::default())
        .with_batch_exporter(otlp_span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Some(tracer_provider)
}

/// Manages the lifetimes of the opentelemetry providers
pub struct OtelProviders {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    /// Flag to indicate if the observability providers have been shutdown
    shutdown: Mutex<bool>,
}

impl OtelProviders {
    /// Create a new set of observability providers
    ///
    /// Will panic if called more than once to prevent double initialization
    ///
    /// The providers will be shutdown automatically when the last reference to this struct is dropped
    pub fn new() -> Self {
        static ALREADY_INITIALIZED: AtomicBool = AtomicBool::new(false);
        if ALREADY_INITIALIZED.fetch_and(true, Ordering::SeqCst) {
            panic!("OtelProviders::new() called more than once");
        }

        let tracer_provider = init_tracer();
        let meter_provider = init_meter();
        let logger_provider = init_logger();

        Self {
            tracer_provider,
            meter_provider,
            logger_provider,
            shutdown: Mutex::new(false),
        }
    }

    /// Shutdown the observability providers
    ///
    /// Does nothing if already shutdown
    pub fn shutdown(&self) {
        let shutdown = self.shutdown.lock();
        if shutdown.as_ref().map_or(false, |shutdown| **shutdown) {
            // Already shutdown
            return;
        }
        eprintln!("Shutting down observability");
        if let Some(tracer_provider) = &self.tracer_provider {
            if let Err(err) = tracer_provider.shutdown() {
                eprintln!("Error shutting down otel tracer: {err:?}");
            }
        }
        if let Some(meter_provider) = &self.meter_provider {
            if let Err(err) = meter_provider.shutdown() {
                eprintln!("Error shutting down otel meter: {err:?}");
            }
        }
        if let Some(logger_provider) = &self.logger_provider {
            if let Err(err) = logger_provider.shutdown() {
                eprintln!("Error shutting down otel logger: {err:?}");
            }
        }
        if let Ok(mut shutdown) = shutdown {
            // Mark as shutdown
            *shutdown = true;
        }
    }

    /// Returns a layer that exports tracing spans to opentelemetry if otel-tracing is enabled
    fn otel_tracer_layer<S>(&self) -> Option<impl Layer<S>>
    where
        S: Subscriber + Sync + Send + for<'span> LookupSpan<'span>,
    {
        let Some(tracer_provider) = &self.tracer_provider else {
            return None;
        };
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
        Some(tracing_layer)
    }

    /// Returns a layer that exports logs to opentelemetry if otel-logs is enabled
    fn otel_logger_layer<S>(&self) -> Option<impl Layer<S>>
    where
        S: Subscriber + Sync + Send + for<'span> LookupSpan<'span>,
    {
        let Some(logger_provider) = &self.logger_provider else {
            return None;
        };
        // Exports logs to otel
        let otel_log_filter = EnvFilter::new("info")
            .add_directive("hyper=off".parse().unwrap())
            .add_directive("h2=off".parse().unwrap())
            .add_directive("opentelemetry=off".parse().unwrap())
            .add_directive("tonic=off".parse().unwrap())
            .add_directive("reqwest=off".parse().unwrap());
        let otel_log_layer =
            OpenTelemetryTracingBridge::new(logger_provider).with_filter(otel_log_filter);

        Some(otel_log_layer)
    }

    /// Returns a layer that exports tracing metrics to opentelemetry if otel-metrics is enabled
    fn otel_metrics_layer<S>(&self) -> Option<impl Layer<S>>
    where
        S: Subscriber + Sync + Send + for<'span> LookupSpan<'span>,
    {
        let Some(meter_provider) = &self.meter_provider else {
            return None;
        };

        Some(tracing_opentelemetry::MetricsLayer::new(
            meter_provider.clone(),
        ))
    }

    /// Get a tracing layer for otel logging, tracing, and metrics
    pub fn tracing_layers<S>(&self) -> impl Layer<S>
    where
        S: Subscriber + Sync + Send + for<'span> LookupSpan<'span>,
    {
        let mut layers: Vec<Box<dyn Layer<S> + Send + Sync + 'static>> = vec![];
        if let Some(tracer_layer) = self.otel_tracer_layer() {
            layers.push(Box::new(tracer_layer));
        }
        if let Some(logger_layer) = self.otel_logger_layer() {
            layers.push(Box::new(logger_layer));
        }
        if let Some(metrics_layer) = self.otel_metrics_layer() {
            layers.push(Box::new(metrics_layer));
        }
        return layers;
    }
}

impl Drop for OtelProviders {
    fn drop(&mut self) {
        self.shutdown();
    }
}

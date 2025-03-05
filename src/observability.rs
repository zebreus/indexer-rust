use crate::config::ARGS;
use console_subscriber::ConsoleLayer;
use otel_providers::OtelProviders;
use std::{process::exit, sync::Arc};
use tokio::signal::ctrl_c;
use tracing::Subscriber;
use tracing_subscriber::{
    layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt, EnvFilter, Layer,
};

mod otel_providers;

/// Layer for enabling tokio-console
pub fn tokio_console_layer<S>() -> Option<impl Layer<S>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    if !ARGS.tokio_console {
        return None;
    }
    Some(ConsoleLayer::builder().with_default_env().spawn())
}

/// Layer for stdout
pub fn stdout_layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let stdout_filter = EnvFilter::new("info").add_directive("opentelemetry=info".parse().unwrap());
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(stdout_filter);
    Box::new(stdout_layer)
}

pub async fn init_observability() -> Arc<OtelProviders> {
    let otel_providers = Arc::new(OtelProviders::new());

    // Initialize the tracing subscribers
    tracing_subscriber::registry()
        .with(stdout_layer())
        .with(tokio_console_layer())
        .with(otel_providers.tracing_layers())
        .init();

    let handler_otel_providers = otel_providers.clone();
    tokio::task::Builder::new()
        .name("Observability shutdown hook")
        .spawn(async move {
            // TODO: Properly manage application shutdown
            ctrl_c().await.unwrap();
            eprintln!("Preparing for unclean exit");

            handler_otel_providers.shutdown();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            eprintln!("Exiting");
            exit(1);
        })
        .unwrap();
    otel_providers
}

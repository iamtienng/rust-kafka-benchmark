use opentelemetry_appender_tracing::layer;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use tracing_subscriber::{EnvFilter, prelude::*};

/// Initialize logging and tracing infrastructure
pub fn init_logging() -> SdkLoggerProvider {
    let exporter = opentelemetry_stdout::LogExporter::default();

    let provider = SdkLoggerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("kafka-rust-benchmark")
                .build(),
        )
        .with_simple_exporter(exporter)
        .build();

    // Filter for OpenTelemetry layer (only errors, suppress noisy dependencies)
    let otel_filter = EnvFilter::new("error")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());

    let otel_layer = layer::OpenTelemetryTracingBridge::new(&provider).with_filter(otel_filter);

    // Filter for fmt layer (info level, with debug for opentelemetry)
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap()
        .add_directive("opentelemetry=debug".parse().unwrap());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_target(false)
        .with_filter(fmt_filter);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    provider
}

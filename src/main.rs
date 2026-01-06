mod config;
mod consumer;
mod producer;
mod types;

use crate::config::Config;
use crate::consumer::run_consumer;
use crate::producer::run_producer;

use futures::future::join_all;
use std::sync::Arc;
use tokio::signal;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

use opentelemetry_appender_tracing::layer;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use tracing::info;
use tracing_subscriber::{EnvFilter, prelude::*};

#[tokio::main]
async fn main() {
    let exporter = opentelemetry_stdout::LogExporter::default();
    let provider: SdkLoggerProvider = SdkLoggerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("log-appender-tracing-example")
                .build(),
        )
        .with_simple_exporter(exporter)
        .build();
    let filter_otel = EnvFilter::new("error")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_layer = layer::OpenTelemetryTracingBridge::new(&provider).with_filter(filter_otel);
    let filter_fmt = EnvFilter::new("info").add_directive("opentelemetry=debug".parse().unwrap());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(filter_fmt);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    let cfg = Arc::new(Config::from_env());

    info!("Starting Kafka benchmark with config: {:?}", cfg);

    let shutdown = Arc::new(Notify::new());
    let throughput = Arc::new(RwLock::new(cfg.throughput));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    // Producers
    let producer_msg_counter = Arc::new(AtomicUsize::new(0));
    for i in 0..cfg.producer_num_threads {
        handles.push(tokio::spawn(run_producer(
            i,
            cfg.clone(),
            throughput.clone(),
            shutdown.clone(),
            error_count.clone(),
            producer_msg_counter.clone(),
        )));
    }

    // Consumers
    let consumer_msg_counter = Arc::new(AtomicUsize::new(0));
    for i in 0..cfg.consumer_num_threads {
        handles.push(tokio::spawn(run_consumer(
            i,
            cfg.clone(),
            shutdown.clone(),
            error_count.clone(),
            consumer_msg_counter.clone(),
        )));
    }

    handles.push(tokio::spawn(async move {
        let mut prev_produced = 0usize;
        let mut prev_consumed = 0usize;
        let mut prev_error = 0usize;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let current = producer_msg_counter.load(Ordering::Relaxed);
            let diff = current - prev_produced;
            prev_produced = current;

            let mps_producer = diff as i32;

            let current = consumer_msg_counter.load(Ordering::Relaxed);
            let diff = current - prev_consumed;
            prev_consumed = current;

            let mps_consumer = diff as i32;

            let current = error_count.load(Ordering::Relaxed);
            let diff = current - prev_error;
            prev_error = current;

            let eps = diff as i32;

            info!(
                "Produced {:.2} messages/sec. Consumed {:.2} messages/sec. Errors: {:.2}.",
                mps_producer, mps_consumer, eps
            );
        }
    }));

    // CTRL+C
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Ctrl+C — Stopping benchmark.");
            let _ = provider.shutdown();
            shutdown.notify_waiters();
        }
    }

    join_all(handles).await;
}

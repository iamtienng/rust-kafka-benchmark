mod config;
mod consumer;
mod error;
mod logging;
mod metrics;
mod producer;

use crate::config::Config;
use crate::consumer::KafkaConsumer;
use crate::logging::init_logging;
use crate::metrics::Metrics;
use crate::producer::Producer;

use std::sync::Arc;
use tokio::signal;
use tokio::sync::{Notify, RwLock};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    // Initialize logging
    let log_provider = init_logging();

    // Load configuration
    let config = match Config::from_env() {
        Ok(cfg) => Arc::new(cfg),
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    info!("Starting Kafka benchmark");
    info!("Bootstrap servers: {}", config.bootstrap);
    info!("Topic: {}", config.topic);
    info!("Producer threads: {}", config.producer_num_threads);
    info!("Consumer threads: {}", config.consumer_num_threads);
    info!("Message size: {} bytes", config.msg_size);
    info!("Target throughput: {} msg/sec", config.throughput);

    // Initialize shared state
    let shutdown = Arc::new(Notify::new());
    let throughput = Arc::new(RwLock::new(config.throughput));
    let metrics = Metrics::new();

    // Spawn workers
    let mut handles = Vec::new();

    // Spawn producers
    for i in 0..config.producer_num_threads {
        let producer = Producer::new(
            i,
            config.clone(),
            throughput.clone(),
            shutdown.clone(),
            metrics.clone(),
        );

        handles.push(tokio::spawn(async move {
            if let Err(e) = producer.run().await {
                error!("Producer {} error: {}", i, e);
            }
        }));
    }

    // Spawn consumers
    for i in 0..config.consumer_num_threads {
        let consumer = KafkaConsumer::new(i, config.clone(), shutdown.clone(), metrics.clone());

        handles.push(tokio::spawn(async move {
            if let Err(e) = consumer.run().await {
                error!("Consumer {} error: {}", i, e);
            }
        }));
    }

    // Spawn metrics reporter
    let metrics_task = tokio::spawn(metrics.clone().start_reporter());

    // Wait for shutdown signal
    info!("Benchmark running. Press Ctrl+C to stop.");

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Received Ctrl+C, shutting down...");
        }
        Err(e) => {
            error!("Failed to listen for Ctrl+C: {}", e);
        }
    }

    // Trigger shutdown
    shutdown.notify_waiters();

    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.await;
    }

    // Stop metrics reporter
    metrics_task.abort();

    // Shutdown logging
    if let Err(e) = log_provider.shutdown() {
        eprintln!("Failed to shutdown log provider: {}", e);
    }

    info!("Benchmark stopped");
}

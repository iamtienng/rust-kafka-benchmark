mod config;
mod consumer;
mod monitor;
mod producer;
mod throughput;
mod types;

use crate::config::Config;
use crate::consumer::run_consumer;
use crate::monitor::monitor_errors;
use crate::producer::run_producer;
use crate::throughput::adjust_throughput_loop;

use futures::future::join_all;
use log::info;
use std::sync::Arc;
use tokio::signal;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

#[tokio::main]
async fn main() {
    env_logger::init();
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

    // Throughput loop
    if cfg.auto_increase {
        handles.push(tokio::spawn(adjust_throughput_loop(
            throughput.clone(),
            shutdown.clone(),
        )));
    }

    // Error monitor
    handles.push(tokio::spawn(monitor_errors(
        shutdown.clone(),
        error_count.clone(),
    )));

    handles.push(tokio::spawn(async move {
        let mut prev_produced = 0usize;
        let mut prev_consumed = 0usize;
        let mut prev_error = 0usize;

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let current = producer_msg_counter.load(Ordering::Relaxed);
            let diff = current - prev_produced;
            prev_produced = current;

            let mps_producer = diff as f64 / 5.0;

            let current = consumer_msg_counter.load(Ordering::Relaxed);
            let diff = current - prev_consumed;
            prev_consumed = current;

            let mps_consumer = diff as f64 / 5.0;

            let current = error_count.load(Ordering::Relaxed);
            let diff = current - prev_error;
            prev_error = current;

            let eps = diff as f64 / 5.0;

            info!(
                "Produced {:.2} messages/sec (last 5s). Consumed {:.2} messages/sec (last 5s). Errors: {:.2}.",
                mps_producer, mps_consumer, eps
            );
        }
    }));

    // CTRL+C
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Ctrl+C — Stopping benchmark.");
            shutdown.notify_waiters();
        }
    }

    join_all(handles).await;

    info!("=== Kafka Benchmark Finished ===");
}

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

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, RwLock};

#[tokio::main]
async fn main() {
    env_logger::init();
    let cfg = Arc::new(Config::from_env());

    info!("Starting Kafka benchmark with config: {:?}", cfg);

    let half = cfg.num_threads / 2;

    let shutdown = Arc::new(Notify::new());
    let throughput = Arc::new(RwLock::new(cfg.throughput));
    let error_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Producers
    for i in 0..half {
        handles.push(tokio::spawn(run_producer(
            i,
            cfg.clone(),
            throughput.clone(),
            shutdown.clone(),
            error_count.clone(),
        )));
    }

    // Consumers
    for i in 0..half {
        handles.push(tokio::spawn(run_consumer(
            i,
            cfg.clone(),
            shutdown.clone(),
            error_count.clone(),
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

    // CTRL+C
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Ctrl+C — Stopping benchmark.");
            shutdown.notify_waiters();
        }
    }

    join_all(handles).await;

    info!("=== Kafka Benchmark Finished ===");
    info!("Final throughput: {} msg/s", *throughput.read().await);
    info!("Final error_count: {}", error_count.load(Ordering::Relaxed));
}

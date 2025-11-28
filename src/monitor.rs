use crate::types::*;
use log::error;
use std::time::Duration;

pub async fn monitor_errors(shutdown: SharedNotify, error_count: SharedErrors) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 50 {
            error!("❌ Kafka BREAK detected — error_count={}", errors);
            shutdown.notify_waiters();
            return;
        }
    }
}

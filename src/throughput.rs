use crate::types::*;
use std::time::Duration;
use tracing::info;

pub async fn adjust_throughput_loop(throughput: SharedThroughput, shutdown: SharedNotify) {
    loop {
        tokio::select! {
            _ = shutdown.notified() => return,

            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                let mut tp = throughput.write().await;
                *tp *= 2;
                info!("Increasing throughput -> {} msg/s", *tp);
            }
        }
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::info;

/// Tracks metrics for producers and consumers
#[derive(Clone)]
pub struct Metrics {
    producer_count: Arc<AtomicUsize>,
    consumer_count: Arc<AtomicUsize>,
    error_count: Arc<AtomicUsize>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            producer_count: Arc::new(AtomicUsize::new(0)),
            consumer_count: Arc::new(AtomicUsize::new(0)),
            error_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn increment_produced(&self) {
        self.producer_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_consumed(&self) {
        self.consumer_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_errors(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_producer_count(&self) -> usize {
        self.producer_count.load(Ordering::Relaxed)
    }

    pub fn get_consumer_count(&self) -> usize {
        self.consumer_count.load(Ordering::Relaxed)
    }

    pub fn get_error_count(&self) -> usize {
        self.error_count.load(Ordering::Relaxed)
    }

    /// Start a background task that logs metrics periodically
    pub async fn start_reporter(self) {
        let mut prev_produced = 0usize;
        let mut prev_consumed = 0usize;
        let mut prev_errors = 0usize;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let current_produced = self.get_producer_count();
            let produced_rate = current_produced.saturating_sub(prev_produced);
            prev_produced = current_produced;

            let current_consumed = self.get_consumer_count();
            let consumed_rate = current_consumed.saturating_sub(prev_consumed);
            prev_consumed = current_consumed;

            let current_errors = self.get_error_count();
            let error_rate = current_errors.saturating_sub(prev_errors);
            prev_errors = current_errors;

            info!(
                produced_per_sec = produced_rate,
                consumed_per_sec = consumed_rate,
                errors_per_sec = error_rate,
                total_produced = current_produced,
                total_consumed = current_consumed,
                total_errors = current_errors,
                "Metrics report"
            );
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

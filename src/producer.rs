use crate::config::Config;
use crate::error::Result;
use crate::metrics::Metrics;

use base64::{Engine, engine::general_purpose};
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tracing::{error, info};

/// Message payload structure
#[derive(serde::Serialize)]
struct Message {
    ts: String,
    producer_id: usize,
    payload: String,
}

/// Kafka producer worker
pub struct Producer {
    id: usize,
    config: Arc<Config>,
    throughput: Arc<RwLock<u64>>,
    shutdown: Arc<Notify>,
    metrics: Metrics,
}

impl Producer {
    pub fn new(
        id: usize,
        config: Arc<Config>,
        throughput: Arc<RwLock<u64>>,
        shutdown: Arc<Notify>,
        metrics: Metrics,
    ) -> Self {
        Self {
            id,
            config,
            throughput,
            shutdown,
            metrics,
        }
    }

    /// Create a Kafka producer client
    fn create_producer(&self) -> Result<FutureProducer> {
        let mut client = ClientConfig::new();

        self.config.apply_to_client(&mut client);

        client
            .set("message.timeout.ms", "5000")
            .set("compression.type", "lz4");

        Ok(client.create()?)
    }

    /// Generate a message payload
    fn create_message(&self) -> String {
        let now = Utc::now().to_rfc3339();
        let payload_bytes = vec![0u8; self.config.msg_size];
        let payload_b64 = general_purpose::STANDARD.encode(&payload_bytes);

        let msg = Message {
            ts: now,
            producer_id: self.id,
            payload: payload_b64,
        };

        json!(msg).to_string()
    }

    /// Run the producer loop
    pub async fn run(self) -> Result<()> {
        let producer = self.create_producer()?;
        info!("Producer {} started", self.id);

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("Producer {} stopped", self.id);
                    return Ok(());
                }

                _ = self.produce_message(&producer) => {}
            }
        }
    }

    async fn produce_message(&self, producer: &FutureProducer) {
        // Calculate delay based on throughput
        let throughput = *self.throughput.read().await;
        let delay = Duration::from_micros(1_000_000 / throughput);

        let payload = self.create_message();
        let key = format!("producer-{}", self.id);

        let record = FutureRecord::to(&self.config.topic)
            .payload(&payload)
            .key(&key);

        match producer.send(record, Duration::from_secs(1)).await {
            Ok(_) => {
                self.metrics.increment_produced();
            }
            Err((err, _)) => {
                error!(producer_id = self.id, error = %err, "Failed to send message");
                self.metrics.increment_errors();
            }
        }

        tokio::time::sleep(delay).await;
    }
}

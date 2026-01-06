use crate::config::Config;
use crate::error::Result;
use crate::metrics::Metrics;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{error, info};

/// Kafka consumer worker
pub struct KafkaConsumer {
    id: usize,
    config: Arc<Config>,
    shutdown: Arc<Notify>,
    metrics: Metrics,
}

impl KafkaConsumer {
    pub fn new(id: usize, config: Arc<Config>, shutdown: Arc<Notify>, metrics: Metrics) -> Self {
        Self {
            id,
            config,
            shutdown,
            metrics,
        }
    }

    /// Create a Kafka consumer client
    fn create_consumer(&self) -> Result<StreamConsumer> {
        let mut client = ClientConfig::new();

        self.config.apply_to_client(&mut client);

        client
            .set("group.id", format!("bench-group-{}", self.id))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest");

        let consumer: StreamConsumer = client.create()?;
        consumer.subscribe(&[&self.config.topic])?;

        Ok(consumer)
    }

    /// Run the consumer loop
    pub async fn run(self) -> Result<()> {
        let consumer = self.create_consumer()?;
        info!("Consumer {} started", self.id);

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("Consumer {} stopped", self.id);
                    return Ok(());
                }

                msg_result = consumer.recv() => {
                    self.handle_message(msg_result);
                }
            }
        }
    }

    fn handle_message(
        &self,
        msg_result: std::result::Result<
            rdkafka::message::BorrowedMessage,
            rdkafka::error::KafkaError,
        >,
    ) {
        match msg_result {
            Ok(msg) => {
                if let Some(payload_bytes) = msg.payload() {
                    match std::str::from_utf8(payload_bytes) {
                        Ok(_payload) => {
                            self.metrics.increment_consumed();
                        }
                        Err(e) => {
                            error!(
                                consumer_id = self.id,
                                error = %e,
                                "Invalid UTF-8 in message payload"
                            );
                            self.metrics.increment_errors();
                        }
                    }
                }
            }
            Err(e) => {
                error!(consumer_id = self.id, error = %e, "Failed to receive message");
                self.metrics.increment_errors();
            }
        }
    }
}

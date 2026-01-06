use thiserror::Error;

#[derive(Error, Debug)]
pub enum BenchmarkError {
    #[error("Kafka configuration error: {0}")]
    KafkaConfig(String),

    #[error("Kafka client creation failed: {0}")]
    KafkaClient(#[from] rdkafka::error::KafkaError),

    #[error("Message send failed: {0}")]
    MessageSend(String),

    #[error("Message receive failed: {0}")]
    MessageReceive(String),

    #[error("Invalid UTF-8 in message payload")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("Configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, BenchmarkError>;

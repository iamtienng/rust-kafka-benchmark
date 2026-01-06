use crate::error::{BenchmarkError, Result};
use rdkafka::config::ClientConfig;
use std::env;

/// Configuration for the Kafka benchmark application
#[derive(Debug, Clone)]
pub struct Config {
    pub bootstrap: String,
    pub topic: String,
    pub ssl_config: Option<SslConfig>,
    pub sasl_config: Option<SaslConfig>,
    pub producer_num_threads: usize,
    pub consumer_num_threads: usize,
    pub msg_size: usize,
    pub throughput: u64,
}

#[derive(Debug, Clone)]
pub struct SslConfig {
    pub ca_location: String,
    pub cert_location: Option<String>,
    pub key_location: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SaslConfig {
    pub username: String,
    pub password: String,
    pub mechanism: String,
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let bootstrap = env::var("BOOTSTRAP_SERVERS")
            .map_err(|_| BenchmarkError::Config("BOOTSTRAP_SERVERS not set".into()))?;

        let topic =
            env::var("TOPIC").map_err(|_| BenchmarkError::Config("TOPIC not set".into()))?;

        let ssl_config = Self::load_ssl_config();
        let sasl_config = Self::load_sasl_config();

        let producer_num_threads = Self::parse_env("PRODUCER_NUM_THREADS", 4)?;
        let consumer_num_threads = Self::parse_env("CONSUMER_NUM_THREADS", 1)?;
        let msg_size = Self::parse_env("MSG_SIZE", 200)?;
        let throughput = Self::parse_env("THROUGHPUT", 10_000)?;

        Ok(Self {
            bootstrap,
            topic,
            ssl_config,
            sasl_config,
            producer_num_threads,
            consumer_num_threads,
            msg_size,
            throughput,
        })
    }

    /// Apply this configuration to a Kafka ClientConfig
    pub fn apply_to_client(&self, client: &mut ClientConfig) {
        client.set("bootstrap.servers", &self.bootstrap);

        // Apply SSL configuration
        if let Some(ssl) = &self.ssl_config {
            client
                .set("security.protocol", "SSL")
                .set("ssl.ca.location", &ssl.ca_location);

            if let (Some(cert), Some(key)) = (&ssl.cert_location, &ssl.key_location) {
                client
                    .set("ssl.certificate.location", cert)
                    .set("ssl.key.location", key);
            }
        }

        // Apply SASL configuration
        if let Some(sasl) = &self.sasl_config {
            client
                .set("security.protocol", "SASL_SSL")
                .set("sasl.username", &sasl.username)
                .set("sasl.password", &sasl.password)
                .set("sasl.mechanism", &sasl.mechanism);
        }
    }

    fn load_ssl_config() -> Option<SslConfig> {
        env::var("SSL_CA_LOCATION")
            .ok()
            .map(|ca_location| SslConfig {
                ca_location,
                cert_location: env::var("SSL_CERT_LOCATION").ok(),
                key_location: env::var("SSL_KEY_LOCATION").ok(),
            })
    }

    fn load_sasl_config() -> Option<SaslConfig> {
        match (
            env::var("SASL_USERNAME").ok(),
            env::var("SASL_PASSWORD").ok(),
            env::var("SASL_MECHANISM").ok(),
        ) {
            (Some(username), Some(password), Some(mechanism)) => Some(SaslConfig {
                username,
                password,
                mechanism,
            }),
            _ => None,
        }
    }

    fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> Result<T> {
        env::var(key)
            .ok()
            .and_then(|s| s.parse().ok())
            .or(Some(default))
            .ok_or_else(|| BenchmarkError::Config(format!("Failed to parse {}", key)))
    }
}

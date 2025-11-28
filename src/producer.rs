use crate::config::Config;
use crate::types::*;

use base64::{Engine, engine::general_purpose};
use chrono::Utc;
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub async fn run_producer(
    id: usize,
    cfg: Arc<Config>,
    throughput: SharedThroughput,
    shutdown: SharedNotify,
    error_count: SharedErrors,
) {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("message.timeout.ms", "5000")
        .set("compression.type", "lz4");

    // Optional TLS
    if let Some(ca) = &cfg.ssl_ca {
        client
            .set("security.protocol", "SSL")
            .set("ssl.ca.location", ca);
        if let (Some(cert), Some(key)) = (&cfg.ssl_cert, &cfg.ssl_key) {
            client
                .set("ssl.certificate.location", cert)
                .set("ssl.key.location", key);
        }
    }

    // Optional SASL
    if let (Some(user), Some(pass), Some(mech)) =
        (&cfg.sasl_username, &cfg.sasl_password, &cfg.sasl_mechanism)
    {
        client
            .set("security.protocol", "SASL_SSL")
            .set("sasl.username", user)
            .set("sasl.password", pass)
            .set("sasl.mechanism", mech);
    }

    let producer: FutureProducer = client.create().expect("producer failed");

    let payload_bytes = vec![0u8; cfg.msg_size];

    info!("Producer {id} started.");

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                info!("Producer {id} stopped.");
                return;
            }

            _ = async {
                let tp = *throughput.read().await;
                let pause = Duration::from_micros(1_000_000 / tp);

                let now = Utc::now().to_rfc3339();
                let payload_b64 = general_purpose::STANDARD.encode(&payload_bytes);

                let body = json!({
                    "ts": now,
                    "producer_id": id,
                    "payload": payload_b64
                });

                let payload = body.to_string();
                let key = format!("producer-{id}");

                let record = FutureRecord::to(&cfg.topic)
                    .payload(&payload)
                    .key(&key);

                match producer.send(record, Duration::from_secs(1)).await {
                    Ok(_) => {}
                    Err((err, _)) => {
                        error!("Producer {id} send error: {}", err);
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                tokio::time::sleep(pause).await;
            } => {}
        }
    }
}

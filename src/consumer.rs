use crate::config::Config;
use crate::types::*;

use log::{error, info};
use rdkafka::Message; // required for m.payload()
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub async fn run_consumer(
    id: usize,
    cfg: Arc<Config>,
    shutdown: SharedNotify,
    error_count: SharedErrors,
    msg_counter: SharedMsgs,
) {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", format!("bench-group-{id}"))
        .set("enable.auto.commit", "true");

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

    let consumer: StreamConsumer = client.create().expect("consumer failed");
    consumer.subscribe(&[&cfg.topic]).expect("cannot subscribe");

    info!("Consumer {id} started.");

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                info!("Consumer {id} stopped.");
                return;
            }

            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        if let Some(payload_bytes) = m.payload() {
                            match std::str::from_utf8(payload_bytes) {
                                Ok(_payload) => {
                                    msg_counter.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    error!("Consumer {id} invalid UTF-8 payload: {}", e);
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Consumer {id} poll error: {}", e);
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

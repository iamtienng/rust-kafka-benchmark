use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub bootstrap: String,
    pub topic: String,
    pub ssl_ca: Option<String>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub num_threads: usize,
    pub msg_size: usize,
    pub throughput: u64,
    pub auto_increase: bool,
}

impl Config {
    pub fn from_env() -> Self {
        let bootstrap = env::var("BOOTSTRAP_SERVERS").unwrap_or_else(|_| "".into());
        let topic = env::var("TOPIC").unwrap_or_else(|_| "".into());

        let ssl_ca = env::var("SSL_CA_LOCATION").ok();
        let ssl_cert = env::var("SSL_CERT_LOCATION").ok();
        let ssl_key = env::var("SSL_KEY_LOCATION").ok();

        let sasl_username = env::var("SASL_USERNAME").ok();
        let sasl_password = env::var("SASL_PASSWORD").ok();
        let sasl_mechanism = env::var("SASL_MECHANISM").ok();

        let num_threads = env::var("NUM_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);

        let msg_size = env::var("MSG_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);

        let throughput = env::var("THROUGHPUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000);

        let auto_increase = env::var("AUTO_INCREASE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        Self {
            bootstrap,
            topic,
            ssl_ca,
            ssl_cert,
            ssl_key,
            sasl_username,
            sasl_password,
            sasl_mechanism,
            num_threads,
            msg_size,
            throughput,
            auto_increase,
        }
    }
}

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use tokio::sync::{Notify, RwLock};

pub type SharedNotify = Arc<Notify>;
pub type SharedThroughput = Arc<RwLock<u64>>;
pub type SharedErrors = Arc<AtomicUsize>;
pub type SharedMsgs = Arc<AtomicUsize>;

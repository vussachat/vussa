use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(crate) static AUTHENTICATIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACTIVE_WEBSOCKETS: AtomicU64 = AtomicU64::new(0);
pub(crate) static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn record_authentication() {
    AUTHENTICATIONS.fetch_add(1, Ordering::Relaxed);
}

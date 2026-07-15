use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Debug, Default)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(test)]
#[derive(Debug)]
struct FixedClock(u64);

#[cfg(test)]
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_clock_is_deterministic() {
        assert_eq!(FixedClock(1234).now_millis(), 1234);
    }
}

use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};

pub(crate) static VALKEY_COMMANDS: OnceLock<
    Arc<std::sync::RwLock<Vec<redis::aio::MultiplexedConnection>>>,
> = OnceLock::new();
pub(crate) static VALKEY_COMMAND_INDEX: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub(crate) enum CacheError {
    Connection(String),
    Command(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(message) => write!(formatter, "cache connection: {message}"),
            Self::Command(message) => write!(formatter, "cache command: {message}"),
        }
    }
}

impl std::error::Error for CacheError {}

#[async_trait]
pub(crate) trait CacheHealth: Send + Sync {
    async fn ping(&self) -> Result<(), CacheError>;
}

pub(crate) struct RedisCacheHealth {
    client: redis::Client,
}

impl RedisCacheHealth {
    pub(crate) fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CacheHealth for RedisCacheHealth {
    async fn ping(&self) -> Result<(), CacheError> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| CacheError::Connection(error.to_string()))?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(|error| CacheError::Command(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingCache;

    #[async_trait]
    impl CacheHealth for FailingCache {
        async fn ping(&self) -> Result<(), CacheError> {
            Err(CacheError::Command("injected failure".into()))
        }
    }

    #[tokio::test]
    async fn cache_failure_is_injectable() {
        assert!(FailingCache.ping().await.is_err());
    }
}

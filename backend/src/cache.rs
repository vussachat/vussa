use async_trait::async_trait;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};

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

#[derive(Clone)]
pub(crate) struct ValkeyPool {
    inner: Arc<ValkeyPoolInner>,
}

struct ValkeyPoolInner {
    client: redis::Client,
    connections: RwLock<Vec<redis::aio::MultiplexedConnection>>,
    index: AtomicUsize,
}

impl ValkeyPool {
    pub(crate) async fn new(client: redis::Client, size: usize) -> redis::RedisResult<Self> {
        let mut connections = Vec::with_capacity(size);
        for _ in 0..size {
            connections.push(client.get_multiplexed_async_connection().await?);
        }
        Ok(Self {
            inner: Arc::new(ValkeyPoolInner {
                client,
                connections: RwLock::new(connections),
                index: AtomicUsize::new(0),
            }),
        })
    }

    pub(crate) fn connection(&self) -> Result<redis::aio::MultiplexedConnection, crate::AppError> {
        let connections =
            self.inner.connections.read().map_err(|_| {
                crate::AppError::service_unavailable("cache connection unavailable")
            })?;
        if connections.is_empty() {
            return Err(crate::AppError::service_unavailable(
                "cache connection unavailable",
            ));
        }
        let index = self.inner.index.fetch_add(1, Ordering::Relaxed) % connections.len();
        Ok(connections[index].clone())
    }

    pub(crate) fn client(&self) -> redis::Client {
        self.inner.client.clone()
    }

    pub(crate) async fn recover_forever(self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let connections = match self.inner.connections.read() {
                Ok(connections) => connections.clone(),
                Err(_) => continue,
            };
            for (index, mut connection) in connections.into_iter().enumerate() {
                if redis::cmd("PING")
                    .query_async::<String>(&mut connection)
                    .await
                    .is_ok()
                {
                    continue;
                }
                if let Ok(replacement) = self.inner.client.get_multiplexed_async_connection().await
                    && let Ok(mut writable) = self.inner.connections.write()
                    && let Some(slot) = writable.get_mut(index)
                {
                    *slot = replacement;
                }
            }
        }
    }
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

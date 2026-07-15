use async_trait::async_trait;
use sqlx::PgPool;

#[derive(Debug)]
pub(crate) struct DatabaseHealthError;

#[async_trait]
pub(crate) trait DatabaseHealth: Send + Sync {
    async fn ping(&self) -> Result<(), DatabaseHealthError>;
}

pub(crate) struct PostgresDatabaseHealth {
    pool: PgPool,
}

impl PostgresDatabaseHealth {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DatabaseHealth for PostgresDatabaseHealth {
    async fn ping(&self) -> Result<(), DatabaseHealthError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|_| DatabaseHealthError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingDatabase;

    #[async_trait]
    impl DatabaseHealth for FailingDatabase {
        async fn ping(&self) -> Result<(), DatabaseHealthError> {
            Err(DatabaseHealthError)
        }
    }

    #[tokio::test]
    async fn database_failure_is_injectable() {
        assert!(FailingDatabase.ping().await.is_err());
    }
}

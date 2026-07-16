use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum NotificationDeliveryError {
    Configuration(String),
    Network(String),
}

impl std::fmt::Display for NotificationDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => {
                write!(formatter, "notification configuration: {message}")
            }
            Self::Network(message) => write!(formatter, "notification delivery: {message}"),
        }
    }
}

impl std::error::Error for NotificationDeliveryError {}

#[derive(Debug, Clone)]
pub(crate) struct NotificationTarget {
    pub(crate) user_id: Uuid,
    pub(crate) email: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) p256dh: Option<String>,
    pub(crate) auth: Option<String>,
}

#[async_trait]
pub(crate) trait NotificationSink: Send + Sync {
    async fn deliver(
        &self,
        target: &NotificationTarget,
        kind: &str,
        body: &str,
    ) -> Result<(), NotificationDeliveryError>;
}

pub(crate) struct DisabledNotificationSink;

#[async_trait]
impl NotificationSink for DisabledNotificationSink {
    async fn deliver(
        &self,
        _target: &NotificationTarget,
        _kind: &str,
        _body: &str,
    ) -> Result<(), NotificationDeliveryError> {
        Ok(())
    }
}

pub(crate) struct WebhookNotificationSink {
    client: Client,
    endpoint: String,
    channel: &'static str,
}

impl WebhookNotificationSink {
    pub(crate) fn new(
        endpoint: impl Into<String>,
        channel: &'static str,
    ) -> Result<Self, NotificationDeliveryError> {
        let endpoint = endpoint.into();
        let url = reqwest::Url::parse(&endpoint).map_err(|_| {
            NotificationDeliveryError::Configuration("notification URL is invalid".into())
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(NotificationDeliveryError::Configuration(
                "notification URL must use HTTP or HTTPS".into(),
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| NotificationDeliveryError::Configuration(error.to_string()))?;
        Ok(Self {
            client,
            endpoint,
            channel,
        })
    }

    pub(crate) fn from_env(
        variable: &str,
        channel: &'static str,
    ) -> Result<Self, NotificationDeliveryError> {
        let endpoint = std::env::var(variable).map_err(|_| {
            NotificationDeliveryError::Configuration(format!("{variable} is not configured"))
        })?;
        Self::new(endpoint, channel)
    }
}

#[async_trait]
impl NotificationSink for WebhookNotificationSink {
    async fn deliver(
        &self,
        target: &NotificationTarget,
        kind: &str,
        body: &str,
    ) -> Result<(), NotificationDeliveryError> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({
                "channel": self.channel,
                "user_id": target.user_id,
                "email": target.email,
                "subscription": {
                    "endpoint": target.endpoint,
                    "p256dh": target.p256dh,
                    "auth": target.auth
                },
                "kind": kind,
                "body": body
            }))
            .send()
            .await
            .map_err(|error| NotificationDeliveryError::Network(error.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(NotificationDeliveryError::Network(
                response.status().to_string(),
            ))
        }
    }
}

/// Deliver durable notification jobs with atomic claims so multiple replicas
/// can share the queue without normally processing the same job concurrently.
pub(crate) async fn run_notification_delivery(
    pool: PgPool,
    email_sink: Arc<dyn NotificationSink>,
    browser_sink: Arc<dyn NotificationSink>,
) {
    loop {
        let now = crate::now_millis() as i64;
        let result = sqlx::query("WITH next AS (SELECT id FROM notification_deliveries WHERE sent_at IS NULL AND next_attempt_at <= $1 AND (claimed_at IS NULL OR claimed_at < $1 - 60000) ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 32), claimed AS (UPDATE notification_deliveries d SET claimed_at=$1,attempts=d.attempts+1 FROM next WHERE d.id=next.id RETURNING d.id,d.user_id,d.email,d.channel,d.kind,d.body,d.attempts) SELECT d.id,d.user_id,d.email,d.channel,d.kind,d.body,d.attempts,s.endpoint,s.p256dh,s.auth FROM claimed d LEFT JOIN LATERAL (SELECT endpoint,p256dh,auth FROM notification_subscriptions WHERE user_id=d.user_id ORDER BY updated_at DESC,id DESC LIMIT 1) s ON d.channel='browser'")
            .bind(now)
            .fetch_all(&pool)
            .await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let id: Uuid = row.get("id");
                    let user_id: Uuid = row.get("user_id");
                    let channel: String = row.get("channel");
                    let kind: String = row.get("kind");
                    let body: String = row.get("body");
                    let attempts: i32 = row.get("attempts");
                    let target = NotificationTarget {
                        user_id,
                        email: row.get("email"),
                        endpoint: row.get("endpoint"),
                        p256dh: row.get("p256dh"),
                        auth: row.get("auth"),
                    };
                    let sink = if channel == "email" {
                        &email_sink
                    } else {
                        &browser_sink
                    };
                    match sink.deliver(&target, &kind, &body).await {
                    Ok(()) => {
                        if let Err(update_error) = sqlx::query("UPDATE notification_deliveries SET sent_at=$1,claimed_at=NULL WHERE id=$2 AND sent_at IS NULL")
                            .bind(crate::now_millis() as i64)
                            .bind(id)
                            .execute(&pool)
                            .await {
                            tracing::warn!(?update_error, %id, "notification delivery acknowledgement failed");
                        }
                    }
                    Err(error) => {
                        ::metrics::counter!("vussa_notification_delivery_failures_total", "channel" => channel.clone()).increment(1);
                        let delay = retry_delay_ms(attempts);
                        if let Err(update_error) = sqlx::query("UPDATE notification_deliveries SET claimed_at=NULL,next_attempt_at=$1,last_error=$2 WHERE id=$3 AND sent_at IS NULL")
                            .bind(crate::now_millis() as i64 + delay)
                            .bind(error.to_string())
                            .bind(id)
                            .execute(&pool)
                            .await {
                            tracing::warn!(?update_error, %id, "notification delivery retry update failed");
                        }
                    }
                }
                }
            }
            Err(error) => tracing::warn!(?error, "notification delivery claim query failed"),
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn retry_delay_ms(attempts: i32) -> i64 {
    (1_i64 << attempts.clamp(0, 12))
        .saturating_mul(1000)
        .min(60 * 60 * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingSink;

    #[async_trait]
    impl NotificationSink for FailingSink {
        async fn deliver(
            &self,
            _target: &NotificationTarget,
            _kind: &str,
            _body: &str,
        ) -> Result<(), NotificationDeliveryError> {
            Err(NotificationDeliveryError::Network(
                "injected delivery outage".into(),
            ))
        }
    }

    #[tokio::test]
    async fn disabled_sink_is_non_failing() {
        DisabledNotificationSink
            .deliver(
                &NotificationTarget {
                    user_id: Uuid::now_v7(),
                    email: "user@example.com".into(),
                    endpoint: None,
                    p256dh: None,
                    auth: None,
                },
                "mention",
                "hello",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn notification_failure_is_injectable_through_sink() {
        let result = FailingSink
            .deliver(
                &NotificationTarget {
                    user_id: Uuid::now_v7(),
                    email: "user@example.com".into(),
                    endpoint: None,
                    p256dh: None,
                    auth: None,
                },
                "mention",
                "hello",
            )
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn webhook_sink_rejects_invalid_scheme() {
        assert!(WebhookNotificationSink::new("ftp://sink", "email").is_err());
        assert!(WebhookNotificationSink::new("https://sink.example.test", "email").is_ok());
    }

    #[test]
    fn retry_delay_is_bounded_exponential_backoff() {
        assert_eq!(retry_delay_ms(0), 1_000);
        assert_eq!(retry_delay_ms(3), 8_000);
        assert_eq!(retry_delay_ms(99), 60 * 60 * 1_000);
    }
}

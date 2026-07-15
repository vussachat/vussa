use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

#[derive(Debug)]
pub(crate) enum RecoveryDeliveryError {
    Configuration(String),
    Network(String),
}

impl std::fmt::Display for RecoveryDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => write!(formatter, "recovery configuration: {message}"),
            Self::Network(message) => write!(formatter, "recovery delivery: {message}"),
        }
    }
}

impl std::error::Error for RecoveryDeliveryError {}

#[async_trait]
pub(crate) trait RecoveryNotifier: Send + Sync {
    async fn notify(&self, email: &str, token: &str) -> Result<(), RecoveryDeliveryError>;
}

pub(crate) struct DisabledRecoveryNotifier;

#[async_trait]
impl RecoveryNotifier for DisabledRecoveryNotifier {
    async fn notify(&self, _email: &str, _token: &str) -> Result<(), RecoveryDeliveryError> {
        Err(RecoveryDeliveryError::Configuration(
            "account recovery delivery is not configured".into(),
        ))
    }
}

pub(crate) struct WebhookRecoveryNotifier {
    client: Client,
    endpoint: String,
}

impl WebhookRecoveryNotifier {
    pub(crate) fn from_env() -> Result<Self, RecoveryDeliveryError> {
        let endpoint = std::env::var("RECOVERY_WEBHOOK_URL").map_err(|_| {
            RecoveryDeliveryError::Configuration("RECOVERY_WEBHOOK_URL is required".into())
        })?;
        if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
            return Err(RecoveryDeliveryError::Configuration(
                "RECOVERY_WEBHOOK_URL must use HTTP or HTTPS".into(),
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| RecoveryDeliveryError::Configuration(error.to_string()))?;
        Ok(Self { client, endpoint })
    }
}

#[async_trait]
impl RecoveryNotifier for WebhookRecoveryNotifier {
    async fn notify(&self, email: &str, token: &str) -> Result<(), RecoveryDeliveryError> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&json!({"email": email, "token": token, "expires_in_seconds": 1800}))
            .send()
            .await
            .map_err(|error| RecoveryDeliveryError::Network(error.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(RecoveryDeliveryError::Network(
                response.status().to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_notifier_fails_closed() {
        assert!(
            DisabledRecoveryNotifier
                .notify("user@example.com", "token")
                .await
                .is_err()
        );
    }

    #[test]
    fn webhook_requires_http_scheme() {
        unsafe { std::env::set_var("RECOVERY_WEBHOOK_URL", "ftp://delivery") };
        assert!(WebhookRecoveryNotifier::from_env().is_err());
        unsafe { std::env::remove_var("RECOVERY_WEBHOOK_URL") };
    }
}

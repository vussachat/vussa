#[derive(Debug)]
pub(crate) enum RepositoryError {
    Database(sqlx::Error),
    Migration(String),
    NotFound,
    Forbidden,
}

impl std::fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::Migration(error) => write!(formatter, "migration error: {error}"),
            Self::NotFound => formatter.write_str("message or channel not found"),
            Self::Forbidden => formatter.write_str("you can only edit your own messages"),
        }
    }
}

impl std::error::Error for RepositoryError {}

impl From<sqlx::Error> for RepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}
use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};

#[derive(Debug)]
pub(crate) struct AppError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
}

impl std::fmt::Display for AppError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AppError {}

impl AppError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(crate) fn internal_server_error(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    pub(crate) fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    pub(crate) fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
        }
    }
}

impl From<redis::RedisError> for AppError {
    fn from(error: redis::RedisError) -> Self {
        tracing::error!(%error, "cache request failed");
        ::metrics::counter!("vussa_cache_failures_total").increment(1);
        Self::service_unavailable("cache service unavailable")
    }
}

impl From<sqlx::Error> for AppError {
    fn from(error: sqlx::Error) -> Self {
        tracing::error!(%error, "database request failed");
        ::metrics::counter!("vussa_database_failures_total").increment(1);
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "database request failed".to_string(),
        }
    }
}

impl From<RepositoryError> for AppError {
    fn from(error: RepositoryError) -> Self {
        match error {
            RepositoryError::NotFound => Self::not_found("resource not found"),
            RepositoryError::Forbidden => Self::forbidden("operation not permitted"),
            RepositoryError::Database(error) => {
                tracing::error!(%error, "repository database request failed");
                Self::internal_server_error("database request failed")
            }
            RepositoryError::Migration(error) => {
                tracing::error!(%error, "repository migration failed");
                Self::internal_server_error("database initialization failed")
            }
        }
    }
}

pub(crate) fn map_conflict(error: RepositoryError) -> AppError {
    match error {
        RepositoryError::Database(sqlx::Error::Database(_)) => {
            AppError::bad_request("email or username already exists")
        }
        other => other.into(),
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            headers.insert("retry-after", HeaderValue::from_static("1"));
        }
        (
            self.status,
            headers,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_errors_include_retryable_status() {
        assert_eq!(
            AppError::too_many_requests("busy").status,
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn constructors_preserve_messages() {
        assert_eq!(AppError::bad_request("invalid").to_string(), "invalid");
        assert_eq!(AppError::unauthorized("login").to_string(), "login");
        assert_eq!(AppError::forbidden("denied").to_string(), "denied");
        assert_eq!(AppError::not_found("missing").status, StatusCode::NOT_FOUND);
        assert_eq!(
            AppError::internal_server_error("failed").status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn repository_errors_have_stable_http_statuses() {
        assert_eq!(
            AppError::from(RepositoryError::NotFound).status,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            AppError::from(RepositoryError::Forbidden).status,
            StatusCode::FORBIDDEN
        );
        let error = AppError::from(RepositoryError::Migration("secret detail".into()));
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!error.message.contains("secret"));
    }
}

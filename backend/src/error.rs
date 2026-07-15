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
        Self::bad_request(error.to_string())
    }
}

impl From<sqlx::Error> for AppError {
    fn from(error: sqlx::Error) -> Self {
        tracing::error!(%error, "database request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "database request failed".to_string(),
        }
    }
}

impl From<RepositoryError> for AppError {
    fn from(error: RepositoryError) -> Self {
        Self::bad_request(error.to_string())
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
    }
}

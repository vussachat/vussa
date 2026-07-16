use async_trait::async_trait;
use reqwest::Client;
use std::time::Duration;

#[derive(Debug)]
pub(crate) enum ScanError {
    Rejected,
    Unavailable(String),
}

#[async_trait]
pub(crate) trait FileScanner: Send + Sync {
    async fn scan(
        &self,
        file_name: &str,
        content_type: &str,
        contents: &[u8],
    ) -> Result<(), ScanError>;
}

pub(crate) struct NoopFileScanner;

#[async_trait]
impl FileScanner for NoopFileScanner {
    async fn scan(
        &self,
        _file_name: &str,
        _content_type: &str,
        _contents: &[u8],
    ) -> Result<(), ScanError> {
        Ok(())
    }
}

pub(crate) struct HttpFileScanner {
    client: Client,
    endpoint: String,
}

impl HttpFileScanner {
    pub(crate) fn new(endpoint: impl Into<String>) -> Result<Self, ScanError> {
        let endpoint = endpoint.into();
        let url = reqwest::Url::parse(&endpoint)
            .map_err(|_| ScanError::Unavailable("scanner URL is invalid".into()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ScanError::Unavailable(
                "scanner URL must use HTTP or HTTPS".into(),
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| ScanError::Unavailable(error.to_string()))?;
        Ok(Self { client, endpoint })
    }

    pub(crate) fn from_env() -> Result<Self, ScanError> {
        let endpoint = std::env::var("FILE_SCANNER_URL")
            .map_err(|_| ScanError::Unavailable("FILE_SCANNER_URL is not configured".into()))?;
        Self::new(endpoint)
    }
}

#[async_trait]
impl FileScanner for HttpFileScanner {
    async fn scan(
        &self,
        file_name: &str,
        content_type: &str,
        contents: &[u8],
    ) -> Result<(), ScanError> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("x-file-name", file_name)
            .header("content-type", content_type)
            .body(contents.to_vec())
            .send()
            .await
            .map_err(|error| ScanError::Unavailable(error.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else if response.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            Err(ScanError::Rejected)
        } else {
            Err(ScanError::Unavailable(response.status().to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_scanner_accepts_content() {
        NoopFileScanner
            .scan("hello.txt", "text/plain", b"hello")
            .await
            .unwrap();
    }

    #[test]
    fn scanner_endpoint_requires_http_scheme() {
        assert!(HttpFileScanner::new("ftp://scanner").is_err());
        assert!(HttpFileScanner::new("https://scanner.example.test").is_ok());
    }
}

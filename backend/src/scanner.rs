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
    pub(crate) fn from_env() -> Result<Self, ScanError> {
        let endpoint = std::env::var("FILE_SCANNER_URL")
            .map_err(|_| ScanError::Unavailable("FILE_SCANNER_URL is not configured".into()))?;
        if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
            return Err(ScanError::Unavailable(
                "FILE_SCANNER_URL must use HTTP or HTTPS".into(),
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| ScanError::Unavailable(error.to_string()))?;
        Ok(Self { client, endpoint })
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
        unsafe { std::env::set_var("FILE_SCANNER_URL", "ftp://scanner") };
        assert!(HttpFileScanner::from_env().is_err());
        unsafe { std::env::remove_var("FILE_SCANNER_URL") };
    }
}

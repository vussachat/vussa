use async_trait::async_trait;
use chrono::Utc;
use reqwest::{Client, Method, Url};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

mod scanner;

pub(crate) use scanner::{FileScanner, HttpFileScanner, NoopFileScanner, ScanError};

#[derive(Debug)]
pub(crate) enum StorageError {
    Configuration(String),
    InvalidKey,
    Io(std::io::Error),
    Http(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(error) => f.write_str(error),
            Self::InvalidKey => f.write_str("invalid storage key"),
            Self::Io(error) => write!(f, "storage I/O error: {error}"),
            Self::Http(error) => write!(f, "object storage request failed: {error}"),
        }
    }
}

impl std::error::Error for StorageError {}

#[async_trait]
pub(crate) trait BlobStore: Send + Sync {
    async fn put(&self, key: &str, contents: &[u8]) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
}

pub(crate) struct S3BlobStore {
    client: Client,
    endpoint: Url,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

impl S3BlobStore {
    pub(crate) fn from_env() -> Result<Self, StorageError> {
        let endpoint = std::env::var("STORAGE_ENDPOINT")
            .map_err(|_| StorageError::Configuration("STORAGE_ENDPOINT is required".into()))?
            .parse::<Url>()
            .map_err(|_| StorageError::Configuration("STORAGE_ENDPOINT is invalid".into()))?;
        let bucket = std::env::var("STORAGE_BUCKET")
            .map_err(|_| StorageError::Configuration("STORAGE_BUCKET is required".into()))?;
        let access_key = std::env::var("STORAGE_ACCESS_KEY")
            .map_err(|_| StorageError::Configuration("STORAGE_ACCESS_KEY is required".into()))?;
        let secret_key = std::env::var("STORAGE_SECRET_KEY")
            .map_err(|_| StorageError::Configuration("STORAGE_SECRET_KEY is required".into()))?;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|error| StorageError::Configuration(error.to_string()))?;
        Ok(Self {
            client,
            endpoint,
            bucket,
            region: std::env::var("STORAGE_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key,
            secret_key,
        })
    }

    fn request_url(&self, key: &str) -> Result<Url, StorageError> {
        if key.is_empty() || key.contains('/') || key.contains('\\') || key == "." || key == ".." {
            return Err(StorageError::InvalidKey);
        }
        let mut url = self.endpoint.clone();
        let path = format!("/{}/{}", self.bucket, key);
        url.set_path(&path);
        Ok(url)
    }

    fn signed_request(
        &self,
        method: Method,
        key: &str,
        body: Vec<u8>,
    ) -> Result<reqwest::RequestBuilder, StorageError> {
        let url = self.request_url(key)?;
        let host = url
            .host_str()
            .ok_or_else(|| StorageError::Configuration("STORAGE_ENDPOINT has no host".into()))?;
        let host = host.to_string();
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let payload_hash = hex::encode(Sha256::digest(&body));
        let canonical_uri = url.path();
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri,
            canonical_headers,
            signed_headers,
            payload_hash
        );
        let scope = format!("{date}/{}/{}/aws4_request", self.region, "s3");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(&self.secret_key, &date, &self.region);
        let signature = hex::encode(hmac_bytes(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );
        Ok(self
            .client
            .request(method, url)
            .body(body)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization))
    }
}

fn hmac_bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
    const BLOCK_SIZE: usize = 64;
    let mut normalized = [0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        normalized[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(BLOCK_SIZE + value.len());
    let mut outer = Vec::with_capacity(BLOCK_SIZE + 32);
    for byte in normalized {
        inner.push(byte ^ 0x36);
        outer.push(byte ^ 0x5c);
    }
    inner.extend_from_slice(value);
    outer.extend_from_slice(&Sha256::digest(&inner));
    Sha256::digest(outer).to_vec()
}

fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let date_key = hmac_bytes(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac_bytes(&date_key, region.as_bytes());
    let service_key = hmac_bytes(&region_key, b"s3");
    hmac_bytes(&service_key, b"aws4_request")
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, key: &str, contents: &[u8]) -> Result<(), StorageError> {
        let response = self
            .signed_request(Method::PUT, key, contents.to_vec())?
            .send()
            .await
            .map_err(|e| StorageError::Http(e.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(StorageError::Http(response.status().to_string()))
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let response = self
            .signed_request(Method::GET, key, Vec::new())?
            .send()
            .await
            .map_err(|e| StorageError::Http(e.to_string()))?;
        if !response.status().is_success() {
            return Err(StorageError::Http(response.status().to_string()));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| StorageError::Http(e.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let response = self
            .signed_request(Method::DELETE, key, Vec::new())?
            .send()
            .await
            .map_err(|e| StorageError::Http(e.to_string()))?;
        if response.status().is_success() || response.status().as_u16() == 404 {
            Ok(())
        } else {
            Err(StorageError::Http(response.status().to_string()))
        }
    }
}

/// Development and single-node implementation. Production deployments can
/// replace this seam with an S3-compatible implementation without changing
/// HTTP handlers or database metadata semantics.
pub(crate) struct FilesystemBlobStore {
    root: PathBuf,
}

impl FilesystemBlobStore {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() || key.contains('/') || key.contains('\\') || key == "." || key == ".." {
            return Err(StorageError::InvalidKey);
        }
        Ok(self.root.join(key))
    }
}

#[async_trait]
impl BlobStore for FilesystemBlobStore {
    async fn put(&self, key: &str, contents: &[u8]) -> Result<(), StorageError> {
        let path = self.path(key)?;
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(StorageError::Io)?;
        tokio::fs::write(path, contents)
            .await
            .map_err(StorageError::Io)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        tokio::fs::read(self.path(key)?)
            .await
            .map_err(StorageError::Io)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        match tokio::fs::remove_file(self.path(key)?).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::Io(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingBlobStore;

    #[async_trait]
    impl BlobStore for FailingBlobStore {
        async fn put(&self, _key: &str, _contents: &[u8]) -> Result<(), StorageError> {
            Err(StorageError::Http("injected storage outage".into()))
        }

        async fn get(&self, _key: &str) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::Http("injected storage outage".into()))
        }

        async fn delete(&self, _key: &str) -> Result<(), StorageError> {
            Err(StorageError::Http("injected storage outage".into()))
        }
    }

    #[test]
    fn storage_keys_cannot_escape_root() {
        let store = FilesystemBlobStore::new("/tmp/uploads");
        assert!(store.path("../secret").is_err());
        assert!(store.path("nested/file").is_err());
        assert!(store.path("safe.bin").is_ok());
    }

    #[test]
    fn hmac_implementation_matches_sha256_reference() {
        assert_eq!(
            hex::encode(hmac_bytes(
                b"key",
                b"The quick brown fox jumps over the lazy dog"
            )),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[tokio::test]
    async fn storage_failure_is_injectable_through_blob_store() {
        let store = FailingBlobStore;
        assert!(store.put("file.bin", b"data").await.is_err());
        assert!(store.get("file.bin").await.is_err());
        assert!(store.delete("file.bin").await.is_err());
    }
}

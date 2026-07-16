use async_trait::async_trait;
use axum::{
    Json,
    body::Body,
    extract::{
        Multipart, Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use futures_util::StreamExt;
use rand::RngCore;
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{
    collections::HashMap,
    env,
    net::IpAddr,
    sync::Arc,
    sync::atomic::Ordering,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Semaphore, mpsc, oneshot, watch};
use tracing::{error, info};
use uuid::Uuid;

mod api;
mod auth;
mod bootstrap;
mod cache;
mod clock;
mod config;
mod database;
mod error;
mod metrics;
mod models;
mod notifications;
mod outbox;
mod persistence;
mod repository;
mod routes;
mod services;
mod state;
mod storage;
mod websocket;

use api::*;
use auth::*;
use cache::{CacheHealth, RedisCacheHealth, ValkeyPool};
use clock::{Clock, SystemClock};
use config::*;
use database::{DatabaseHealth, PostgresDatabaseHealth};
use error::{AppError, RepositoryError, map_conflict};
use metrics::*;
use models::*;
use notifications::{
    DisabledNotificationSink, NotificationSink, WebhookNotificationSink, run_notification_delivery,
};
use persistence::{AuditEvent, queue_auth_invalidation, record_audit, record_audit_pool};
use repository::{ChatRepository, PostgresRepository};
use services::*;
use state::{AppState, VerificationKey, VerificationOutcome};
use storage::{
    BlobStore, FileScanner, FilesystemBlobStore, HttpFileScanner, NoopFileScanner, ScanError,
};
use websocket::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    bootstrap::run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_urls_reject_non_http_and_private_literals() {
        assert!(validate_preview_url("file:///etc/passwd").is_err());
        assert!(validate_preview_url("http://127.0.0.1:8080").is_ok());
        assert!(!is_public_ip("127.0.0.1".parse().unwrap()));
        assert!(is_public_ip("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn preview_metadata_extractors_are_bounded() {
        let html = r#"<title>Hello &amp; friends</title><meta name="description" content="A page"><meta property="og:image" content="/image.png">"#;
        let base = reqwest::Url::parse("https://example.test/page").unwrap();
        let metadata = preview_metadata(html, &base);
        assert_eq!(metadata.title.as_deref(), Some("Hello & friends"));
        assert_eq!(metadata.description.as_deref(), Some("A page"));
        assert_eq!(
            metadata.image_url.as_deref(),
            Some("https://example.test/image.png")
        );
        let unsafe_image = preview_metadata(
            r#"<meta property="og:image" content="javascript:alert(1)">"#,
            &base,
        );
        assert!(unsafe_image.image_url.is_none());
    }

    #[test]
    fn history_keys_are_namespaced() {
        assert_eq!(history_key("main"), "chat:history:main:messages");
        assert_eq!(history_order_key("main"), "chat:history:main:order");
    }

    #[test]
    fn emoji_validation_rejects_empty_and_control_input() {
        assert_eq!(normalize_emoji("  👍  ").unwrap(), "👍");
        assert!(normalize_emoji("").is_err());
        assert!(normalize_emoji("bad\nemoji").is_err());
    }

    #[test]
    fn upload_names_cannot_escape_storage_directory() {
        assert_eq!(sanitize_upload_name("../../secret.txt"), "secret.txt");
        assert_eq!(sanitize_upload_name("photo\n.png"), "photo.png");
    }
}

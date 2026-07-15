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
use tokio::sync::{Mutex as TokioMutex, Semaphore, broadcast, mpsc, oneshot, watch};
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
mod repository;
mod routes;
mod state;
mod storage;
mod websocket;

use api::*;
use auth::*;
use cache::{CacheHealth, RedisCacheHealth, VALKEY_COMMAND_INDEX, VALKEY_COMMANDS};
use clock::{Clock, SystemClock};
use config::*;
use database::{DatabaseHealth, PostgresDatabaseHealth};
use error::{AppError, RepositoryError, map_conflict};
use metrics::*;
use models::*;
use notifications::{
    DisabledNotificationSink, NotificationSink, WebhookNotificationSink, run_notification_delivery,
};
use repository::{ChatRepository, PostgresRepository};
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
        let html = r#"<title>Hello</title><meta name="description" content="A page">"#;
        assert_eq!(html_tag_value(html, "title").as_deref(), Some("Hello"));
        assert_eq!(
            html_meta_value(html, "description").as_deref(),
            Some("A page")
        );
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

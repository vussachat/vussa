use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use async_trait::async_trait;
use axum::{
    Json,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use futures_util::StreamExt;
use rand::RngCore;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{
    collections::HashMap,
    env,
    sync::Arc,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, OnceCell, Semaphore, broadcast, mpsc, oneshot, watch};
use tracing::{error, info};
use uuid::Uuid;

mod error;
mod routes;

use error::AppError;

const MAIN_CHANNEL: &str = "main";
const HISTORY_PAGE_SIZE: usize = 50;
const HOT_HISTORY_LIMIT: usize = 300;
const SESSION_TTL_SECONDS: u64 = 60 * 60 * 24 * 14;
const CSRF_HEADER: &str = "x-csrf-token";
const PRESENCE_TTL_SECONDS: u64 = 45;
static AUTHENTICATIONS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_WEBSOCKETS: AtomicU64 = AtomicU64::new(0);
static VALKEY_COMMANDS: OnceCell<Vec<redis::aio::MultiplexedConnection>> = OnceCell::const_new();
static VALKEY_COMMAND_INDEX: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthUser {
    id: Uuid,
    email: String,
    username: String,
    roles: Vec<String>,
    permissions: Vec<String>,
}

#[derive(Debug, Clone)]
struct Session {
    id: Uuid,
    csrf: String,
    user: AuthUser,
}

#[derive(Debug, Serialize)]
struct AuditEvent {
    id: Uuid,
    actor_user_id: Option<Uuid>,
    action: String,
    target_type: String,
    target_id: Option<Uuid>,
    created_at: i64,
}

#[derive(Clone)]
struct AppState {
    valkey: redis::Client,
    database: PgPool,
    repository: Arc<dyn ChatRepository>,
    rooms: Arc<RoomManager>,
    password_verifiers: Arc<Semaphore>,
    password_verification_flights: Arc<TokioMutex<HashMap<VerificationKey, VerificationSender>>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct VerificationKey([u8; 32]);

#[derive(Clone, Copy, Debug)]
enum VerificationOutcome {
    Verified(bool),
    Overloaded,
    Unavailable,
}

type VerificationSender = watch::Sender<Option<VerificationOutcome>>;

struct RoomManager {
    commands: mpsc::Sender<ManagerCommand>,
    control: broadcast::Sender<Message>,
}

enum ManagerCommand {
    Subscribe {
        channel: String,
        reply: oneshot::Sender<Result<broadcast::Receiver<Message>, String>>,
    },
    Release {
        channel: String,
    },
}

struct RoomEntry {
    sender: broadcast::Sender<Message>,
    clients: usize,
}

#[derive(Debug, Serialize)]
struct Channel {
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ConversationSummary {
    id: Uuid,
    name: String,
    kind: String,
    owner_user_id: Option<Uuid>,
    display_name: String,
    peer_user_id: Option<Uuid>,
    peer_username: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChannelMember {
    user_id: Uuid,
    username: String,
    membership_role: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientEvent {
    ListChannels,
    CreateChannel {
        name: String,
    },
    CreatePrivateChannel {
        name: String,
    },
    OpenDirect {
        user_id: Uuid,
    },
    InviteMember {
        channel: String,
        user_id: Uuid,
    },
    RemoveMember {
        channel: String,
        user_id: Uuid,
    },
    JoinChannel {
        name: String,
    },
    DeleteChannel {
        name: String,
    },
    DeleteMessage {
        id: Uuid,
    },
    SendMessage {
        text: String,
    },
    EditMessage {
        id: Uuid,
        text: String,
    },
    LoadHistory {
        channel: String,
        before_created_at: u64,
        before_id: Uuid,
    },
    Heartbeat,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    id: Uuid,
    channel: String,
    username: String,
    text: String,
    created_at: u64,
    edited: bool,
    deleted: bool,
}

#[derive(Debug, bitcode::Encode, bitcode::Decode, Clone)]
struct StoredMessage {
    id: Uuid,
    channel: String,
    username: String,
    text: String,
    created_at: u64,
    edited: bool,
    deleted: bool,
    owner_session: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Participant {
    user_id: Uuid,
    username: String,
    roles: Vec<String>,
    online: bool,
}

#[derive(Debug)]
enum RepositoryError {
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

#[async_trait]
trait ChatRepository: Send + Sync {
    async fn ensure_main_channel(&self) -> Result<(), RepositoryError>;
    async fn list_channels(&self) -> Result<Vec<String>, RepositoryError>;
    async fn create_channel(&self, name: &str) -> Result<bool, RepositoryError>;
    async fn delete_channel(&self, name: &str) -> Result<bool, RepositoryError>;
    async fn save_message(
        &self,
        message: &ChatMessage,
        owner_session: Uuid,
    ) -> Result<(), RepositoryError>;
    async fn load_messages(
        &self,
        channel: &str,
        before: Option<(i64, Uuid)>,
        limit: i64,
    ) -> Result<Vec<ChatMessage>, RepositoryError>;
    async fn edit_message(
        &self,
        channel: &str,
        id: Uuid,
        owner_session: Uuid,
        text: &str,
    ) -> Result<ChatMessage, RepositoryError>;
    async fn delete_message(
        &self,
        channel: &str,
        id: Uuid,
        owner_session: Uuid,
    ) -> Result<ChatMessage, RepositoryError>;
    async fn prune_expired(&self) -> Result<u64, RepositoryError>;
    async fn register_user(
        &self,
        email: &str,
        username: &str,
        password_hash: &str,
    ) -> Result<AuthUser, RepositoryError>;
    async fn find_user_for_login(
        &self,
        email: &str,
    ) -> Result<Option<(AuthUser, String, bool)>, RepositoryError>;
    async fn list_users(
        &self,
        limit: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<AuthUser>, RepositoryError>;
    async fn set_user_disabled(
        &self,
        actor: Uuid,
        user: Uuid,
        disabled: bool,
    ) -> Result<(), RepositoryError>;
    async fn update_username(&self, user: Uuid, username: &str) -> Result<(), RepositoryError>;
    async fn assign_role(&self, actor: Uuid, user: Uuid, role: &str)
    -> Result<(), RepositoryError>;
    async fn list_audit(&self, limit: i64) -> Result<Vec<AuditEvent>, RepositoryError>;
}

struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    async fn connect(database_url: &str) -> Result<Arc<Self>, RepositoryError> {
        let max_connections = std::env::var("PG_MAX_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(16);
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(database_url)
            .await?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|error| RepositoryError::Migration(error.to_string()))?;
        Ok(Arc::new(Self { pool }))
    }

    async fn seed_authorization(&self) -> Result<(), RepositoryError> {
        let permissions = [
            "chat:write",
            "chat:moderate",
            "users:read",
            "users:write",
            "roles:write",
            "audit:read",
            "channels:read",
            "channels:write",
            "moderation:read",
            "moderation:write",
            "operations:read",
        ];
        for permission in permissions {
            sqlx::query(
                "INSERT INTO permissions (id, name) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
            )
            .bind(Uuid::now_v7())
            .bind(permission)
            .execute(&self.pool)
            .await?;
        }
        for role in ["user", "moderator", "admin"] {
            sqlx::query("INSERT INTO roles (id, name, created_at) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING")
                .bind(Uuid::now_v7()).bind(role).bind(now_millis() as i64).execute(&self.pool).await?;
        }
        sqlx::query("INSERT INTO role_permissions (role_id, permission_id) SELECT r.id, p.id FROM roles r CROSS JOIN permissions p WHERE r.name = 'user' AND p.name = 'chat:write' ON CONFLICT DO NOTHING").execute(&self.pool).await?;
        sqlx::query("INSERT INTO role_permissions (role_id, permission_id) SELECT r.id, p.id FROM roles r CROSS JOIN permissions p WHERE r.name = 'moderator' AND p.name IN ('chat:write','chat:moderate') ON CONFLICT DO NOTHING").execute(&self.pool).await?;
        sqlx::query("INSERT INTO role_permissions (role_id, permission_id) SELECT r.id, p.id FROM roles r CROSS JOIN permissions p WHERE r.name = 'admin' ON CONFLICT DO NOTHING").execute(&self.pool).await?;
        sqlx::query("INSERT INTO role_permissions (role_id, permission_id) SELECT r.id, p.id FROM roles r CROSS JOIN permissions p WHERE r.name = 'moderator' AND p.name IN ('moderation:read','moderation:write','channels:read') ON CONFLICT DO NOTHING").execute(&self.pool).await?;
        Ok(())
    }

    async fn seed_test_accounts(&self) -> Result<(), RepositoryError> {
        for number in 1..=6 {
            let username = format!("test{number}");
            let email = format!("{username}@example.com");
            let password = username.clone();
            let hash = hash_password_unchecked(&password)
                .map_err(|error| RepositoryError::Migration(error.to_string()))?;
            let now = now_millis() as i64;
            let row = sqlx::query("INSERT INTO users (id,email,username,password_hash,created_at,updated_at) VALUES ($1,lower($2),$3,$4,$5,$5) ON CONFLICT (lower(email)) DO UPDATE SET username=EXCLUDED.username,password_hash=EXCLUDED.password_hash,disabled_at=NULL,updated_at=EXCLUDED.updated_at RETURNING id")
                .bind(Uuid::now_v7()).bind(&email).bind(&username).bind(hash).bind(now)
                .fetch_one(&self.pool).await?;
            let role = if number == 1 { "admin" } else { "user" };
            sqlx::query("INSERT INTO user_roles (user_id,role_id,assigned_at) SELECT $1,id,$2 FROM roles WHERE name=$3 ON CONFLICT DO NOTHING")
                .bind(row.get::<Uuid, _>("id")).bind(now).bind(role).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn user_from_row(&self, row: sqlx::postgres::PgRow) -> AuthUser {
        AuthUser {
            id: row.get("id"),
            email: row.get("email"),
            username: row.get("username"),
            roles: row.try_get("roles").unwrap_or_default(),
            permissions: row.try_get("permissions").unwrap_or_default(),
        }
    }
}

#[async_trait]
impl ChatRepository for PostgresRepository {
    async fn ensure_main_channel(&self) -> Result<(), RepositoryError> {
        self.create_channel(MAIN_CHANNEL).await?;
        Ok(())
    }

    async fn list_channels(&self) -> Result<Vec<String>, RepositoryError> {
        let rows = sqlx::query("SELECT name FROM channels WHERE deleted_at IS NULL AND kind = 'public' ORDER BY (name = 'main') DESC, name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|row| row.get("name")).collect())
    }

    async fn create_channel(&self, name: &str) -> Result<bool, RepositoryError> {
        let result = sqlx::query("INSERT INTO channels (id, name, created_at) VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET deleted_at = NULL WHERE channels.deleted_at IS NOT NULL")
            .bind(Uuid::now_v7())
            .bind(name)
            .bind(now_millis() as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_channel(&self, name: &str) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "UPDATE channels SET deleted_at = $1 WHERE name = $2 AND deleted_at IS NULL",
        )
        .bind(now_millis() as i64)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn save_message(
        &self,
        message: &ChatMessage,
        owner_session: Uuid,
    ) -> Result<(), RepositoryError> {
        let result = sqlx::query("INSERT INTO messages (id, channel_id, username, text, created_at, edited, owner_session) SELECT $1, id, $2, $3, $4, $5, $6 FROM channels WHERE name = $7 AND deleted_at IS NULL")
            .bind(message.id)
            .bind(&message.username)
            .bind(&message.text)
            .bind(message.created_at as i64)
            .bind(message.edited)
            .bind(owner_session)
            .bind(&message.channel)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        Ok(())
    }

    async fn load_messages(
        &self,
        channel: &str,
        before: Option<(i64, Uuid)>,
        limit: i64,
    ) -> Result<Vec<ChatMessage>, RepositoryError> {
        let rows = sqlx::query("SELECT m.id, c.name AS channel, m.username, CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text, m.created_at, m.edited, m.deleted_at IS NOT NULL AS deleted FROM messages m JOIN channels c ON c.id = m.channel_id WHERE c.name = $1 AND c.deleted_at IS NULL AND m.created_at >= $2 AND ($3::bigint IS NULL OR (m.created_at, m.id) < ($3, $4)) ORDER BY m.created_at DESC, m.id DESC LIMIT $5")
            .bind(channel)
            .bind(now_millis() as i64 - 90 * 24 * 60 * 60 * 1000)
            .bind(before.map(|value| value.0))
            .bind(before.map(|value| value.1))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        let mut messages = rows
            .into_iter()
            .map(|row| ChatMessage {
                id: row.get("id"),
                channel: row.get("channel"),
                username: row.get("username"),
                text: row.get("text"),
                created_at: row.get::<i64, _>("created_at") as u64,
                edited: row.get("edited"),
                deleted: row.get("deleted"),
            })
            .collect::<Vec<_>>();
        messages.reverse();
        Ok(messages)
    }

    async fn edit_message(
        &self,
        channel: &str,
        id: Uuid,
        owner_session: Uuid,
        text: &str,
    ) -> Result<ChatMessage, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT m.channel_id, c.name AS channel, m.username, m.created_at, m.edited, m.deleted_at, m.owner_session FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.id = $1 AND c.name = $2 AND c.deleted_at IS NULL FOR UPDATE")
            .bind(id)
            .bind(channel)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::NotFound)?;
        if row.get::<Uuid, _>("owner_session") != owner_session {
            return Err(RepositoryError::Forbidden);
        }
        sqlx::query("UPDATE messages SET text = $1, edited = TRUE WHERE id = $2")
            .bind(text)
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(ChatMessage {
            id,
            channel: row.get("channel"),
            username: row.get("username"),
            text: text.to_string(),
            created_at: row.get::<i64, _>("created_at") as u64,
            edited: true,
            deleted: false,
        })
    }

    async fn delete_message(
        &self,
        channel: &str,
        id: Uuid,
        owner_session: Uuid,
    ) -> Result<ChatMessage, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT c.name AS channel, m.username, m.created_at, m.owner_session, m.deleted_at FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.id=$1 AND c.name=$2 AND c.deleted_at IS NULL FOR UPDATE")
            .bind(id).bind(channel).fetch_optional(&mut *transaction).await?
            .ok_or(RepositoryError::NotFound)?;
        if row.get::<Uuid, _>("owner_session") != owner_session {
            return Err(RepositoryError::Forbidden);
        }
        sqlx::query("UPDATE messages SET deleted_at=COALESCE(deleted_at,$1), edited=FALSE WHERE id=$2")
            .bind(now_millis() as i64).bind(id).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(ChatMessage {
            id,
            channel: row.get("channel"),
            username: row.get("username"),
            text: String::new(),
            created_at: row.get::<i64, _>("created_at") as u64,
            edited: false,
            deleted: true,
        })
    }

    async fn prune_expired(&self) -> Result<u64, RepositoryError> {
        let result = sqlx::query("DELETE FROM messages WHERE created_at < $1")
            .bind(now_millis() as i64 - 90 * 24 * 60 * 60 * 1000)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn register_user(
        &self,
        email: &str,
        username: &str,
        password_hash: &str,
    ) -> Result<AuthUser, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let id = Uuid::now_v7();
        let now = now_millis() as i64;
        sqlx::query("INSERT INTO users (id,email,username,password_hash,created_at,updated_at) VALUES ($1,lower($2),$3,$4,$5,$5)")
            .bind(id).bind(email).bind(username).bind(password_hash).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO user_roles (user_id, role_id, assigned_at) SELECT $1, id, $2 FROM roles WHERE name='user'")
            .bind(id).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO audit_events (id, action, target_type, target_id, created_at) VALUES ($1,'user.registered','user',$2,$3)")
            .bind(Uuid::now_v7()).bind(id).bind(now).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT u.id,u.email,u.username, ARRAY(SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=u.id) AS roles, ARRAY(SELECT DISTINCT p.name FROM permissions p JOIN role_permissions rp ON rp.permission_id=p.id JOIN user_roles ur ON ur.role_id=rp.role_id WHERE ur.user_id=u.id) AS permissions FROM users u WHERE u.id=$1")
            .bind(id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(self.user_from_row(row).await)
    }

    async fn find_user_for_login(
        &self,
        email: &str,
    ) -> Result<Option<(AuthUser, String, bool)>, RepositoryError> {
        let row = sqlx::query("SELECT u.id,u.email,u.username,u.password_hash,u.disabled_at IS NOT NULL AS disabled, ARRAY(SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=u.id) AS roles, ARRAY(SELECT DISTINCT p.name FROM permissions p JOIN role_permissions rp ON rp.permission_id=p.id JOIN user_roles ur ON ur.role_id=rp.role_id WHERE ur.user_id=u.id) AS permissions FROM users u WHERE lower(u.email)=lower($1) OR lower(u.username)=lower($1)")
            .bind(email).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| {
            (
                AuthUser {
                    id: row.get("id"),
                    email: row.get("email"),
                    username: row.get("username"),
                    roles: row.try_get("roles").unwrap_or_default(),
                    permissions: row.try_get("permissions").unwrap_or_default(),
                },
                row.get("password_hash"),
                row.get("disabled"),
            )
        }))
    }

    async fn list_users(
        &self,
        limit: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<AuthUser>, RepositoryError> {
        let rows = if let Some(after) = after {
            sqlx::query("SELECT u.id,u.email,u.username,ARRAY(SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=u.id) AS roles,ARRAY[]::text[] AS permissions FROM users u WHERE u.id>$1 ORDER BY u.id LIMIT $2").bind(after).bind(limit).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT u.id,u.email,u.username,ARRAY(SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=u.id) AS roles,ARRAY[]::text[] AS permissions FROM users u ORDER BY u.id LIMIT $1").bind(limit).fetch_all(&self.pool).await?
        };
        Ok(rows
            .into_iter()
            .map(|row| AuthUser {
                id: row.get("id"),
                email: row.get("email"),
                username: row.get("username"),
                roles: row.try_get("roles").unwrap_or_default(),
                permissions: Vec::new(),
            })
            .collect())
    }

    async fn set_user_disabled(
        &self,
        actor: Uuid,
        user: Uuid,
        disabled: bool,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("UPDATE users SET disabled_at=CASE WHEN $1 THEN COALESCE(disabled_at,$2) ELSE NULL END, role_version=role_version+1, updated_at=$2 WHERE id=$3")
            .bind(disabled).bind(now_millis() as i64).bind(user).execute(&mut *tx).await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,$3,'user',$4,$5)")
            .bind(Uuid::now_v7()).bind(actor).bind(if disabled { "user.disabled" } else { "user.enabled" }).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)")
            .bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn update_username(&self, user: Uuid, username: &str) -> Result<(), RepositoryError> {
        let result = sqlx::query(
            "UPDATE users SET username=$1,updated_at=$2,role_version=role_version+1 WHERE id=$3",
        )
        .bind(username)
        .bind(now_millis() as i64)
        .bind(user)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        Ok(())
    }

    async fn assign_role(
        &self,
        actor: Uuid,
        user: Uuid,
        role: &str,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("INSERT INTO user_roles (user_id,role_id,assigned_at) SELECT $1,id,$3 FROM roles WHERE name=$2 ON CONFLICT DO NOTHING")
            .bind(user).bind(role).bind(now_millis() as i64).execute(&mut *tx).await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        sqlx::query("UPDATE users SET role_version=role_version+1,updated_at=$1 WHERE id=$2")
            .bind(now_millis() as i64)
            .bind(user)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'user.role_assigned','user',$3,jsonb_build_object('role',$4),$5)")
            .bind(Uuid::now_v7()).bind(actor).bind(user).bind(role).bind(now_millis() as i64).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)")
            .bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_audit(&self, limit: i64) -> Result<Vec<AuditEvent>, RepositoryError> {
        let rows = sqlx::query("SELECT id,actor_user_id,action,target_type,target_id,created_at FROM audit_events ORDER BY created_at DESC,id DESC LIMIT $1")
            .bind(limit).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| AuditEvent {
                id: row.get("id"),
                actor_user_id: row.get("actor_user_id"),
                action: row.get("action"),
                target_type: row.get("target_type"),
                target_id: row.get("target_id"),
                created_at: row.get("created_at"),
            })
            .collect())
    }
}

impl StoredMessage {
    fn from_message(message: ChatMessage, owner_session: Uuid) -> Self {
        Self {
            id: message.id,
            channel: message.channel,
            username: message.username,
            text: message.text,
            created_at: message.created_at,
            edited: message.edited,
            deleted: message.deleted,
            owner_session,
        }
    }

    fn into_message(self) -> ChatMessage {
        ChatMessage {
            id: self.id,
            channel: self.channel,
            username: self.username,
            text: self.text,
            created_at: self.created_at,
            edited: self.edited,
            deleted: self.deleted,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerEvent {
    Welcome {
        username: String,
    },
    Channels {
        channels: Vec<String>,
    },
    PrivateConversations {
        conversations: Vec<ConversationSummary>,
    },
    Members {
        channel: String,
        members: Vec<ChannelMember>,
    },
    ChannelCreated {
        name: String,
    },
    ChannelDeleted {
        name: String,
    },
    Joined {
        name: String,
    },
    History {
        channel: String,
        messages: Vec<ChatMessage>,
        source: HistorySource,
        has_more: bool,
    },
    HistoryPage {
        channel: String,
        messages: Vec<ChatMessage>,
        source: HistorySource,
        has_more: bool,
    },
    Message {
        message: ChatMessage,
    },
    MessageUpdated {
        message: ChatMessage,
    },
    Error {
        message: String,
    },
    Participants {
        channel: String,
        participants: Vec<Participant>,
    },
    ParticipantJoined {
        channel: String,
        participant: Participant,
    },
    ParticipantLeft {
        channel: String,
        user_id: Uuid,
    },
    PresenceSync {
        channel: String,
        participants: Vec<Participant>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
enum HistorySource {
    Cache,
    Database,
}

impl RoomManager {
    async fn start(client: &redis::Client) -> redis::RedisResult<Arc<Self>> {
        let room_event_capacity = env::var("WS_ROOM_EVENT_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4096)
            .clamp(128, 65_536);
        let pubsub = client.get_async_pubsub().await?;
        let (mut sink, stream) = pubsub.split();
        sink.subscribe("_control").await?;

        let (commands, command_rx) = mpsc::channel(128);
        let (control, _) = broadcast::channel(128);
        tokio::spawn(run_room_manager(
            sink,
            stream,
            command_rx,
            control.clone(),
            room_event_capacity,
        ));
        info!(room_event_capacity, "WebSocket room buffer configured");

        Ok(Arc::new(Self { commands, control }))
    }

    fn subscribe_control(&self) -> broadcast::Receiver<Message> {
        self.control.subscribe()
    }

    async fn subscribe(&self, channel: &str) -> Result<broadcast::Receiver<Message>, AppError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ManagerCommand::Subscribe {
                channel: channel.to_string(),
                reply,
            })
            .await
            .map_err(|_| AppError::bad_request("room manager is unavailable"))?;
        response
            .await
            .map_err(|_| AppError::bad_request("room manager is unavailable"))?
            .map_err(AppError::bad_request)
    }

    async fn release(&self, channel: &str) {
        let _ = self
            .commands
            .send(ManagerCommand::Release {
                channel: channel.to_string(),
            })
            .await;
    }
}

async fn run_room_manager(
    mut sink: redis::aio::PubSubSink,
    mut stream: redis::aio::PubSubStream,
    mut commands: mpsc::Receiver<ManagerCommand>,
    control: broadcast::Sender<Message>,
    room_event_capacity: usize,
) {
    let mut rooms: HashMap<String, RoomEntry> = HashMap::new();

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(ManagerCommand::Subscribe { channel, reply }) => {
                        let entry = if let Some(entry) = rooms.get_mut(&channel) {
                            entry.clients += 1;
                            entry.sender.clone()
                        } else {
                            let sender = broadcast::channel(room_event_capacity).0;
                            if sink.subscribe(room_key(&channel)).await.is_err() {
                                let _ = reply.send(Err("Valkey room subscription failed".to_string()));
                                continue;
                            }
                            rooms.insert(channel.clone(), RoomEntry { sender: sender.clone(), clients: 1 });
                            sender
                        };
                        let _ = reply.send(Ok(entry.subscribe()));
                    }
                    Some(ManagerCommand::Release { channel }) => {
                        let should_unsubscribe = if let Some(entry) = rooms.get_mut(&channel) {
                            entry.clients = entry.clients.saturating_sub(1);
                            entry.clients == 0
                        } else {
                            false
                        };
                        if should_unsubscribe {
                            rooms.remove(&channel);
                            let _ = sink.unsubscribe(room_key(&channel)).await;
                        }
                    }
                    None => break,
                }
            }
            pubsub_message = stream.next() => {
                match pubsub_message {
                    Some(pubsub_message) => {
                        let payload = pubsub_message.get_payload_bytes();
                        if let Ok(event) = serde_json::from_slice::<ServerEvent>(payload) {
                            // Redis already carries the canonical serialized
                            // event. Reuse one bytes-backed WebSocket frame for
                            // every local subscriber instead of serializing and
                            // allocating the same JSON once per connection.
                            let wire = Message::Text(
                                String::from_utf8_lossy(payload).into_owned().into()
                            );
                            match &event {
                                ServerEvent::Message { message } | ServerEvent::MessageUpdated { message } => {
                                    if let Some(entry) = rooms.get(&message.channel) {
                                        let _ = entry.sender.send(wire);
                                    }
                                }
                                ServerEvent::ChannelCreated { .. } | ServerEvent::ChannelDeleted { .. } => {
                                    let _ = control.send(wire);
                                }
                                ServerEvent::Participants { channel, .. }
                                | ServerEvent::ParticipantJoined { channel, .. }
                                | ServerEvent::ParticipantLeft { channel, .. }
                                | ServerEvent::PresenceSync { channel, .. } => {
                                    if let Some(entry) = rooms.get(channel) {
                                        let _ = entry.sender.send(wire);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateChannelRequest {
    name: String,
}

#[derive(Debug, Serialize)]
struct CreateChannelResponse {
    name: String,
}

#[derive(Debug, Deserialize)]
struct DirectConversationRequest {
    user_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct MemberRequest {
    user_id: Uuid,
}

#[derive(Debug, Serialize)]
struct UserSearchResult {
    id: Uuid,
    username: String,
}

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    email: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AccountUpdateRequest {
    username: String,
}

#[derive(Debug, Deserialize, Default)]
struct AdminListQuery {
    q: Option<String>,
    after: Option<Uuid>,
    limit: Option<i64>,
    channel: Option<String>,
    user: Option<Uuid>,
    from: Option<i64>,
    to: Option<i64>,
    deleted: Option<bool>,
    actor: Option<Uuid>,
    action: Option<String>,
    target: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct AdminUserView {
    #[serde(flatten)]
    user: AuthUser,
    disabled_at: Option<i64>,
    deleted_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Deserialize)]
struct PasswordResetRequest {
    password: String,
}

#[derive(Debug, Deserialize)]
struct ChannelAdminRequest {
    name: Option<String>,
    description: Option<String>,
    retention_days: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct ModerationRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BulkModerationRequest {
    ids: Vec<Uuid>,
    action: String,
    reason: Option<String>,
}

fn password_hash(password: &str) -> Result<String, AppError> {
    if password.len() < 12 || password.len() > 200 {
        return Err(AppError::bad_request("password must be 12–200 characters"));
    }
    hash_password_unchecked(password)
}

fn hash_password_unchecked(password: &str) -> Result<String, AppError> {
    let salt = argon2::password_hash::SaltString::encode_b64(Uuid::now_v7().as_bytes())
        .map_err(|_| AppError::bad_request("could not create password salt"))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AppError::bad_request("could not hash password"))
}

fn verify_password(password: &str, encoded: &str) -> bool {
    PasswordHash::new(encoded)
        .ok()
        .map(|hash| {
            Argon2::default()
                .verify_password(password.as_bytes(), &hash)
                .is_ok()
        })
        .unwrap_or(false)
}

fn verification_key(user_id: Uuid, password: &str, encoded: &str) -> VerificationKey {
    let mut digest = Sha256::new();
    digest.update(user_id.as_bytes());
    digest.update(encoded.as_bytes());
    digest.update(password.as_bytes());
    VerificationKey(digest.finalize().into())
}

async fn verify_password_coalesced(
    state: &Arc<AppState>,
    user_id: Uuid,
    password: String,
    encoded: String,
) -> Result<bool, AppError> {
    let key = verification_key(user_id, &password, &encoded);
    let (mut receiver, new_flight) = {
        let mut flights = state.password_verification_flights.lock().await;
        if let Some(sender) = flights.get(&key) {
            (sender.subscribe(), None)
        } else {
            let (sender, receiver) = watch::channel(None);
            flights.insert(key, sender.clone());
            (receiver, Some(sender))
        }
    };

    if let Some(sender) = new_flight {
        let verifiers = state.password_verifiers.clone();
        let flights = state.password_verification_flights.clone();
        tokio::spawn(async move {
            let outcome =
                match tokio::time::timeout(Duration::from_secs(5), verifiers.acquire_owned()).await
                {
                    Ok(Ok(permit)) => match tokio::task::spawn_blocking(move || {
                        let verified = verify_password(&password, &encoded);
                        drop(permit);
                        verified
                    })
                    .await
                    {
                        Ok(verified) => VerificationOutcome::Verified(verified),
                        Err(_) => VerificationOutcome::Unavailable,
                    },
                    _ => VerificationOutcome::Overloaded,
                };
            let _ = sender.send(Some(outcome));
            flights.lock().await.remove(&key);
        });
    }

    loop {
        if let Some(outcome) = *receiver.borrow() {
            return match outcome {
                VerificationOutcome::Verified(verified) => Ok(verified),
                VerificationOutcome::Overloaded => Err(AppError::too_many_requests(
                    "authentication overloaded, retry shortly",
                )),
                VerificationOutcome::Unavailable => Err(AppError::service_unavailable(
                    "authentication service unavailable",
                )),
            };
        }
        receiver
            .changed()
            .await
            .map_err(|_| AppError::service_unavailable("authentication service unavailable"))?;
    }
}

fn password_verifier_limit() -> usize {
    env::var("AUTH_VERIFY_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(4)
                .saturating_mul(2)
                .clamp(2, 16)
        })
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get("cookie")?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == name).then(|| value.to_string())
        })
}

fn session_key(id: Uuid) -> String {
    format!("vussa:session:{id}")
}

fn valkey_commands() -> Result<redis::aio::MultiplexedConnection, AppError> {
    let connections = VALKEY_COMMANDS
        .get()
        .ok_or_else(|| AppError::service_unavailable("Valkey connection unavailable"))?;
    let index = VALKEY_COMMAND_INDEX.fetch_add(1, Ordering::Relaxed) % connections.len();
    Ok(connections[index].clone())
}

async fn create_session(
    _client: &redis::Client,
    user: &AuthUser,
) -> Result<(Uuid, String), AppError> {
    let id = Uuid::now_v7();
    let mut csrf_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut csrf_bytes);
    let csrf = hex::encode(csrf_bytes);
    let user_json = serde_json::to_string(user)
        .map_err(|_| AppError::bad_request("could not create session"))?;
    let mut connection = valkey_commands()?;
    redis::pipe()
        .atomic()
        .cmd("HSET")
        .arg(session_key(id))
        .arg("csrf")
        .arg(&csrf)
        .arg("user")
        .arg(user_json)
        .ignore()
        .cmd("EXPIRE")
        .arg(session_key(id))
        .arg(SESSION_TTL_SECONDS)
        .ignore()
        .cmd("SADD")
        .arg(format!("vussa:user_sessions:{}", user.id))
        .arg(id.to_string())
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok((id, csrf))
}

async fn load_session(headers: &HeaderMap, _client: &redis::Client) -> Result<Session, AppError> {
    let raw = cookie_value(headers, "vussa_session")
        .ok_or_else(|| AppError::unauthorized("authentication required"))?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| AppError::unauthorized("authentication required"))?;
    let mut connection = valkey_commands()?;
    let values: Vec<Option<String>> = redis::cmd("HMGET")
        .arg(session_key(id))
        .arg(&["csrf", "user"])
        .query_async(&mut connection)
        .await?;
    let csrf = values
        .first()
        .and_then(Clone::clone)
        .ok_or_else(|| AppError::unauthorized("session expired"))?;
    let user: AuthUser = serde_json::from_str(
        values
            .get(1)
            .and_then(Clone::clone)
            .as_deref()
            .ok_or_else(|| AppError::unauthorized("session expired"))?,
    )
    .map_err(|_| AppError::unauthorized("session expired"))?;
    let _: bool = connection
        .expire(session_key(id), SESSION_TTL_SECONDS as i64)
        .await?;
    Ok(Session { id, csrf, user })
}

fn require_csrf(headers: &HeaderMap, session: &Session) -> Result<(), AppError> {
    if headers.get(CSRF_HEADER).and_then(|v| v.to_str().ok()) != Some(session.csrf.as_str()) {
        return Err(AppError::bad_request("invalid csrf token"));
    }
    Ok(())
}

fn require_permission(user: &AuthUser, permission: &str) -> Result<(), AppError> {
    if user.permissions.iter().any(|value| value == permission) {
        Ok(())
    } else {
        Err(AppError::forbidden("permission denied"))
    }
}

fn auth_cookie(id: Uuid) -> HeaderValue {
    let secure = if env::var("COOKIE_SECURE").unwrap_or_else(|_| "false".into()) == "true" {
        "; Secure"
    } else {
        ""
    };
    HeaderValue::from_str(&format!(
        "vussa_session={id}; Path=/; HttpOnly{secure}; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}"
    ))
    .unwrap()
}

async fn register(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegisterRequest>,
) -> Result<(StatusCode, HeaderMap, Json<AuthUser>), AppError> {
    let email = request.email.trim().to_lowercase();
    let username = request.username.trim().to_string();
    if !email.contains('@') || email.len() > 320 {
        return Err(AppError::bad_request("invalid email"));
    }
    if username.len() < 2
        || username.len() > 40
        || !username
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AppError::bad_request("invalid username"));
    }
    let hash = password_hash(&request.password)?;
    let user = state
        .repository
        .register_user(&email, &username, &hash)
        .await
        .map_err(map_conflict)?;
    let (id, csrf) = create_session(&state.valkey, &user).await?;
    let mut headers = HeaderMap::new();
    headers.insert("set-cookie", auth_cookie(id));
    headers.insert(CSRF_HEADER, HeaderValue::from_str(&csrf).unwrap());
    Ok((StatusCode::CREATED, headers, Json(user)))
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(request): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<AuthUser>), AppError> {
    let found = state
        .repository
        .find_user_for_login(request.email.trim())
        .await?;
    let Some((user, hash, disabled)) = found else {
        return Err(AppError::unauthorized("invalid credentials"));
    };
    if disabled {
        return Err(AppError::unauthorized("invalid credentials"));
    }
    let verified = verify_password_coalesced(&state, user.id, request.password, hash).await?;
    if !verified {
        return Err(AppError::unauthorized("invalid credentials"));
    }
    AUTHENTICATIONS.fetch_add(1, Ordering::Relaxed);
    let (id, csrf) = create_session(&state.valkey, &user).await?;
    let mut headers = HeaderMap::new();
    headers.insert("set-cookie", auth_cookie(id));
    headers.insert(CSRF_HEADER, HeaderValue::from_str(&csrf).unwrap());
    Ok((headers, Json(user)))
}

async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let mut connection = valkey_commands()?;
    let _: usize = connection.del(session_key(session.id)).await?;
    let _: usize = connection
        .srem(
            format!("vussa:user_sessions:{}", session.user.id),
            session.id.to_string(),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn me(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<AuthUser>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(CSRF_HEADER, HeaderValue::from_str(&session.csrf).unwrap());
    Ok((response_headers, Json(session.user)))
}

async fn update_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<AccountUpdateRequest>,
) -> Result<Json<AuthUser>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let username = request.username.trim();
    if username.len() < 2 || username.len() > 40 {
        return Err(AppError::bad_request("invalid username"));
    }
    state
        .repository
        .update_username(session.user.id, username)
        .await?;
    let user = AuthUser {
        username: username.to_string(),
        ..session.user
    };
    let serialized = serde_json::to_string(&user)
        .map_err(|_| AppError::bad_request("could not update session"))?;
    let mut connection = valkey_commands()?;
    let _: usize = connection
        .hset(session_key(session.id), "user", serialized)
        .await?;
    Ok(Json(user))
}

async fn admin_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    admin_users_query(state, headers, query).await
}

async fn admin_users_query(
    state: Arc<AppState>,
    headers: HeaderMap,
    query: AdminListQuery,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let search = query.q.unwrap_or_default();
    let rows = sqlx::query("SELECT u.id,u.email,u.username,u.disabled_at,u.deleted_at,u.created_at,u.updated_at, ARRAY(SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=u.id) AS roles, ARRAY(SELECT DISTINCT p.name FROM permissions p JOIN role_permissions rp ON rp.permission_id=p.id JOIN user_roles ur ON ur.role_id=rp.role_id WHERE ur.user_id=u.id) AS permissions FROM users u WHERE u.deleted_at IS NULL AND ($1 = '' OR lower(u.username) LIKE lower('%' || $1 || '%') OR lower(u.email) LIKE lower('%' || $1 || '%')) AND ($2::uuid IS NULL OR u.id > $2) ORDER BY u.id LIMIT $3")
        .bind(search).bind(query.after).bind(limit + 1).fetch_all(&state.database).await?;
    let has_more = rows.len() > limit as usize;
    let mut users = rows
        .into_iter()
        .map(|row| AdminUserView {
            user: AuthUser {
                id: row.get("id"),
                email: row.get("email"),
                username: row.get("username"),
                roles: row.try_get("roles").unwrap_or_default(),
                permissions: row.try_get("permissions").unwrap_or_default(),
            },
            disabled_at: row.get("disabled_at"),
            deleted_at: row.get("deleted_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect::<Vec<_>>();
    if users.len() > limit as usize {
        users.truncate(limit as usize);
    }
    let next = if has_more {
        users.last().map(|user| user.user.id)
    } else {
        None
    };
    Ok(Json(serde_json::json!({"items": users, "next": next})))
}

async fn admin_disable_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(user): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "users:write")?;
    state
        .repository
        .set_user_disabled(session.user.id, user, true)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_enable_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(user): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "users:write")?;
    state
        .repository
        .set_user_disabled(session.user.id, user, false)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_delete_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(user): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "users:write")?;
    if user == session.user.id {
        return Err(AppError::bad_request("you cannot delete your own account"));
    }
    let mut tx = state.database.begin().await?;
    let result = sqlx::query("UPDATE users SET deleted_at=COALESCE(deleted_at,$1), disabled_at=COALESCE(disabled_at,$1), role_version=role_version+1, updated_at=$1 WHERE id=$2 AND deleted_at IS NULL").bind(now_millis() as i64).bind(user).execute(&mut *tx).await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'user.deleted','user',$3,$4)").bind(Uuid::now_v7()).bind(session.user.id).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)").bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_reset_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(user): axum::extract::Path<Uuid>,
    Json(request): Json<PasswordResetRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "users:write")?;
    let hash = password_hash(&request.password)?;
    let mut tx = state.database.begin().await?;
    let result = sqlx::query(
        "UPDATE users SET password_hash=$1,updated_at=$2 WHERE id=$3 AND deleted_at IS NULL",
    )
    .bind(hash)
    .bind(now_millis() as i64)
    .bind(user)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'user.password_reset','user',$3,$4)").bind(Uuid::now_v7()).bind(session.user.id).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)").bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_invalidate_sessions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(user): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "users:write")?;
    let mut tx = state.database.begin().await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'user.sessions_invalidated','user',$3,$4)").bind(Uuid::now_v7()).bind(session.user.id).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)").bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_roles(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    let rows = sqlx::query("SELECT r.name AS role, COALESCE(array_agg(p.name ORDER BY p.name) FILTER (WHERE p.name IS NOT NULL), ARRAY[]::text[]) AS permissions FROM roles r LEFT JOIN role_permissions rp ON rp.role_id=r.id LEFT JOIN permissions p ON p.id=rp.permission_id GROUP BY r.name ORDER BY r.name").fetch_all(&state.database).await?;
    let roles = rows.into_iter().map(|row| serde_json::json!({"role": row.get::<String,_>("role"), "permissions": row.get::<Vec<String>,_>("permissions")})).collect::<Vec<_>>();
    Ok(Json(
        serde_json::json!({"roles": roles, "fixed": ["user", "moderator", "admin"]}),
    ))
}

async fn admin_permissions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    let rows = sqlx::query("SELECT name FROM permissions ORDER BY name")
        .fetch_all(&state.database)
        .await?;
    Ok(Json(
        serde_json::json!({"permissions": rows.into_iter().map(|row| row.get::<String,_>("name")).collect::<Vec<_>>() }),
    ))
}

async fn admin_participants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(channel): axum::extract::Path<String>,
) -> Result<Json<Vec<Participant>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    Ok(Json(list_presence(&state.valkey, &channel).await?))
}

async fn admin_operations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")
            .fetch_one(&state.database)
            .await?;
    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE disabled_at IS NULL")
        .fetch_one(&state.database)
        .await?;
    let mut connection = valkey_commands()?;
    let _: String = redis::cmd("PING").query_async(&mut connection).await?;
    Ok(Json(
        serde_json::json!({"postgres":"ok", "valkey":"ok", "sessions":"valkey", "websockets": ACTIVE_WEBSOCKETS.load(Ordering::Relaxed), "outbox_pending": pending, "active_users": users, "cache":"valkey"}),
    ))
}

async fn admin_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "channels:read")?;
    let search = query.q.unwrap_or_default();
    let rows = sqlx::query("SELECT c.id,c.name,c.kind,c.owner_user_id,c.description,c.created_at,c.archived_at,c.deleted_at,c.retention_days,(SELECT count(*) FROM channel_members cm WHERE cm.channel_id=c.id) AS member_count FROM channels c WHERE ($1='' OR lower(c.name) LIKE lower('%'||$1||'%')) ORDER BY (c.name='main') DESC,c.name LIMIT $2").bind(search).bind(query.limit.unwrap_or(100).clamp(1,200)).fetch_all(&state.database).await?;
    let items = rows.into_iter().map(|row| serde_json::json!({"id":row.get::<Uuid,_>("id"),"name":row.get::<String,_>("name"),"kind":row.get::<String,_>("kind"),"owner_user_id":row.get::<Option<Uuid>,_>("owner_user_id"),"description":row.get::<String,_>("description"),"created_at":row.get::<i64,_>("created_at"),"archived_at":row.get::<Option<i64>,_>("archived_at"),"deleted_at":row.get::<Option<i64>,_>("deleted_at"),"retention_days":row.get::<i32,_>("retention_days"),"member_count":row.get::<i64,_>("member_count")})).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"items":items})))
}

async fn admin_create_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ChannelAdminRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "channels:write")?;
    let name = normalize_channel_name(request.name.as_deref().unwrap_or(""))?;
    if name == MAIN_CHANNEL {
        return Err(AppError::bad_request("main is reserved"));
    }
    if !state.repository.create_channel(&name).await? {
        return Err(AppError::bad_request("channel already exists"));
    }
    sqlx::query("UPDATE channels SET description=COALESCE($1,description),retention_days=COALESCE($2,retention_days) WHERE name=$3").bind(request.description).bind(request.retention_days).bind(&name).execute(&state.database).await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({"name":name}))))
}

async fn admin_update_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    Json(request): Json<ChannelAdminRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "channels:write")?;
    let mut tx = state.database.begin().await?;
    let row =
        sqlx::query("SELECT name FROM channels WHERE id=$1 AND deleted_at IS NULL FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RepositoryError::NotFound)?;
    let current: String = row.get("name");
    let name = if let Some(raw) = request.name {
        let n = normalize_channel_name(&raw)?;
        if current == MAIN_CHANNEL && n != MAIN_CHANNEL {
            return Err(AppError::bad_request("main cannot be renamed"));
        }
        Some(n)
    } else {
        None
    };
    sqlx::query("UPDATE channels SET name=COALESCE($1,name),description=COALESCE($2,description),retention_days=COALESCE($3,retention_days) WHERE id=$4").bind(name).bind(request.description).bind(request.retention_days).bind(id).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.updated','channel',$3,$4)").bind(Uuid::now_v7()).bind(session.user.id).bind(id).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_channel_state(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((id, action)): axum::extract::Path<(Uuid, String)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "channels:write")?;
    let mut tx = state.database.begin().await?;
    let row = sqlx::query("SELECT name FROM channels WHERE id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(RepositoryError::NotFound)?;
    if row.get::<String, _>("name") == MAIN_CHANNEL {
        return Err(AppError::bad_request("main cannot be changed"));
    }
    let now = now_millis() as i64;
    let sql = match action.as_str() {
        "archive" => "UPDATE channels SET archived_at=COALESCE(archived_at,$1) WHERE id=$2",
        "restore" => "UPDATE channels SET archived_at=NULL WHERE id=$2",
        "delete" => "UPDATE channels SET deleted_at=COALESCE(deleted_at,$1) WHERE id=$2",
        "undelete" => "UPDATE channels SET deleted_at=NULL WHERE id=$2",
        _ => return Err(AppError::bad_request("unknown channel action")),
    };
    sqlx::query(sql)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,$3,'channel',$4,$5)").bind(Uuid::now_v7()).bind(session.user.id).bind(format!("channel.{action}")).bind(id).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "moderation:read")?;
    let deleted = query.deleted.unwrap_or(false);
    let rows=sqlx::query("SELECT m.id,c.name AS channel,m.username,m.text,m.created_at,m.edited,m.deleted_at,m.deletion_reason FROM messages m JOIN channels c ON c.id=m.channel_id WHERE ($1::text IS NULL OR c.name=$1) AND ($2::uuid IS NULL OR EXISTS(SELECT 1 FROM users u WHERE u.username=m.username AND u.id=$2)) AND ($3::bigint IS NULL OR m.created_at >= $3) AND ($4::bigint IS NULL OR m.created_at <= $4) AND (($5 AND m.deleted_at IS NOT NULL) OR (NOT $5 AND m.deleted_at IS NULL)) ORDER BY m.created_at DESC,m.id DESC LIMIT $6").bind(query.channel).bind(query.user).bind(query.from).bind(query.to).bind(deleted).bind(query.limit.unwrap_or(100).clamp(1,200)).fetch_all(&state.database).await?;
    let items=rows.into_iter().map(|row|serde_json::json!({"id":row.get::<Uuid,_>("id"),"channel":row.get::<String,_>("channel"),"username":row.get::<String,_>("username"),"text":row.get::<String,_>("text"),"created_at":row.get::<i64,_>("created_at"),"edited":row.get::<bool,_>("edited"),"deleted_at":row.get::<Option<i64>,_>("deleted_at"),"deletion_reason":row.get::<Option<String>,_>("deletion_reason")})).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"items":items})))
}

async fn admin_moderate_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((id, action)): axum::extract::Path<(Uuid, String)>,
    Json(request): Json<ModerationRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    let mut tx = state.database.begin().await?;
    let row = sqlx::query("SELECT m.channel_id,c.name AS channel FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(RepositoryError::NotFound)?;
    let sql = match action.as_str() {
        "delete" => {
            "UPDATE messages SET deleted_at=$1,deleted_by=$2,deletion_reason=$3 WHERE id=$4"
        }
        "restore" => {
            "UPDATE messages SET deleted_at=NULL,deleted_by=NULL,deletion_reason=NULL WHERE id=$1"
        }
        _ => return Err(AppError::bad_request("unknown moderation action")),
    };
    if action == "delete" {
        sqlx::query(sql)
            .bind(now_millis() as i64)
            .bind(session.user.id)
            .bind(request.reason.clone())
            .bind(id)
            .execute(&mut *tx)
            .await?;
    } else {
        sqlx::query(sql).bind(id).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,$3,'message',$4,jsonb_build_object('reason',$5),$6)").bind(Uuid::now_v7()).bind(session.user.id).bind(format!("message.{action}" )).bind(id).bind(request.reason.clone()).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    let channel: String = row.get("channel");
    let _: redis::RedisResult<usize> = valkey_commands()?.del(history_key(&channel)).await;
    let mut connection = valkey_commands()?;
    let _: redis::RedisResult<usize> = connection.del(history_order_key(&channel)).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_message_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "moderation:read")?;
    let rows = sqlx::query("SELECT id,editor_user_id,previous_text,created_at FROM message_edit_history WHERE message_id=$1 ORDER BY created_at DESC,id DESC").bind(id).fetch_all(&state.database).await?;
    Ok(Json(rows.into_iter().map(|row| serde_json::json!({"id":row.get::<Uuid,_>("id"),"editor_user_id":row.get::<Option<Uuid>,_>("editor_user_id"),"previous_text":row.get::<String,_>("previous_text"),"created_at":row.get::<i64,_>("created_at")})).collect()))
}

async fn admin_bulk_moderate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<BulkModerationRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    if request.ids.is_empty() || request.ids.len() > 100 {
        return Err(AppError::bad_request(
            "bulk moderation must contain 1–100 messages",
        ));
    }
    if request.action != "delete" && request.action != "restore" {
        return Err(AppError::bad_request("unknown moderation action"));
    }
    let mut results = Vec::with_capacity(request.ids.len());
    for id in request.ids {
        let result = if request.action == "delete" {
            sqlx::query("UPDATE messages SET deleted_at=$1,deleted_by=$2,deletion_reason=$3 WHERE id=$4 AND deleted_at IS NULL").bind(now_millis() as i64).bind(session.user.id).bind(request.reason.clone()).bind(id).execute(&state.database).await?
        } else {
            sqlx::query("UPDATE messages SET deleted_at=NULL,deleted_by=NULL,deletion_reason=NULL WHERE id=$1 AND deleted_at IS NOT NULL").bind(id).execute(&state.database).await?
        };
        results.push(serde_json::json!({"id": id, "updated": result.rows_affected() > 0}));
    }
    Ok(Json(serde_json::json!({"results": results})))
}

async fn admin_assign_role(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((user, role)): axum::extract::Path<(Uuid, String)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "roles:write")?;
    state
        .repository
        .assign_role(session.user.id, user, &role)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_remove_role(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((user, role)): axum::extract::Path<(Uuid, String)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "roles:write")?;
    let mut tx = state.database.begin().await?;
    if role == "admin" {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM user_roles ur JOIN roles r ON r.id=ur.role_id JOIN users u ON u.id=ur.user_id WHERE r.name='admin' AND u.deleted_at IS NULL AND u.disabled_at IS NULL").fetch_one(&mut *tx).await?;
        if count <= 1 {
            return Err(AppError::bad_request(
                "the final administrator cannot be removed",
            ));
        }
    }
    let result = sqlx::query("DELETE FROM user_roles ur USING roles r WHERE ur.role_id=r.id AND ur.user_id=$1 AND r.name=$2").bind(user).bind(&role).execute(&mut *tx).await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("UPDATE users SET role_version=role_version+1,updated_at=$1 WHERE id=$2")
        .bind(now_millis() as i64)
        .bind(user)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'user.role_removed','user',$3,jsonb_build_object('role',$4),$5)").bind(Uuid::now_v7()).bind(session.user.id).bind(user).bind(&role).bind(now_millis() as i64).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) VALUES ($1,'auth.invalidate',jsonb_build_object('user_id',$2::text),$3)").bind(Uuid::now_v7()).bind(user).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_audit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "audit:read")?;
    let rows = sqlx::query("SELECT id,actor_user_id,action,target_type,target_id,created_at,metadata FROM audit_events WHERE ($1::uuid IS NULL OR actor_user_id=$1) AND ($2::text IS NULL OR action=$2) AND ($3::uuid IS NULL OR target_id=$3) ORDER BY created_at DESC,id DESC LIMIT $4")
        .bind(query.actor).bind(query.action).bind(query.target).bind(query.limit.unwrap_or(100).clamp(1, 200)).fetch_all(&state.database).await?;
    let items = rows.into_iter().map(|row| serde_json::json!({"id":row.get::<Uuid,_>("id"),"actor_user_id":row.get::<Option<Uuid>,_>("actor_user_id"),"action":row.get::<String,_>("action"),"target_type":row.get::<String,_>("target_type"),"target_id":row.get::<Option<Uuid>,_>("target_id"),"metadata":row.get::<serde_json::Value,_>("metadata"),"created_at":row.get::<i64,_>("created_at")})).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"items": items})))
}

async fn bootstrap_admin(repository: &PostgresRepository) -> Result<(), RepositoryError> {
    let (Some(email), Some(password)) = (
        env::var("ADMIN_EMAIL").ok(),
        env::var("ADMIN_PASSWORD").ok(),
    ) else {
        return Ok(());
    };
    let hash =
        password_hash(&password).map_err(|error| RepositoryError::Migration(error.to_string()))?;
    let mut tx = repository.pool.begin().await?;
    let id = Uuid::now_v7();
    let now = now_millis() as i64;
    let row = sqlx::query("INSERT INTO users (id,email,username,password_hash,created_at,updated_at) VALUES ($1,lower($2),$3,$4,$5,$5) ON CONFLICT (lower(email)) DO UPDATE SET password_hash=EXCLUDED.password_hash,disabled_at=NULL,updated_at=EXCLUDED.updated_at RETURNING id")
        .bind(id).bind(&email).bind("admin").bind(hash).bind(now).fetch_one(&mut *tx).await?;
    sqlx::query("INSERT INTO user_roles (user_id,role_id,assigned_at) SELECT $1,id,$2 FROM roles WHERE name='admin' ON CONFLICT DO NOTHING")
        .bind(row.get::<Uuid,_>("id")).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

async fn run_outbox(pool: PgPool, _client: redis::Client) {
    loop {
        let result = sqlx::query("WITH next AS (SELECT id,topic,payload FROM outbox_events WHERE published_at IS NULL ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 32) SELECT id,topic,payload FROM next")
            .fetch_all(&pool).await;
        if let Ok(rows) = result {
            for row in rows {
                let id: Uuid = row.get("id");
                let topic: String = row.get("topic");
                let payload: serde_json::Value = row.get("payload");
                if let Ok(mut connection) = valkey_commands() {
                    if topic == "auth.invalidate" {
                        if let Some(user_id) =
                            payload.get("user_id").and_then(|value| value.as_str())
                        {
                            if let Ok(ids) = connection
                                .smembers::<_, Vec<String>>(format!(
                                    "vussa:user_sessions:{user_id}"
                                ))
                                .await
                            {
                                for session_id in ids {
                                    let _: redis::RedisResult<usize> = connection
                                        .del(session_key(
                                            Uuid::parse_str(&session_id).unwrap_or(Uuid::nil()),
                                        ))
                                        .await;
                                }
                            }
                            let _: redis::RedisResult<usize> = connection
                                .del(format!("vussa:user_sessions:{user_id}"))
                                .await;
                        }
                    }
                    let sent: redis::RedisResult<i32> = redis::cmd("PUBLISH")
                        .arg(&topic)
                        .arg(payload.to_string())
                        .query_async(&mut connection)
                        .await;
                    if sent.is_ok() {
                        let _ = sqlx::query("UPDATE outbox_events SET published_at=$1,attempts=attempts+1 WHERE id=$2 AND published_at IS NULL").bind(now_millis() as i64).bind(id).execute(&pool).await;
                    } else {
                        let _ =
                            sqlx::query("UPDATE outbox_events SET attempts=attempts+1 WHERE id=$1")
                                .bind(id)
                                .execute(&pool)
                                .await;
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn map_conflict(error: RepositoryError) -> AppError {
    match error {
        RepositoryError::Database(sqlx::Error::Database(_)) => {
            AppError::bad_request("email or username already exists")
        }
        other => other.into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let valkey_url = env::var("VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let valkey = redis::Client::open(valkey_url)?;
    let valkey_pool_size = env::var("VALKEY_POOL_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
        .clamp(1, 64);
    let mut valkey_connections = Vec::with_capacity(valkey_pool_size);
    for _ in 0..valkey_pool_size {
        valkey_connections.push(valkey.get_multiplexed_async_connection().await?);
    }
    VALKEY_COMMANDS.set(valkey_connections).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Valkey command pool already initialized",
        )
    })?;
    info!(valkey_pool_size, "Valkey command pool configured");
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://vussa_chat:vussa_chat@127.0.0.1:5432/vussa_chat".into());
    let repository = PostgresRepository::connect(&database_url).await?;
    repository.ensure_main_channel().await?;
    repository.seed_authorization().await?;
    if env::var("SEED_TEST_ACCOUNTS").as_deref() == Ok("true") {
        repository.seed_test_accounts().await?;
    }
    bootstrap_admin(&repository).await?;
    tokio::spawn(run_outbox(repository.pool.clone(), valkey.clone()));
    let retention_repository = repository.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            if let Err(error) = retention_repository.prune_expired().await {
                error!(?error, "failed to prune expired messages");
            }
        }
    });

    let rooms = RoomManager::start(&valkey).await?;
    let password_verifier_limit = password_verifier_limit();
    info!(
        password_verifier_limit,
        "password verification concurrency configured"
    );
    let state = Arc::new(AppState {
        valkey,
        database: repository.pool.clone(),
        repository,
        rooms,
        password_verifiers: Arc::new(Semaphore::new(password_verifier_limit)),
        password_verification_flights: Arc::new(TokioMutex::new(HashMap::new())),
    });
    let app = routes::build(state);

    let address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = TcpListener::bind(&address).await?;
    info!(%address, "chat backend listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler")
                .recv()
                .await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    info!("shutdown signal received; draining HTTP and WebSocket connections");
}

async fn health() -> &'static str {
    "ok"
}

async fn live() -> &'static str {
    "ok"
}
async fn startup() -> &'static str {
    "ok"
}
async fn metrics() -> ([(HeaderName, HeaderValue); 1], String) {
    let body = format!(
        "# TYPE vussa_authentications_total counter\nvussa_authentications_total {}\n# TYPE vussa_active_websockets gauge\nvussa_active_websockets {}\n",
        AUTHENTICATIONS.load(Ordering::Relaxed),
        ACTIVE_WEBSOCKETS.load(Ordering::Relaxed)
    );
    (
        [(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("text/plain; version=0.0.4"),
        )],
        body,
    )
}
async fn ready(State(state): State<Arc<AppState>>) -> Result<&'static str, StatusCode> {
    let mut connection = valkey_commands().map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let _: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    sqlx::query("SELECT 1")
        .execute(&state.database)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok("ok")
}

async fn list_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<Channel>>, AppError> {
    let _ = load_session(&headers, &state.valkey).await?;
    let names = state.repository.list_channels().await?;
    Ok(Json(
        names.into_iter().map(|name| Channel { name }).collect(),
    ))
}

async fn list_conversations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ConversationSummary>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    Ok(Json(
        list_visible_conversations(&state.database, session.user.id).await?,
    ))
}

async fn search_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<Vec<UserSearchResult>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let search = query.q.unwrap_or_default();
    let rows = sqlx::query("SELECT id,username FROM users WHERE id<>$1 AND disabled_at IS NULL AND deleted_at IS NULL AND ($2='' OR lower(username) LIKE lower('%'||$2||'%') OR lower(email) LIKE lower('%'||$2||'%')) ORDER BY username LIMIT 20")
        .bind(session.user.id).bind(search).fetch_all(&state.database).await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| UserSearchResult {
                id: row.get("id"),
                username: row.get("username"),
            })
            .collect(),
    ))
}

async fn create_private_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ConversationSummary>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let name = normalize_channel_name(&request.name)?;
    let mut tx = state.database.begin().await?;
    let id = Uuid::now_v7();
    let now = now_millis() as i64;
    let result = sqlx::query("INSERT INTO channels (id,name,kind,owner_user_id,created_at) VALUES ($1,$2,'private',$3,$4) ON CONFLICT (name) DO NOTHING")
        .bind(id).bind(&name).bind(session.user.id).bind(now).execute(&mut *tx).await?;
    if result.rows_affected() == 0 {
        return Err(AppError::bad_request("channel already exists"));
    }
    sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'owner',$2,$3)")
        .bind(id).bind(session.user.id).bind(now).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.private_created','channel',$3,$4)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(id).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    let summary = list_visible_conversations(&state.database, session.user.id)
        .await?
        .into_iter()
        .find(|item| item.id == id)
        .ok_or(RepositoryError::NotFound)?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn open_direct_conversation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<DirectConversationRequest>,
) -> Result<Json<ConversationSummary>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    if request.user_id == session.user.id {
        return Err(AppError::bad_request("you cannot message yourself"));
    }
    let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)")
        .bind(request.user_id).fetch_one(&state.database).await?;
    if !target_exists {
        return Err(RepositoryError::NotFound.into());
    }
    let (first, second) = if session.user.id < request.user_id {
        (session.user.id, request.user_id)
    } else {
        (request.user_id, session.user.id)
    };
    let direct_key = format!("{first}:{second}");
    let mut tx = state.database.begin().await?;
    let now = now_millis() as i64;
    let id = Uuid::now_v7();
    let name = format!("dm_{id}");
    let row = sqlx::query("INSERT INTO channels (id,name,kind,direct_key,created_at) VALUES ($1,$2,'direct',$3,$4) ON CONFLICT (direct_key) WHERE kind='direct' AND deleted_at IS NULL DO UPDATE SET deleted_at=NULL RETURNING id")
        .bind(id).bind(&name).bind(&direct_key).bind(now).fetch_one(&mut *tx).await?;
    let channel_id: Uuid = row.get("id");
    for member in [first, second] {
        sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
            .bind(channel_id).bind(member).bind(session.user.id).bind(now).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let summary = list_visible_conversations(&state.database, session.user.id)
        .await?
        .into_iter()
        .find(|item| item.id == channel_id)
        .ok_or(RepositoryError::NotFound)?;
    Ok(Json(summary))
}

async fn list_channel_members(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<Vec<ChannelMember>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    ensure_channel_access(&state.database, &name, session.user.id).await?;
    Ok(Json(channel_members(&state.database, &name).await?))
}

async fn invite_channel_member(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(request): Json<MemberRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let channel_id = owned_private_channel(&state.database, &name, session.user.id).await?;
    let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)").bind(request.user_id).fetch_one(&state.database).await?;
    if !target_exists {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
        .bind(channel_id).bind(request.user_id).bind(session.user.id).bind(now_millis() as i64).execute(&state.database).await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_added','channel',$3,jsonb_build_object('user_id',$4),$5)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(request.user_id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_channel_member(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((name, user_id)): axum::extract::Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let channel_id = owned_private_channel(&state.database, &name, session.user.id).await?;
    let result = sqlx::query("DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2 AND membership_role <> 'owner'")
        .bind(channel_id).bind(user_id).execute(&state.database).await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn list_visible_conversations(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<ConversationSummary>, AppError> {
    let rows = sqlx::query("SELECT c.id,c.name,c.kind,c.owner_user_id,CASE WHEN c.kind='direct' THEN COALESCE(peer.username,c.name) ELSE c.name END AS display_name,peer.id AS peer_user_id,peer.username AS peer_username FROM channels c LEFT JOIN channel_members mine ON mine.channel_id=c.id AND mine.user_id=$1 LEFT JOIN channel_members other ON other.channel_id=c.id AND other.user_id<>$1 LEFT JOIN users peer ON peer.id=other.user_id WHERE c.deleted_at IS NULL AND (c.kind='public' OR mine.user_id IS NOT NULL) ORDER BY (c.kind='public' AND c.name='main') DESC,c.kind,c.name")
        .bind(user_id).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|row| ConversationSummary {
            id: row.get("id"),
            name: row.get("name"),
            kind: row.get("kind"),
            owner_user_id: row.get("owner_user_id"),
            display_name: row.get("display_name"),
            peer_user_id: row.get("peer_user_id"),
            peer_username: row.get("peer_username"),
        })
        .collect())
}

async fn channel_members(pool: &PgPool, name: &str) -> Result<Vec<ChannelMember>, AppError> {
    let rows = sqlx::query("SELECT u.id AS user_id,u.username,cm.membership_role FROM channel_members cm JOIN channels c ON c.id=cm.channel_id JOIN users u ON u.id=cm.user_id WHERE c.name=$1 AND c.deleted_at IS NULL ORDER BY cm.membership_role DESC,u.username")
        .bind(name).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|row| ChannelMember {
            user_id: row.get("user_id"),
            username: row.get("username"),
            membership_role: row.get("membership_role"),
        })
        .collect())
}

async fn ensure_channel_access(pool: &PgPool, name: &str, user_id: Uuid) -> Result<(), AppError> {
    let allowed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM channels c LEFT JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=$2 WHERE c.name=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR cm.user_id IS NOT NULL))")
        .bind(name).bind(user_id).fetch_one(pool).await?;
    if allowed {
        Ok(())
    } else {
        Err(AppError::forbidden("conversation access denied"))
    }
}

async fn owned_private_channel(pool: &PgPool, name: &str, user_id: Uuid) -> Result<Uuid, AppError> {
    sqlx::query_scalar("SELECT id FROM channels WHERE name=$1 AND kind='private' AND owner_user_id=$2 AND deleted_at IS NULL")
        .bind(name).bind(user_id).fetch_optional(pool).await?.ok_or_else(|| AppError::forbidden("only the private channel owner can manage members"))
}

async fn create_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<CreateChannelResponse>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let name = normalize_channel_name(&request.name)?;
    if !state.repository.create_channel(&name).await? {
        return Err(AppError::bad_request("channel already exists"));
    }
    Ok((StatusCode::CREATED, Json(CreateChannelResponse { name })))
}

fn normalize_channel_name(raw: &str) -> Result<String, AppError> {
    let name = raw.trim();
    if name.is_empty() || name.len() > 40 {
        return Err(AppError::bad_request(
            "channel name must be 1–40 characters",
        ));
    }
    if name == MAIN_CHANNEL
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AppError::bad_request(
            "channel name may contain only letters, numbers, '-' and '_'",
        ));
    }
    Ok(name.to_lowercase())
}

async fn websocket(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, session)))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, session: Session) {
    ACTIVE_WEBSOCKETS.fetch_add(1, Ordering::Relaxed);
    let session_id = session.id;
    let username = session.user.username.clone();
    let participant = Participant {
        user_id: session.user.id,
        username: username.clone(),
        roles: session.user.roles.clone(),
        online: true,
    };
    let mut channel = MAIN_CHANNEL.to_string();

    if send_event(
        &mut socket,
        &ServerEvent::Welcome {
            username: username.clone(),
        },
    )
    .await
    .is_err()
        || send_channels(&mut socket, state.repository.as_ref())
            .await
            .is_err()
        || send_private_conversations(&mut socket, &state.database, session.user.id)
            .await
            .is_err()
    {
        return;
    }

    let mut control_rx = state.rooms.subscribe_control();
    let mut room_rx = match state.rooms.subscribe(&channel).await {
        Ok(receiver) => receiver,
        Err(error) => {
            error!(?error, "could not subscribe to main room");
            return;
        }
    };
    if send_joined_with_history(
        &mut socket,
        &channel,
        &state.valkey,
        state.repository.as_ref(),
    )
    .await
    .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    if send_event(
        &mut socket,
        &ServerEvent::Members {
            channel: channel.clone(),
            members: channel_members(&state.database, &channel)
                .await
                .unwrap_or_default(),
        },
    )
    .await
    .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    if refresh_presence(&state.valkey, &channel, &participant)
        .await
        .is_err()
        || sync_presence(&mut socket, &state.valkey, &channel)
            .await
            .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    let _ = broadcast(
        &state.valkey,
        &channel,
        &ServerEvent::ParticipantJoined {
            channel: channel.clone(),
            participant: participant.clone(),
        },
    )
    .await;

    loop {
        tokio::select! {
            Some(result) = socket.next() => {
                match result {
                    Ok(Message::Text(text)) => {
                        let previous_channel = channel.clone();
                        match handle_client_event(&mut socket, &mut channel, &username, &session.user, session_id, &state, &text).await {
                            Ok(Some(new_channel)) => {
                                state.rooms.release(&previous_channel).await;
                                let _ = remove_presence(&state.valkey, &previous_channel, session.user.id).await;
                                let _ = broadcast(&state.valkey, &previous_channel, &ServerEvent::ParticipantLeft { channel: previous_channel.clone(), user_id: session.user.id }).await;
                                channel = new_channel;
                                match state.rooms.subscribe(&channel).await {
                                    Ok(receiver) => {
                                        room_rx = receiver;
                                        if send_joined_with_history(&mut socket, &channel, &state.valkey, state.repository.as_ref()).await.is_err() {
                                            break;
                                        }
                                        let _ = send_event(&mut socket, &ServerEvent::Members { channel: channel.clone(), members: channel_members(&state.database, &channel).await.unwrap_or_default() }).await;
                                        let _ = refresh_presence(&state.valkey, &channel, &participant).await;
                                        let _ = sync_presence(&mut socket, &state.valkey, &channel).await;
                                        let _ = broadcast(&state.valkey, &channel, &ServerEvent::ParticipantJoined { channel: channel.clone(), participant: participant.clone() }).await;
                                    }
                                    Err(error) => {
                                        let _ = send_event(&mut socket, &ServerEvent::Error { message: error.to_string() }).await;
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                let _ = send_event(&mut socket, &ServerEvent::Error { message: error.to_string() }).await;
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
            Ok(message) = control_rx.recv() => {
                if socket.send(message).await.is_err() { break; }
            }
            Ok(message) = room_rx.recv() => {
                if socket.send(message).await.is_err() { break; }
            }
            else => break,
        }
    }
    let _ = remove_presence(&state.valkey, &channel, session.user.id).await;
    let _ = broadcast(
        &state.valkey,
        &channel,
        &ServerEvent::ParticipantLeft {
            channel: channel.clone(),
            user_id: session.user.id,
        },
    )
    .await;
    state.rooms.release(&channel).await;
    ACTIVE_WEBSOCKETS.fetch_sub(1, Ordering::Relaxed);
}

async fn handle_client_event(
    socket: &mut WebSocket,
    channel: &mut String,
    username: &str,
    user: &AuthUser,
    session_id: Uuid,
    state: &AppState,
    text: &str,
) -> Result<Option<String>, AppError> {
    let event: ClientEvent =
        serde_json::from_str(text).map_err(|_| AppError::bad_request("invalid event"))?;
    let mut switch_to = None;
    match event {
        ClientEvent::ListChannels => {
            send_channels(socket, state.repository.as_ref()).await?;
            send_private_conversations(socket, &state.database, user.id).await?;
        }
        ClientEvent::CreateChannel { name } => {
            require_permission(user, "chat:write")?;
            let name = normalize_channel_name(&name)?;
            if !state.repository.create_channel(&name).await? {
                return Err(AppError::bad_request("channel already exists"));
            }
            broadcast_control(&state.valkey, &ServerEvent::ChannelCreated { name }).await?;
        }
        ClientEvent::CreatePrivateChannel { name } => {
            require_permission(user, "chat:write")?;
            let name = normalize_channel_name(&name)?;
            let mut tx = state.database.begin().await?;
            let id = Uuid::now_v7();
            let now = now_millis() as i64;
            let result = sqlx::query("INSERT INTO channels (id,name,kind,owner_user_id,created_at) VALUES ($1,$2,'private',$3,$4) ON CONFLICT (name) DO NOTHING")
                .bind(id).bind(&name).bind(user.id).bind(now).execute(&mut *tx).await?;
            if result.rows_affected() == 0 {
                return Err(AppError::bad_request("channel already exists"));
            }
            sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'owner',$2,$3)")
                .bind(id).bind(user.id).bind(now).execute(&mut *tx).await?;
            tx.commit().await?;
            send_private_conversations(socket, &state.database, user.id).await?;
        }
        ClientEvent::OpenDirect { user_id } => {
            require_permission(user, "chat:write")?;
            if user_id == user.id {
                return Err(AppError::bad_request("you cannot message yourself"));
            }
            let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)").bind(user_id).fetch_one(&state.database).await?;
            if !target_exists {
                return Err(RepositoryError::NotFound.into());
            }
            let (first, second) = if user.id < user_id {
                (user.id, user_id)
            } else {
                (user_id, user.id)
            };
            let direct_key = format!("{first}:{second}");
            let mut tx = state.database.begin().await?;
            let now = now_millis() as i64;
            let id = Uuid::now_v7();
            let name = format!("dm_{id}");
            let row = sqlx::query("INSERT INTO channels (id,name,kind,direct_key,created_at) VALUES ($1,$2,'direct',$3,$4) ON CONFLICT (direct_key) WHERE kind='direct' AND deleted_at IS NULL DO UPDATE SET deleted_at=NULL RETURNING name")
                .bind(id).bind(&name).bind(&direct_key).bind(now).fetch_one(&mut *tx).await?;
            let direct_name: String = row.get("name");
            let channel_id: Uuid = sqlx::query_scalar("SELECT id FROM channels WHERE name=$1")
                .bind(&direct_name)
                .fetch_one(&mut *tx)
                .await?;
            for member in [first, second] {
                sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
                    .bind(channel_id).bind(member).bind(user.id).bind(now).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            send_private_conversations(socket, &state.database, user.id).await?;
            *channel = direct_name.clone();
            switch_to = Some(direct_name);
        }
        ClientEvent::InviteMember {
            channel: target,
            user_id,
        } => {
            require_permission(user, "chat:write")?;
            let channel_id = owned_private_channel(&state.database, &target, user.id).await?;
            sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) SELECT $1,$2,'member',$3,$4 WHERE EXISTS(SELECT 1 FROM users WHERE id=$2 AND disabled_at IS NULL AND deleted_at IS NULL) ON CONFLICT DO NOTHING")
                .bind(channel_id).bind(user_id).bind(user.id).bind(now_millis() as i64).execute(&state.database).await?;
        }
        ClientEvent::RemoveMember {
            channel: target,
            user_id,
        } => {
            require_permission(user, "chat:write")?;
            let channel_id = owned_private_channel(&state.database, &target, user.id).await?;
            sqlx::query("DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2 AND membership_role <> 'owner'").bind(channel_id).bind(user_id).execute(&state.database).await?;
        }
        ClientEvent::JoinChannel { name } => {
            let name = normalize_channel_name(&name).or_else(|_| {
                if name == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            ensure_channel_access(&state.database, &name, user.id).await?;
            *channel = name.clone();
            switch_to = Some(name.clone());
        }
        ClientEvent::DeleteChannel { name } => {
            require_permission(user, "chat:moderate")?;
            let name = normalize_channel_name(&name).or_else(|_| {
                if name == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            if name == MAIN_CHANNEL {
                return Err(AppError::bad_request("the main channel cannot be removed"));
            }
            if !state.repository.delete_channel(&name).await? {
                return Err(AppError::bad_request("channel does not exist"));
            }
            broadcast_control(
                &state.valkey,
                &ServerEvent::ChannelDeleted { name: name.clone() },
            )
            .await?;
            if *channel == name {
                *channel = MAIN_CHANNEL.to_string();
                switch_to = Some(MAIN_CHANNEL.to_string());
            }
        }
        ClientEvent::SendMessage { text } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let text = text.trim();
            if text.is_empty() || text.len() > 2000 {
                return Err(AppError::bad_request("message must be 1–2000 characters"));
            }
            let message = ChatMessage {
                id: Uuid::now_v7(),
                channel: channel.clone(),
                username: username.to_string(),
                text: text.to_string(),
                created_at: now_millis(),
                edited: false,
                deleted: false,
            };
            state.repository.save_message(&message, session_id).await?;
            store_message(&state.valkey, &message, session_id).await?;
            let event = ServerEvent::Message { message };
            broadcast(&state.valkey, channel, &event).await?;
        }
        ClientEvent::EditMessage { id, text } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let text = text.trim();
            if text.is_empty() || text.len() > 2000 {
                return Err(AppError::bad_request("message must be 1–2000 characters"));
            }
            let message = state
                .repository
                .edit_message(channel, id, session_id, text)
                .await?;
            update_hot_message(&state.valkey, &message).await?;
            broadcast(
                &state.valkey,
                channel,
                &ServerEvent::MessageUpdated { message },
            )
            .await?;
        }
        ClientEvent::DeleteMessage { id } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let message = state.repository.delete_message(channel, id, session_id).await?;
            update_hot_message(&state.valkey, &message).await?;
            broadcast(&state.valkey, channel, &ServerEvent::MessageUpdated { message }).await?;
        }
        ClientEvent::LoadHistory {
            channel: requested_channel,
            before_created_at,
            before_id,
        } => {
            let requested_channel = normalize_channel_name(&requested_channel).or_else(|_| {
                if requested_channel == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            if requested_channel != *channel {
                // A scroll event from the previous view can arrive while a
                // channel switch is completing. The request is stale, not an
                // invalid user action, so discard it without surfacing an
                // error toast.
                return Ok(switch_to);
            }
            ensure_channel_access(&state.database, &requested_channel, user.id).await?;
            send_history_page(
                socket,
                &requested_channel,
                &state.valkey,
                state.repository.as_ref(),
                Some((before_created_at, before_id)),
            )
            .await?;
        }
        ClientEvent::Heartbeat => {
            let participant = Participant {
                user_id: user.id,
                username: username.to_string(),
                roles: user.roles.clone(),
                online: true,
            };
            // Heartbeats are best-effort: a transient sync write must not be
            // reported as a rejected chat action. The next heartbeat or
            // reconnect will reconcile the channel state.
            // Heartbeats stay entirely in Valkey. Channel access was checked
            // when the socket joined, and presence reconciliation is sent on
            // connect/channel switch rather than on every refresh.
            let _ = refresh_presence(&state.valkey, channel, &participant).await;
        }
    }
    Ok(switch_to)
}

fn history_key(channel: &str) -> String {
    format!("chat:history:{channel}:messages")
}

fn history_order_key(channel: &str) -> String {
    format!("chat:history:{channel}:order")
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn send_joined_with_history(
    socket: &mut WebSocket,
    channel: &str,
    client: &redis::Client,
    repository: &dyn ChatRepository,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::Joined {
            name: channel.to_string(),
        },
    )
    .await?;
    send_history_page(socket, channel, client, repository, None).await
}

async fn send_history_page(
    socket: &mut WebSocket,
    channel: &str,
    client: &redis::Client,
    repository: &dyn ChatRepository,
    before: Option<(u64, Uuid)>,
) -> Result<(), AppError> {
    let cache_before = before.map(|(created_at, id)| (created_at as isize, id));
    let cache_limit = if before.is_none() {
        HOT_HISTORY_LIMIT
    } else {
        HISTORY_PAGE_SIZE
    };
    let mut messages = load_hot_history_before(client, channel, cache_before).await?;
    let mut source = HistorySource::Cache;
    let mut has_more = messages.len() >= cache_limit;
    if messages.len() < cache_limit {
        source = HistorySource::Database;
        let database_messages = repository
            .load_messages(
                channel,
                before.map(|(created_at, id)| (created_at as i64, id)),
                (cache_limit + 1) as i64,
            )
            .await?;
        has_more = database_messages.len() > cache_limit;
        if before.is_none() {
            let cache_start = database_messages.len().saturating_sub(HOT_HISTORY_LIMIT);
            hydrate_hot_history(client, channel, &database_messages[cache_start..]).await?;
        }
        messages = database_messages;
    }
    messages.sort_by_key(|message| (message.created_at, message.id));
    if before.is_none() && messages.len() > HISTORY_PAGE_SIZE {
        messages = messages.split_off(messages.len() - HISTORY_PAGE_SIZE);
    } else if before.is_some() && messages.len() > HISTORY_PAGE_SIZE {
        messages.truncate(HISTORY_PAGE_SIZE);
    }
    let event = if before.is_some() {
        ServerEvent::HistoryPage {
            channel: channel.to_string(),
            messages,
            source,
            has_more,
        }
    } else {
        ServerEvent::History {
            channel: channel.to_string(),
            messages,
            source,
            has_more,
        }
    };
    send_event(socket, &event).await
}

async fn load_hot_history(
    _client: &redis::Client,
    channel: &str,
) -> Result<Vec<ChatMessage>, AppError> {
    let mut connection = valkey_commands()?;
    let ids: Vec<String> = connection
        .zrange(
            history_order_key(channel),
            -(HOT_HISTORY_LIMIT as isize),
            -1,
        )
        .await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let payloads: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
        .arg(history_key(channel))
        .arg(&ids)
        .query_async(&mut connection)
        .await?;
    let messages = payloads
        .into_iter()
        .flatten()
        .filter_map(|payload| bitcode::decode::<StoredMessage>(&payload).ok())
        .map(StoredMessage::into_message)
        .collect();
    Ok(messages)
}

async fn load_hot_history_before(
    client: &redis::Client,
    channel: &str,
    before: Option<(isize, Uuid)>,
) -> Result<Vec<ChatMessage>, AppError> {
    if before.is_none() {
        return load_hot_history(client, channel).await;
    }
    // The hot tier intentionally contains only the newest 300 messages. Read
    // the bounded tier once and compare the full (created_at, id) cursor in
    // Rust so messages sharing a millisecond are not skipped.
    let (created_at, id) = before.expect("checked above");
    let mut messages = load_hot_history(client, channel)
        .await?
        .into_iter()
        .filter(|message| (message.created_at as isize, message.id) < (created_at, id))
        .collect::<Vec<_>>();
    messages.sort_by_key(|message| (message.created_at, message.id));
    if messages.len() > HISTORY_PAGE_SIZE {
        messages = messages.split_off(messages.len() - HISTORY_PAGE_SIZE);
    }
    Ok(messages)
}

async fn hydrate_hot_history(
    _client: &redis::Client,
    channel: &str,
    messages: &[ChatMessage],
) -> Result<(), AppError> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut connection = valkey_commands()?;
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    for message in messages {
        let record = bitcode::encode(&StoredMessage::from_message(message.clone(), Uuid::nil()));
        pipeline
            .cmd("HSET")
            .arg(history_key(channel))
            .arg(message.id.to_string())
            .arg(record)
            .ignore()
            .cmd("ZADD")
            .arg(history_order_key(channel))
            .arg(message.created_at)
            .arg(message.id.to_string())
            .ignore();
    }
    pipeline.query_async::<()>(&mut connection).await?;
    Ok(())
}

async fn store_message(
    _client: &redis::Client,
    message: &ChatMessage,
    owner_session: Uuid,
) -> Result<(), AppError> {
    let record = bitcode::encode(&StoredMessage::from_message(message.clone(), owner_session));
    let mut connection = valkey_commands()?;
    redis::pipe()
        .atomic()
        .cmd("HSET")
        .arg(history_key(&message.channel))
        .arg(message.id.to_string())
        .arg(record)
        .ignore()
        .cmd("ZADD")
        .arg(history_order_key(&message.channel))
        .arg(message.created_at)
        .arg(message.id.to_string())
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;

    let old_ids: Vec<String> = connection
        .zrange(
            history_order_key(&message.channel),
            0,
            -((HOT_HISTORY_LIMIT + 1) as isize),
        )
        .await?;
    if !old_ids.is_empty() {
        redis::pipe()
            .cmd("ZREM")
            .arg(history_order_key(&message.channel))
            .arg(&old_ids)
            .ignore()
            .cmd("HDEL")
            .arg(history_key(&message.channel))
            .arg(&old_ids)
            .ignore()
            .query_async::<()>(&mut connection)
            .await?;
    }
    Ok(())
}

async fn update_hot_message(client: &redis::Client, message: &ChatMessage) -> Result<(), AppError> {
    store_message(client, message, Uuid::nil()).await
}

async fn channel_exists(repository: &dyn ChatRepository, name: &str) -> Result<bool, AppError> {
    Ok(repository
        .list_channels()
        .await?
        .iter()
        .any(|channel| channel == name))
}

async fn send_channels(
    socket: &mut WebSocket,
    repository: &dyn ChatRepository,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::Channels {
            channels: repository.list_channels().await?,
        },
    )
    .await
}

async fn send_private_conversations(
    socket: &mut WebSocket,
    pool: &PgPool,
    user_id: Uuid,
) -> Result<(), AppError> {
    let conversations = list_visible_conversations(pool, user_id)
        .await?
        .into_iter()
        .filter(|conversation| conversation.kind != "public")
        .collect();
    send_event(socket, &ServerEvent::PrivateConversations { conversations }).await
}

async fn broadcast_control(client: &redis::Client, event: &ServerEvent) -> Result<(), AppError> {
    broadcast(client, "_control", event).await
}

fn room_key(channel: &str) -> String {
    format!("chat:room:{channel}")
}

fn presence_set_key(channel: &str) -> String {
    format!("chat:presence:{channel}:users")
}

fn presence_key(channel: &str, user_id: Uuid) -> String {
    format!("chat:presence:{channel}:{user_id}")
}

async fn refresh_presence(
    _client: &redis::Client,
    channel: &str,
    participant: &Participant,
) -> Result<(), AppError> {
    let mut connection = valkey_commands()?;
    let payload = serde_json::to_string(participant)
        .map_err(|_| AppError::bad_request("could not encode presence"))?;
    redis::pipe()
        .atomic()
        .cmd("SADD")
        .arg(presence_set_key(channel))
        .arg(participant.user_id.to_string())
        .ignore()
        .cmd("SETEX")
        .arg(presence_key(channel, participant.user_id))
        .arg(PRESENCE_TTL_SECONDS)
        .arg(payload)
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok(())
}

async fn remove_presence(
    _client: &redis::Client,
    channel: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    let mut connection = valkey_commands()?;
    redis::pipe()
        .atomic()
        .cmd("SREM")
        .arg(presence_set_key(channel))
        .arg(user_id.to_string())
        .ignore()
        .cmd("DEL")
        .arg(presence_key(channel, user_id))
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok(())
}

async fn list_presence(
    _client: &redis::Client,
    channel: &str,
) -> Result<Vec<Participant>, AppError> {
    let mut connection = valkey_commands()?;
    let ids: Vec<String> = connection.smembers(presence_set_key(channel)).await?;
    let mut participants = Vec::with_capacity(ids.len());
    for id in ids {
        let Ok(user_id) = Uuid::parse_str(&id) else {
            continue;
        };
        let key = presence_key(channel, user_id);
        let value: Option<String> = connection.get(&key).await?;
        match value.and_then(|value| serde_json::from_str::<Participant>(&value).ok()) {
            Some(participant) => participants.push(participant),
            None => {
                let _: usize = connection.srem(presence_set_key(channel), id).await?;
            }
        }
    }
    participants.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(participants)
}

async fn sync_presence(
    socket: &mut WebSocket,
    client: &redis::Client,
    channel: &str,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::PresenceSync {
            channel: channel.to_string(),
            participants: list_presence(client, channel).await?,
        },
    )
    .await
}

async fn broadcast(
    _client: &redis::Client,
    channel: &str,
    event: &ServerEvent,
) -> Result<(), AppError> {
    let payload =
        serde_json::to_vec(event).map_err(|_| AppError::bad_request("could not encode event"))?;
    let mut connection = valkey_commands()?;
    let publish_channel = if channel == "_control" {
        channel.to_string()
    } else {
        format!("chat:room:{channel}")
    };
    let _: i32 = redis::cmd("PUBLISH")
        .arg(publish_channel)
        .arg(payload)
        .query_async(&mut connection)
        .await?;
    Ok(())
}

async fn send_event(socket: &mut WebSocket, event: &ServerEvent) -> Result<(), AppError> {
    let payload = serde_json::to_string(event)
        .map_err(|_| AppError::bad_request("could not encode event"))?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| AppError::bad_request("connection closed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_names_are_normalized_and_bounded() {
        assert_eq!(normalize_channel_name("  Team-Room  ").unwrap(), "team-room");
        assert!(normalize_channel_name("").is_err());
        assert!(normalize_channel_name("bad name").is_err());
        assert!(normalize_channel_name(&"a".repeat(41)).is_err());
    }

    #[test]
    fn history_keys_are_namespaced() {
        assert_eq!(history_key("main"), "chat:history:main:messages");
        assert_eq!(history_order_key("main"), "chat:history:main:order");
    }
}

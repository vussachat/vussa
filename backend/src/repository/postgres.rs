use super::*;

#[async_trait]
pub(crate) trait ChatRepository: Send + Sync {
    async fn ensure_main_channel(&self) -> Result<(), RepositoryError>;
    async fn list_channels(&self, user_id: Uuid) -> Result<Vec<String>, RepositoryError>;
    async fn create_channel(&self, name: &str) -> Result<bool, RepositoryError>;
    async fn save_message(
        &self,
        message: &ChatMessage,
        owner_session: Uuid,
        owner_user: Uuid,
    ) -> Result<(), RepositoryError>;
    async fn find_message_by_client_id(
        &self,
        channel: &str,
        owner_session: Uuid,
        client_id: &str,
    ) -> Result<Option<ChatMessage>, RepositoryError>;
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
        can_moderate: bool,
    ) -> Result<ChatMessage, RepositoryError>;
    async fn prune_expired(&self) -> Result<u64, RepositoryError>;
    async fn claim_orphan_files(
        &self,
        older_than: i64,
    ) -> Result<Vec<(Uuid, String)>, RepositoryError>;
    async fn delete_file_metadata(&self, id: Uuid) -> Result<(), RepositoryError>;
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
    async fn set_user_disabled(
        &self,
        actor: Uuid,
        user: Uuid,
        disabled: bool,
    ) -> Result<(), RepositoryError>;
    async fn update_username(&self, user: Uuid, username: &str) -> Result<(), RepositoryError>;
    async fn assign_role(&self, actor: Uuid, user: Uuid, role: &str)
    -> Result<(), RepositoryError>;
}

pub(crate) struct PostgresRepository {
    pub(crate) pool: PgPool,
}

impl PostgresRepository {
    pub(crate) async fn connect(database_url: &str) -> Result<Arc<Self>, RepositoryError> {
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

    pub(crate) async fn seed_authorization(&self) -> Result<(), RepositoryError> {
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

    pub(crate) async fn seed_test_accounts(&self) -> Result<(), RepositoryError> {
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

    async fn list_channels(&self, user_id: Uuid) -> Result<Vec<String>, RepositoryError> {
        let rows = sqlx::query("SELECT c.name FROM channels c WHERE c.deleted_at IS NULL AND c.archived_at IS NULL AND c.kind = 'public' AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $2) AND (b.channel_id IS NULL OR b.channel_id=c.id)) ORDER BY (c.name = 'main') DESC, c.name")
            .bind(user_id)
            .bind(now_millis() as i64)
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

    async fn save_message(
        &self,
        message: &ChatMessage,
        owner_session: Uuid,
        owner_user: Uuid,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query("INSERT INTO messages (id, channel_id, username, text, created_at, edited, owner_session, owner_user_id, root_message_id, client_id, metadata, mentions) SELECT $1, id, $2, $3, $4, $5, $6, $7, $9, $10, $11, $12 FROM channels WHERE name = $8 AND deleted_at IS NULL AND ($9::uuid IS NULL OR EXISTS (SELECT 1 FROM messages parent WHERE parent.id=$9 AND parent.channel_id=channels.id))")
            .bind(message.id)
            .bind(&message.username)
            .bind(&message.text)
            .bind(message.created_at as i64)
            .bind(message.edited)
            .bind(owner_session)
            .bind(owner_user)
            .bind(&message.channel)
            .bind(message.root_message_id)
            .bind(&message.client_id)
            .bind(&message.metadata)
            .bind(&message.mentions)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        for file_id in &message.file_ids {
            sqlx::query("INSERT INTO message_files (message_id,file_id) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                .bind(message.id)
                .bind(file_id)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn find_message_by_client_id(
        &self,
        _channel: &str,
        owner_session: Uuid,
        client_id: &str,
    ) -> Result<Option<ChatMessage>, RepositoryError> {
        let row = sqlx::query("SELECT m.id,c.name AS channel,m.username,CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id=m.channel_id WHERE c.deleted_at IS NULL AND m.owner_session=$1 AND m.client_id=$2")
            .bind(owner_session)
            .bind(client_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| ChatMessage {
            id: row.get("id"),
            channel: row.get("channel"),
            username: row.get("username"),
            text: row.get("text"),
            created_at: row.get::<i64, _>("created_at") as u64,
            edited: row.get("edited"),
            deleted: row.get("deleted"),
            root_message_id: row.get("root_message_id"),
            reply_count: row.try_get::<i64, _>("reply_count").unwrap_or_default() as u32,
            metadata: row
                .try_get("metadata")
                .unwrap_or_else(|_| serde_json::json!({})),
            mentions: row.try_get("mentions").unwrap_or_default(),
            client_id: row.get("client_id"),
            file_ids: row.try_get("file_ids").unwrap_or_default(),
        }))
    }

    async fn load_messages(
        &self,
        channel: &str,
        before: Option<(i64, Uuid)>,
        limit: i64,
    ) -> Result<Vec<ChatMessage>, RepositoryError> {
        let rows = sqlx::query("SELECT m.id, c.name AS channel, m.username, CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text, m.created_at, m.edited, m.deleted_at IS NOT NULL AS deleted, m.root_message_id, (SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count, m.metadata, m.mentions, m.client_id, COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id = m.channel_id WHERE c.name = $1 AND c.deleted_at IS NULL AND m.created_at >= $2 AND m.root_message_id IS NULL AND ($3::bigint IS NULL OR (m.created_at, m.id) < ($3, $4)) ORDER BY m.created_at DESC, m.id DESC LIMIT $5")
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
                root_message_id: row.get("root_message_id"),
                reply_count: row.try_get::<i64, _>("reply_count").unwrap_or_default() as u32,
                metadata: row
                    .try_get("metadata")
                    .unwrap_or_else(|_| serde_json::json!({})),
                mentions: row.try_get("mentions").unwrap_or_default(),
                client_id: row.get("client_id"),
                file_ids: row.try_get("file_ids").unwrap_or_default(),
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
        let row = sqlx::query("SELECT m.channel_id, c.name AS channel, m.username, m.created_at, m.edited, m.deleted_at, m.owner_session, m.root_message_id, (SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count, m.metadata, m.mentions, m.client_id, COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.id = $1 AND c.name = $2 AND c.deleted_at IS NULL FOR UPDATE")
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
            root_message_id: row.get("root_message_id"),
            reply_count: row.try_get::<i64, _>("reply_count").unwrap_or_default() as u32,
            metadata: row
                .try_get("metadata")
                .unwrap_or_else(|_| serde_json::json!({})),
            mentions: row.try_get("mentions").unwrap_or_default(),
            client_id: row.get("client_id"),
            file_ids: row.try_get("file_ids").unwrap_or_default(),
        })
    }

    async fn delete_message(
        &self,
        channel: &str,
        id: Uuid,
        owner_session: Uuid,
        can_moderate: bool,
    ) -> Result<ChatMessage, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT c.name AS channel, m.username, m.created_at, m.owner_session, m.deleted_at, m.root_message_id, (SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count, m.metadata, m.mentions, m.client_id, COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.id=$1 AND c.name=$2 AND c.deleted_at IS NULL FOR UPDATE")
            .bind(id).bind(channel).fetch_optional(&mut *transaction).await?
            .ok_or(RepositoryError::NotFound)?;
        if !can_moderate && row.get::<Uuid, _>("owner_session") != owner_session {
            return Err(RepositoryError::Forbidden);
        }
        sqlx::query(
            "UPDATE messages SET deleted_at=COALESCE(deleted_at,$1), edited=FALSE WHERE id=$2",
        )
        .bind(now_millis() as i64)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ChatMessage {
            id,
            channel: row.get("channel"),
            username: row.get("username"),
            text: String::new(),
            created_at: row.get::<i64, _>("created_at") as u64,
            edited: false,
            deleted: true,
            root_message_id: row.try_get("root_message_id").ok(),
            reply_count: row.try_get::<i64, _>("reply_count").unwrap_or_default() as u32,
            metadata: row
                .try_get("metadata")
                .unwrap_or_else(|_| serde_json::json!({})),
            mentions: row.try_get("mentions").unwrap_or_default(),
            client_id: row.get("client_id"),
            file_ids: row.try_get("file_ids").unwrap_or_default(),
        })
    }

    async fn prune_expired(&self) -> Result<u64, RepositoryError> {
        let result = sqlx::query("DELETE FROM messages m USING channels c WHERE m.channel_id=c.id AND m.created_at < $1 - (GREATEST(c.retention_days,1)::BIGINT * 86400000)")
            .bind(now_millis() as i64)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn claim_orphan_files(
        &self,
        older_than: i64,
    ) -> Result<Vec<(Uuid, String)>, RepositoryError> {
        let rows = sqlx::query(
            "UPDATE files SET deleted_at=COALESCE(deleted_at,$1)
             WHERE (deleted_at IS NOT NULL OR created_at < $2)
               AND NOT EXISTS (SELECT 1 FROM message_files mf WHERE mf.file_id=files.id)
             RETURNING id,storage_key",
        )
        .bind(now_millis() as i64)
        .bind(older_than)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("id"), row.get("storage_key")))
            .collect())
    }

    async fn delete_file_metadata(&self, id: Uuid) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM files WHERE id=$1 AND deleted_at IS NOT NULL")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        let mut tx = self.pool.begin().await?;
        let now = now_millis() as i64;
        let result = sqlx::query(
            "UPDATE users SET username=$1,updated_at=$2,role_version=role_version+1 WHERE id=$3 AND deleted_at IS NULL",
        )
        .bind(username)
        .bind(now)
        .bind(user)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'user.username_changed','user',$2,jsonb_build_object('username',$3),$4)")
            .bind(Uuid::now_v7()).bind(user).bind(username).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
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
}

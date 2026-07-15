use super::*;

pub(crate) async fn create_invite_link(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(request): Json<InviteLinkRequest>,
) -> Result<Json<InviteLinkResponse>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    ensure_not_globally_banned(&state.database, session.user.id).await?;
    let channel = sqlx::query(
        "SELECT id,kind,owner_user_id FROM channels WHERE name=$1 AND deleted_at IS NULL",
    )
    .bind(&name)
    .fetch_optional(&state.database)
    .await?
    .ok_or(RepositoryError::NotFound)?;
    ensure_channel_access(&state.database, &name, session.user.id).await?;
    if channel.get::<String, _>("kind") == "private"
        && channel.get::<Option<Uuid>, _>("owner_user_id") != Some(session.user.id)
        && !session
            .user
            .permissions
            .iter()
            .any(|permission| permission == "chat:moderate")
    {
        return Err(AppError::forbidden(
            "only the channel owner can create invite links",
        ));
    }
    let max_uses = request.max_uses.unwrap_or(0);
    if !(0..=100_000).contains(&max_uses) {
        return Err(AppError::bad_request(
            "max_uses must be between 0 and 100000",
        ));
    }
    if let Some(expires_at) = request.expires_at
        && expires_at <= now_millis() as i64
    {
        return Err(AppError::bad_request(
            "invite link expiry must be in the future",
        ));
    }
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let token = hex::encode(bytes);
    let token_hash = invite_token_hash(&token);
    sqlx::query("INSERT INTO channel_invite_links (id,channel_id,token_hash,created_by,expires_at,max_uses,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .bind(Uuid::now_v7()).bind(channel.get::<Uuid, _>("id")).bind(token_hash).bind(session.user.id).bind(request.expires_at).bind(max_uses).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(Json(InviteLinkResponse {
        token,
        expires_at: request.expires_at,
        max_uses,
    }))
}

pub(crate) async fn accept_invite_link(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(token): axum::extract::Path<String>,
) -> Result<Json<Channel>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::bad_request("invalid invite link"));
    }
    let hash = invite_token_hash(&token);
    let mut tx = state.database.begin().await?;
    let row = sqlx::query("SELECT c.id,c.name,l.expires_at,l.max_uses,l.uses FROM channel_invite_links l JOIN channels c ON c.id=l.channel_id WHERE l.token_hash=$1 AND c.deleted_at IS NULL FOR UPDATE")
        .bind(&hash).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::NotFound)?;
    let now = now_millis() as i64;
    if row
        .get::<Option<i64>, _>("expires_at")
        .is_some_and(|expires| expires <= now)
        || (row.get::<i32, _>("max_uses") > 0
            && row.get::<i32, _>("uses") >= row.get::<i32, _>("max_uses"))
    {
        return Err(AppError::bad_request("invite link is expired or exhausted"));
    }
    let channel_id: Uuid = row.get("id");
    let banned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM user_bans WHERE user_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2) AND (channel_id IS NULL OR channel_id=$3))")
        .bind(session.user.id).bind(now).bind(channel_id).fetch_one(&state.database).await?;
    if banned {
        return Err(AppError::forbidden("conversation access denied"));
    }
    sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',NULL,$3) ON CONFLICT DO NOTHING")
        .bind(channel_id).bind(session.user.id).bind(now).execute(&mut *tx).await?;
    sqlx::query("UPDATE channel_invite_links SET uses=uses+1 WHERE token_hash=$1")
        .bind(hash)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    publish_system_message(
        &state,
        row.get("name"),
        format!("{} joined the conversation", session.user.username),
    )
    .await?;
    Ok(Json(Channel {
        name: row.get("name"),
    }))
}

pub(crate) async fn list_visible_conversations(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<ConversationSummary>, AppError> {
    let rows = sqlx::query("SELECT c.id,c.name,c.kind,c.owner_user_id,CASE WHEN c.kind='direct' THEN COALESCE(peer.username,c.name) ELSE c.name END AS display_name,peer.id AS peer_user_id,peer.username AS peer_username,recent.last_message_at FROM channels c LEFT JOIN channel_members mine ON mine.channel_id=c.id AND mine.user_id=$1 LEFT JOIN channel_members other ON other.channel_id=c.id AND other.user_id<>$1 AND c.kind='direct' LEFT JOIN users peer ON peer.id=other.user_id LEFT JOIN (SELECT channel_id,MAX(created_at) AS last_message_at FROM messages WHERE deleted_at IS NULL GROUP BY channel_id) recent ON recent.channel_id=c.id WHERE c.deleted_at IS NULL AND c.archived_at IS NULL AND (c.kind='public' OR mine.user_id IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $2) AND (b.channel_id IS NULL OR b.channel_id=c.id)) ORDER BY recent.last_message_at DESC NULLS LAST,(c.kind='public' AND c.name='main') DESC,c.kind,c.name")
        .bind(user_id).bind(now_millis() as i64).fetch_all(pool).await?;
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
            last_message_at: row.get("last_message_at"),
        })
        .collect())
}

pub(crate) async fn channel_members(
    pool: &PgPool,
    name: &str,
) -> Result<Vec<ChannelMember>, AppError> {
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

pub(crate) async fn create_system_message(
    pool: &PgPool,
    channel: &str,
    text: impl Into<String>,
) -> Result<Option<ChatMessage>, AppError> {
    let text = text.into();
    let id = Uuid::now_v7();
    let created_at = now_millis();
    let result = sqlx::query("INSERT INTO messages (id,channel_id,username,text,created_at,edited,owner_session,owner_user_id,root_message_id,client_id,metadata,mentions) SELECT $1,id,'system',$2,$3,FALSE,$4,NULL,NULL,NULL,jsonb_build_object('kind','system'),'{}' FROM channels WHERE name=$5 AND deleted_at IS NULL")
        .bind(id).bind(&text).bind(created_at as i64).bind(Uuid::nil()).bind(channel).execute(pool).await?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    Ok(Some(ChatMessage {
        id,
        channel: channel.to_string(),
        username: "system".to_string(),
        text,
        created_at,
        edited: false,
        deleted: false,
        root_message_id: None,
        reply_count: 0,
        metadata: serde_json::json!({"kind": "system"}),
        mentions: Vec::new(),
        client_id: None,
        file_ids: Vec::new(),
    }))
}

pub(crate) async fn publish_system_message(
    state: &AppState,
    channel: &str,
    text: impl Into<String>,
) -> Result<(), AppError> {
    if let Some(message) = create_system_message(&state.database, channel, text).await? {
        let _ = broadcast(&state.valkey, channel, &ServerEvent::Message { message }).await;
    }
    Ok(())
}

pub(crate) async fn ensure_channel_access(
    pool: &PgPool,
    name: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    let allowed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM channels c LEFT JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=$2 WHERE c.name=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR cm.user_id IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$2 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $3) AND (b.channel_id IS NULL OR b.channel_id=c.id)))")
        .bind(name).bind(user_id).bind(now_millis() as i64).fetch_one(pool).await?;
    if allowed {
        Ok(())
    } else {
        Err(AppError::forbidden("conversation access denied"))
    }
}

pub(crate) async fn ensure_not_globally_banned(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<(), AppError> {
    let banned: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM user_bans WHERE user_id=$1 AND channel_id IS NULL AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2))",
    )
    .bind(user_id)
    .bind(now_millis() as i64)
    .fetch_one(pool)
    .await?;
    if banned {
        return Err(AppError::forbidden("conversation access denied"));
    }
    Ok(())
}

pub(crate) async fn ensure_channel_posting_access(
    pool: &PgPool,
    name: &str,
    user_id: Uuid,
    permissions: &[String],
) -> Result<(), AppError> {
    let row = sqlx::query(
        "SELECT archived_at,posting_restricted FROM channels WHERE name=$1 AND deleted_at IS NULL",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Err(RepositoryError::NotFound.into());
    };
    let is_moderator = permissions
        .iter()
        .any(|permission| permission == "chat:moderate");
    if row.get::<Option<i64>, _>("archived_at").is_some() && !is_moderator {
        return Err(AppError::forbidden("archived channels are read-only"));
    }
    if !row.get::<bool, _>("posting_restricted") || is_moderator {
        return Ok(());
    }
    let can_post: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM channel_members cm JOIN channels c ON c.id=cm.channel_id WHERE c.name=$1 AND cm.user_id=$2 AND cm.membership_role IN ('owner','moderator'))").bind(name).bind(user_id).fetch_one(pool).await?;
    if can_post {
        Ok(())
    } else {
        Err(AppError::forbidden("posting is restricted in this channel"))
    }
}

pub(crate) async fn owned_private_channel(
    pool: &PgPool,
    name: &str,
    user_id: Uuid,
) -> Result<Uuid, AppError> {
    sqlx::query_scalar("SELECT id FROM channels WHERE name=$1 AND kind='private' AND owner_user_id=$2 AND deleted_at IS NULL").bind(name).bind(user_id).fetch_optional(pool).await?.ok_or_else(|| AppError::forbidden("only the private channel owner can manage members"))
}

pub(crate) async fn channel_moderator_manager(
    pool: &PgPool,
    name: &str,
    user: &AuthUser,
) -> Result<Uuid, AppError> {
    if user
        .permissions
        .iter()
        .any(|permission| permission == "chat:moderate")
    {
        return sqlx::query_scalar("SELECT id FROM channels WHERE name=$1 AND deleted_at IS NULL")
            .bind(name)
            .fetch_optional(pool)
            .await?
            .ok_or(RepositoryError::NotFound.into());
    }
    owned_private_channel(pool, name, user.id).await
}

pub(crate) async fn create_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<CreateChannelResponse>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    ensure_not_globally_banned(&state.database, session.user.id).await?;
    let name = normalize_channel_name(&request.name)?;
    let mut tx = state.database.begin().await?;
    let channel_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO channels (id,name,created_at) VALUES ($1,$2,$3)
         ON CONFLICT (name) DO UPDATE SET deleted_at=NULL
         WHERE channels.deleted_at IS NOT NULL
         RETURNING id",
    )
    .bind(Uuid::now_v7())
    .bind(&name)
    .bind(now_millis() as i64)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::bad_request("channel already exists"))?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.created','channel',$3,$4)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    publish_system_message(
        &state,
        &name,
        format!("{} created this channel", session.user.username),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(CreateChannelResponse { name })))
}

pub(crate) fn normalize_channel_name(raw: &str) -> Result<String, AppError> {
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

pub(crate) fn normalize_draft_body(raw: &str) -> Result<String, AppError> {
    let body = raw.trim().to_string();
    if body.len() > 2000 {
        return Err(AppError::bad_request(
            "draft must be at most 2000 characters",
        ));
    }
    Ok(body)
}

pub(crate) fn invite_token_hash(token: &str) -> String {
    hex::encode(sha2::Sha256::digest(token.as_bytes()))
}

pub(crate) async fn list_notifications(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<Vec<NotificationView>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let rows = sqlx::query("SELECT id,kind,message_id,channel_id,body,created_at,read_at FROM notifications WHERE user_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2")
        .bind(session.user.id).bind(limit).fetch_all(&state.database).await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| NotificationView {
                id: row.get("id"),
                kind: row.get("kind"),
                message_id: row.get("message_id"),
                channel_id: row.get("channel_id"),
                body: row.get("body"),
                created_at: row.get("created_at"),
                read_at: row.get("read_at"),
            })
            .collect(),
    ))
}

pub(crate) async fn mark_notification_read(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    sqlx::query("UPDATE notifications SET read_at=COALESCE(read_at,$1) WHERE id=$2 AND user_id=$3")
        .bind(now_millis() as i64)
        .bind(id)
        .bind(session.user.id)
        .execute(&state.database)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn notification_preferences(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<NotificationPreferencesView>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let row = sqlx::query("INSERT INTO notification_preferences (user_id,updated_at) VALUES ($1,$2) ON CONFLICT (user_id) DO NOTHING RETURNING mentions,direct_messages,channel_messages,email_enabled,browser_push_enabled")
        .bind(session.user.id).bind(now_millis() as i64).fetch_optional(&state.database).await?;
    let row = match row {
        Some(row) => row,
        None => sqlx::query("SELECT mentions,direct_messages,channel_messages,email_enabled,browser_push_enabled FROM notification_preferences WHERE user_id=$1")
            .bind(session.user.id).fetch_one(&state.database).await?,
    };
    Ok(Json(NotificationPreferencesView {
        mentions: row.get("mentions"),
        direct_messages: row.get("direct_messages"),
        channel_messages: row.get("channel_messages"),
        email_enabled: row.get("email_enabled"),
        browser_push_enabled: row.get("browser_push_enabled"),
    }))
}

pub(crate) async fn notification_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let _ = load_session(&headers, &state.valkey).await?;
    Ok(Json(serde_json::json!({
        "vapid_public_key": std::env::var("VAPID_PUBLIC_KEY").ok()
    })))
}

pub(crate) async fn update_notification_preferences(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<NotificationPreferencesUpdate>,
) -> Result<Json<NotificationPreferencesView>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let row = sqlx::query("INSERT INTO notification_preferences (user_id,mentions,direct_messages,channel_messages,email_enabled,browser_push_enabled,updated_at) VALUES ($1,COALESCE($2,TRUE),COALESCE($3,TRUE),COALESCE($4,FALSE),COALESCE($5,FALSE),COALESCE($6,FALSE),$7) ON CONFLICT (user_id) DO UPDATE SET mentions=COALESCE($2,notification_preferences.mentions),direct_messages=COALESCE($3,notification_preferences.direct_messages),channel_messages=COALESCE($4,notification_preferences.channel_messages),email_enabled=COALESCE($5,notification_preferences.email_enabled),browser_push_enabled=COALESCE($6,notification_preferences.browser_push_enabled),updated_at=$7 RETURNING mentions,direct_messages,channel_messages,email_enabled,browser_push_enabled")
        .bind(session.user.id).bind(request.mentions).bind(request.direct_messages).bind(request.channel_messages).bind(request.email_enabled).bind(request.browser_push_enabled).bind(now_millis() as i64).fetch_one(&state.database).await?;
    Ok(Json(NotificationPreferencesView {
        mentions: row.get("mentions"),
        direct_messages: row.get("direct_messages"),
        channel_messages: row.get("channel_messages"),
        email_enabled: row.get("email_enabled"),
        browser_push_enabled: row.get("browser_push_enabled"),
    }))
}

pub(crate) async fn notification_subscriptions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<NotificationSubscriptionView>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let rows = sqlx::query("SELECT id,endpoint,p256dh,auth FROM notification_subscriptions WHERE user_id=$1 ORDER BY updated_at DESC,id DESC")
        .bind(session.user.id)
        .fetch_all(&state.database)
        .await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| NotificationSubscriptionView {
                id: row.get("id"),
                endpoint: row.get("endpoint"),
                p256dh: row.get("p256dh"),
                auth: row.get("auth"),
            })
            .collect(),
    ))
}

pub(crate) async fn save_notification_subscription(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<NotificationSubscriptionRequest>,
) -> Result<Json<NotificationSubscriptionView>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let endpoint = request.endpoint.trim().to_string();
    let parsed = endpoint
        .parse::<reqwest::Url>()
        .map_err(|_| AppError::bad_request("invalid push endpoint"))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || endpoint.len() > 2048
    {
        return Err(AppError::bad_request("push endpoint must be HTTPS"));
    }
    let p256dh = request.p256dh.trim().to_string();
    let auth = request.auth.trim().to_string();
    if !(16..=512).contains(&p256dh.len()) || !(8..=256).contains(&auth.len()) {
        return Err(AppError::bad_request("invalid push subscription keys"));
    }
    let now = state.clock.now_millis() as i64;
    let row = sqlx::query("INSERT INTO notification_subscriptions (id,user_id,endpoint,p256dh,auth,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$6) ON CONFLICT (user_id,endpoint) DO UPDATE SET p256dh=EXCLUDED.p256dh,auth=EXCLUDED.auth,updated_at=EXCLUDED.updated_at RETURNING id,endpoint,p256dh,auth")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(endpoint).bind(p256dh).bind(auth).bind(now)
        .fetch_one(&state.database).await?;
    Ok(Json(NotificationSubscriptionView {
        id: row.get("id"),
        endpoint: row.get("endpoint"),
        p256dh: row.get("p256dh"),
        auth: row.get("auth"),
    }))
}

pub(crate) async fn delete_notification_subscription(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    sqlx::query("DELETE FROM notification_subscriptions WHERE id=$1 AND user_id=$2")
        .bind(id)
        .bind(session.user.id)
        .execute(&state.database)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn profile(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ProfileView>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let row = sqlx::query("SELECT id,username,display_name,CASE WHEN status_expires_at IS NULL OR status_expires_at > $2 THEN custom_status ELSE '' END AS custom_status,CASE WHEN status_expires_at IS NULL OR status_expires_at > $2 THEN status_expires_at ELSE NULL END AS status_expires_at FROM users WHERE id=$1 AND deleted_at IS NULL")
        .bind(session.user.id)
        .bind(state.clock.now_millis() as i64)
        .fetch_one(&state.database)
        .await?;
    Ok(Json(ProfileView {
        id: row.get("id"),
        username: row.get("username"),
        display_name: row.get("display_name"),
        custom_status: row.get("custom_status"),
        status_expires_at: row.get("status_expires_at"),
    }))
}

pub(crate) async fn update_profile(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ProfileUpdateRequest>,
) -> Result<Json<ProfileView>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let display_name = request
        .display_name
        .unwrap_or_default()
        .trim()
        .chars()
        .take(80)
        .collect::<String>();
    let custom_status = request
        .custom_status
        .unwrap_or_default()
        .trim()
        .chars()
        .take(160)
        .collect::<String>();
    if request
        .status_expires_at
        .is_some_and(|expires_at| expires_at <= state.clock.now_millis() as i64)
    {
        return Err(AppError::bad_request("status expiry must be in the future"));
    }
    sqlx::query("UPDATE users SET display_name=$1,custom_status=$2,status_expires_at=$3,updated_at=$4 WHERE id=$5 AND deleted_at IS NULL")
        .bind(&display_name).bind(&custom_status).bind(request.status_expires_at).bind(now_millis() as i64).bind(session.user.id).execute(&state.database).await?;
    Ok(Json(ProfileView {
        id: session.user.id,
        username: session.user.username,
        display_name,
        custom_status,
        status_expires_at: request.status_expires_at,
    }))
}

pub(crate) async fn create_message_notifications(
    state: &AppState,
    actor: &AuthUser,
    message: &ChatMessage,
) -> Result<(), AppError> {
    create_message_notifications_with_clock(&state.database, state.clock.as_ref(), actor, message)
        .await
}

async fn create_message_notifications_with_clock(
    database: &PgPool,
    clock: &dyn Clock,
    actor: &AuthUser,
    message: &ChatMessage,
) -> Result<(), AppError> {
    let mention_usernames = extract_mentioned_usernames(&message.text, &actor.username);
    let scope = mention_scope(&message.text);
    let channel_mention = scope == Some("channel");
    let here_ids = message
        .metadata
        .get("online_user_ids")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().and_then(|id| Uuid::parse_str(id).ok()))
        .collect::<Vec<_>>();
    let here_mention = scope == Some("here");
    let rows = sqlx::query("SELECT c.id AS channel_id,c.kind,u.id,u.username,u.email,COALESCE(np.mentions,TRUE) AS mentions,COALESCE(np.direct_messages,TRUE) AS direct_messages,COALESCE(np.channel_messages,FALSE) AS channel_messages,COALESCE(np.email_enabled,FALSE) AS email_enabled,COALESCE(np.browser_push_enabled,FALSE) AS browser_enabled FROM channels c JOIN users u ON u.id<>$2 LEFT JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=u.id LEFT JOIN notification_preferences np ON np.user_id=u.id WHERE c.name=$1 AND c.deleted_at IS NULL AND u.disabled_at IS NULL AND u.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=u.id AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $3) AND (b.channel_id IS NULL OR b.channel_id=c.id)) AND (((((lower(u.username)=ANY($4::text[])) OR $5 OR (u.id=ANY($6::uuid[]))) AND COALESCE(np.mentions,TRUE) AND (c.kind='public' OR cm.user_id IS NOT NULL)) OR (c.kind='direct' AND cm.user_id IS NOT NULL AND COALESCE(np.direct_messages,TRUE)) OR (c.kind='private' AND cm.user_id IS NOT NULL AND COALESCE(np.channel_messages,FALSE)) OR (c.kind='public' AND COALESCE(np.channel_messages,FALSE))))")
        .bind(&message.channel)
        .bind(actor.id)
        .bind(clock.now_millis() as i64)
        .bind(&mention_usernames)
        .bind(channel_mention)
        .bind(&here_ids)
        .fetch_all(database)
        .await?;
    for row in rows {
        let username = row.get::<String, _>("username").to_lowercase();
        let is_mention = row.get::<bool, _>("mentions")
            && (channel_mention
                || (here_mention && here_ids.contains(&row.get::<Uuid, _>("id")))
                || mention_usernames
                    .iter()
                    .any(|candidate| candidate == &username));
        let kind = if is_mention {
            "mention"
        } else if row.get::<String, _>("kind") == "direct" {
            "direct_message"
        } else {
            "channel_message"
        };
        let body = match kind {
            "mention" => format!("{} mentioned you in #{}", actor.username, message.channel),
            "direct_message" => format!("{} sent you a direct message", actor.username),
            _ => format!("{} posted in #{}", actor.username, message.channel),
        };
        let channel_id: Uuid = row.get("channel_id");
        let user_id: Uuid = row.get("id");
        let notification_id: Uuid = sqlx::query_scalar("INSERT INTO notifications (id,user_id,actor_user_id,kind,message_id,channel_id,body,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (user_id,message_id,kind) DO UPDATE SET body=EXCLUDED.body RETURNING id")
            .bind(Uuid::now_v7()).bind(user_id).bind(actor.id).bind(kind).bind(message.id).bind(channel_id).bind(&body).bind(message.created_at as i64).fetch_one(database).await?;
        if row.get::<bool, _>("email_enabled") {
            sqlx::query("INSERT INTO notification_deliveries (id,notification_id,user_id,channel,email,kind,body,next_attempt_at) VALUES ($1,$2,$3,'email',$4,$5,$6,$7) ON CONFLICT (notification_id,channel) DO NOTHING")
                .bind(Uuid::now_v7()).bind(notification_id).bind(user_id)
                .bind(row.get::<String, _>("email")).bind(kind).bind(&body)
                .bind(clock.now_millis() as i64).execute(database).await?;
        }
        if row.get::<bool, _>("browser_enabled") {
            sqlx::query("INSERT INTO notification_deliveries (id,notification_id,user_id,channel,email,kind,body,next_attempt_at) VALUES ($1,$2,$3,'browser',$4,$5,$6,$7) ON CONFLICT (notification_id,channel) DO NOTHING")
                .bind(Uuid::now_v7()).bind(notification_id).bind(user_id)
                .bind(row.get::<String, _>("email")).bind(kind).bind(&body)
                .bind(clock.now_millis() as i64).execute(database).await?;
        }
    }
    Ok(())
}

pub(crate) async fn retry_message_notifications(
    database: &PgPool,
    actor_id: Uuid,
    message_id: Uuid,
) -> Result<(), AppError> {
    let Some(row) = sqlx::query("SELECT m.id,c.name AS channel,m.username,m.text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids,u.id AS actor_id,u.email,u.username AS actor_username FROM messages m JOIN channels c ON c.id=m.channel_id JOIN users u ON u.id=$1 WHERE m.id=$2")
        .bind(actor_id)
        .bind(message_id)
        .fetch_optional(database)
        .await? else {
        // Retention or moderation may remove the message before a retry is
        // claimed. There is then no notification work left to perform.
        return Ok(());
    };
    let actor = AuthUser {
        id: row.get("actor_id"),
        email: row.get("email"),
        username: row.get("actor_username"),
        roles: Vec::new(),
        permissions: Vec::new(),
    };
    let message = ChatMessage {
        id: row.get("id"),
        channel: row.get("channel"),
        username: row.get("username"),
        text: row.get("text"),
        created_at: row.get::<i64, _>("created_at") as u64,
        edited: row.get("edited"),
        deleted: row.get("deleted"),
        root_message_id: row.get("root_message_id"),
        reply_count: 0,
        metadata: row
            .try_get("metadata")
            .unwrap_or_else(|_| serde_json::json!({})),
        mentions: row.try_get("mentions").unwrap_or_default(),
        client_id: row.get("client_id"),
        file_ids: row.try_get("file_ids").unwrap_or_default(),
    };
    create_message_notifications_with_clock(database, &SystemClock, &actor, &message).await
}

pub(crate) fn extract_mentioned_usernames(text: &str, actor_username: &str) -> Vec<String> {
    let actor_username = actor_username.to_lowercase();
    let mut usernames = std::collections::BTreeSet::new();
    for token in text.split_whitespace() {
        if let Some(username) = token.strip_prefix('@') {
            let username = username.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != '_' && character != '-'
            });
            if !username.is_empty()
                && username.len() <= 64
                && username != "channel"
                && username != "here"
                && username.to_lowercase() != actor_username
            {
                usernames.insert(username.to_lowercase());
            }
        }
    }
    usernames.into_iter().collect()
}

pub(crate) fn mention_scope(text: &str) -> Option<&'static str> {
    text.split_whitespace().find_map(|token| {
        let username = token
            .strip_prefix('@')?
            .trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != '_' && character != '-'
            })
            .to_lowercase();
        match username.as_str() {
            "channel" => Some("channel"),
            "here" => Some("here"),
            _ => None,
        }
    })
}

pub(crate) async fn create_private_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ConversationSummary>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    ensure_not_globally_banned(&state.database, session.user.id).await?;
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
    publish_system_message(
        &state,
        &name,
        format!("{} created this private channel", session.user.username),
    )
    .await?;
    let summary = list_visible_conversations(&state.database, session.user.id)
        .await?
        .into_iter()
        .find(|item| item.id == id)
        .ok_or(RepositoryError::NotFound)?;
    Ok((StatusCode::CREATED, Json(summary)))
}

pub(crate) async fn open_direct_conversation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<DirectConversationRequest>,
) -> Result<Json<ConversationSummary>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    ensure_not_globally_banned(&state.database, session.user.id).await?;
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
    let inserted = sqlx::query("INSERT INTO channels (id,name,kind,direct_key,created_at) VALUES ($1,$2,'direct',$3,$4) ON CONFLICT (direct_key) WHERE kind='direct' AND deleted_at IS NULL DO NOTHING RETURNING id")
        .bind(id).bind(&name).bind(&direct_key).bind(now).fetch_optional(&mut *tx).await?;
    let (channel_id, created) = if let Some(row) = inserted {
        (row.get::<Uuid, _>("id"), true)
    } else {
        (sqlx::query_scalar("SELECT id FROM channels WHERE direct_key=$1 AND kind='direct' AND deleted_at IS NULL")
            .bind(&direct_key).fetch_one(&mut *tx).await?, false)
    };
    for member in [first, second] {
        sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
            .bind(channel_id).bind(member).bind(session.user.id).bind(now).execute(&mut *tx).await?;
    }
    if created {
        sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.direct_created','channel',$3,jsonb_build_object('user_id',$4),$5)")
            .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(request.user_id).bind(now).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    let summary = list_visible_conversations(&state.database, session.user.id)
        .await?
        .into_iter()
        .find(|item| item.id == channel_id)
        .ok_or(RepositoryError::NotFound)?;
    Ok(Json(summary))
}

pub(crate) async fn list_channel_members(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<Json<Vec<ChannelMember>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    ensure_channel_access(&state.database, &name, session.user.id).await?;
    Ok(Json(channel_members(&state.database, &name).await?))
}

pub(crate) async fn invite_channel_member(
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
    let result = sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
        .bind(channel_id).bind(request.user_id).bind(session.user.id).bind(now_millis() as i64).execute(&state.database).await?;
    if result.rows_affected() > 0 {
        let target: String = sqlx::query_scalar("SELECT username FROM users WHERE id=$1")
            .bind(request.user_id)
            .fetch_one(&state.database)
            .await?;
        publish_system_message(
            &state,
            &name,
            format!("{} invited {}", session.user.username, target),
        )
        .await?;
        let _ = broadcast(
            &state.valkey,
            &name,
            &ServerEvent::Members {
                channel: name.clone(),
                members: channel_members(&state.database, &name).await?,
            },
        )
        .await;
        sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_added','channel',$3,jsonb_build_object('user_id',$4),$5)")
            .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(request.user_id).bind(now_millis() as i64).execute(&state.database).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn remove_channel_member(
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
    let target: String = sqlx::query_scalar("SELECT username FROM users WHERE id=$1")
        .bind(user_id)
        .fetch_optional(&state.database)
        .await?
        .unwrap_or_else(|| "a member".to_string());
    publish_system_message(
        &state,
        &name,
        format!("{} removed {}", session.user.username, target),
    )
    .await?;
    let _ = broadcast(
        &state.valkey,
        &name,
        &ServerEvent::Members {
            channel: name.clone(),
            members: channel_members(&state.database, &name).await?,
        },
    )
    .await;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_removed','channel',$3,jsonb_build_object('user_id',$4),$5)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(user_id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn leave_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let row = sqlx::query("SELECT c.id,c.kind,c.owner_user_id,cm.membership_role FROM channels c JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=$1 WHERE c.name=$2 AND c.deleted_at IS NULL")
        .bind(session.user.id)
        .bind(&name)
        .fetch_optional(&state.database)
        .await?
        .ok_or(RepositoryError::NotFound)?;
    if row.get::<String, _>("kind") == "public" {
        return Err(AppError::bad_request(
            "public channels do not require leaving",
        ));
    }
    if row.get::<Option<Uuid>, _>("owner_user_id") == Some(session.user.id)
        || row.get::<String, _>("membership_role") == "owner"
    {
        return Err(AppError::bad_request(
            "the channel owner must transfer ownership or delete the channel",
        ));
    }
    sqlx::query("DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2")
        .bind(row.get::<Uuid, _>("id"))
        .bind(session.user.id)
        .execute(&state.database)
        .await?;
    publish_system_message(
        &state,
        &name,
        format!("{} left the conversation", session.user.username),
    )
    .await?;
    let _ = broadcast(
        &state.valkey,
        &name,
        &ServerEvent::Members {
            channel: name.clone(),
            members: channel_members(&state.database, &name).await?,
        },
    )
    .await;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_left','channel',$3,jsonb_build_object('user_id',$4),$5)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(row.get::<Uuid, _>("id")).bind(session.user.id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn promote_channel_moderator(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((name, user_id)): axum::extract::Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let channel_id = channel_moderator_manager(&state.database, &name, &session.user).await?;
    let result = sqlx::query("UPDATE channel_members SET membership_role='moderator' WHERE channel_id=$1 AND user_id=$2 AND membership_role='member'")
        .bind(channel_id)
        .bind(user_id)
        .execute(&state.database)
        .await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.moderator_promoted','channel',$3,jsonb_build_object('user_id',$4),$5)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(user_id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn demote_channel_moderator(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((name, user_id)): axum::extract::Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let channel_id = channel_moderator_manager(&state.database, &name, &session.user).await?;
    let result = sqlx::query("UPDATE channel_members SET membership_role='member' WHERE channel_id=$1 AND user_id=$2 AND membership_role='moderator'")
        .bind(channel_id)
        .bind(user_id)
        .execute(&state.database)
        .await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.moderator_demoted','channel',$3,jsonb_build_object('user_id',$4),$5)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(channel_id).bind(user_id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn favorite_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    ensure_channel_access(&state.database, &name, session.user.id).await?;
    sqlx::query("INSERT INTO channel_favorites (user_id,channel_id,created_at) SELECT $1,id,$2 FROM channels WHERE name=$3 AND deleted_at IS NULL ON CONFLICT DO NOTHING")
        .bind(session.user.id).bind(now_millis() as i64).bind(&name).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn unfavorite_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    sqlx::query("DELETE FROM channel_favorites f USING channels c WHERE f.channel_id=c.id AND f.user_id=$1 AND c.name=$2")
        .bind(session.user.id).bind(&name).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_favorite_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let names = sqlx::query_scalar("SELECT c.name FROM channel_favorites f JOIN channels c ON c.id=f.channel_id WHERE f.user_id=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel_id=c.id AND cm.user_id=$1)) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $2) AND (b.channel_id IS NULL OR b.channel_id=c.id)) ORDER BY f.created_at,c.name")
        .bind(session.user.id).bind(now_millis() as i64).fetch_all(&state.database).await?;
    Ok(Json(names))
}

pub(crate) async fn get_draft(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(channel): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let channel = normalize_channel_name(&channel)?;
    ensure_channel_access(&state.database, &channel, session.user.id).await?;
    let body: Option<String> = sqlx::query_scalar(
        "SELECT d.body FROM channel_drafts d JOIN channels c ON c.id=d.channel_id WHERE d.user_id=$1 AND c.name=$2 AND c.deleted_at IS NULL",
    )
    .bind(session.user.id)
    .bind(&channel)
    .fetch_optional(&state.database)
    .await?;
    Ok(Json(
        serde_json::json!({"channel": channel, "body": body.unwrap_or_default()}),
    ))
}

pub(crate) async fn update_draft(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(channel): axum::extract::Path<String>,
    Json(request): Json<DraftUpdateRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let channel = normalize_channel_name(&channel)?;
    ensure_channel_access(&state.database, &channel, session.user.id).await?;
    let body = normalize_draft_body(&request.body)?;
    let now = now_millis() as i64;
    sqlx::query(
        "INSERT INTO channel_drafts (user_id,channel_id,body,updated_at) SELECT $1,id,$2,$3 FROM channels WHERE name=$4 AND deleted_at IS NULL ON CONFLICT (user_id,channel_id) DO UPDATE SET body=EXCLUDED.body,updated_at=EXCLUDED.updated_at",
    )
    .bind(session.user.id)
    .bind(&body)
    .bind(now)
    .bind(&channel)
    .execute(&state.database)
    .await?;
    Ok(Json(
        serde_json::json!({"channel": channel, "body": body, "updated_at": now}),
    ))
}

pub(crate) async fn delete_draft(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(channel): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let channel = normalize_channel_name(&channel)?;
    ensure_channel_access(&state.database, &channel, session.user.id).await?;
    sqlx::query("DELETE FROM channel_drafts d USING channels c WHERE d.channel_id=c.id AND d.user_id=$1 AND c.name=$2")
        .bind(session.user.id)
        .bind(&channel)
        .execute(&state.database)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::{
        extract_mentioned_usernames, invite_token_hash, mention_scope, normalize_channel_name,
        normalize_draft_body,
    };

    #[test]
    fn mention_extraction_normalizes_deduplicates_and_excludes_author() {
        assert_eq!(
            extract_mentioned_usernames("Hello @Alice, @alice and @author!", "author"),
            vec!["alice"]
        );
    }

    #[test]
    fn mention_extraction_ignores_invalid_or_oversized_tokens() {
        let oversized = "a".repeat(65);
        assert!(extract_mentioned_usernames(&format!("@ -@ @{}", oversized), "author").is_empty());
    }

    #[test]
    fn special_mention_scopes_are_detected_without_becoming_usernames() {
        assert_eq!(mention_scope("hello @channel"), Some("channel"));
        assert_eq!(mention_scope("hello @here!"), Some("here"));
        assert_eq!(mention_scope("hello @Alice"), None);
        assert_eq!(
            extract_mentioned_usernames("hello @channel @here @Alice", "author"),
            vec!["alice"]
        );
    }

    #[test]
    fn channel_names_are_normalized_and_bounded() {
        assert_eq!(
            normalize_channel_name("  Team-Room  ").unwrap(),
            "team-room"
        );
        assert!(normalize_channel_name("").is_err());
        assert!(normalize_channel_name("bad name").is_err());
        assert!(normalize_channel_name(&"a".repeat(41)).is_err());
    }

    #[test]
    fn draft_bodies_are_trimmed_and_bounded() {
        assert_eq!(normalize_draft_body("  hello  ").unwrap(), "hello");
        assert_eq!(normalize_draft_body("   ").unwrap(), "");
        assert!(normalize_draft_body(&"x".repeat(2001)).is_err());
    }

    #[test]
    fn invite_tokens_are_stored_as_one_way_hashes() {
        assert_ne!(invite_token_hash("token"), "token");
        assert_eq!(invite_token_hash("token"), invite_token_hash("token"));
    }
}

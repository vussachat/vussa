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
    let already_member: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM channel_members WHERE channel_id=$1 AND user_id=$2)")
        .bind(channel_id).bind(session.user.id).fetch_one(&mut *tx).await?;
    if !already_member {
        sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',NULL,$3)")
            .bind(channel_id).bind(session.user.id).bind(now).execute(&mut *tx).await?;
        sqlx::query("UPDATE channel_invite_links SET uses=uses+1 WHERE token_hash=$1")
            .bind(hash)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if !already_member {
        publish_system_message(
            &state,
            row.get("name"),
            format!("{} joined the conversation", session.user.username),
        )
        .await?;
    }
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
    let name = create_public_channel(&state, &session.user, &request.name).await?;
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

pub(crate) async fn create_private_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CreateChannelRequest>,
) -> Result<(StatusCode, Json<ConversationSummary>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let (id, _name) = create_private_conversation(&state, &session.user, &request.name).await?;
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
    let (channel_id, _name) = open_direct(&state, &session.user, request.user_id).await?;
    let summary = list_visible_conversations(&state.database, session.user.id)
        .await?
        .into_iter()
        .find(|item| item.id == channel_id)
        .ok_or(RepositoryError::NotFound)?;
    Ok(Json(summary))
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

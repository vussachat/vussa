use super::*;

pub(crate) async fn admin_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    admin_users_query(state, headers, query).await
}

pub(crate) async fn admin_users_query(
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

pub(crate) async fn admin_disable_user(
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

pub(crate) async fn admin_enable_user(
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

pub(crate) async fn admin_delete_user(
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

pub(crate) async fn admin_reset_password(
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

pub(crate) async fn admin_invalidate_sessions(
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

pub(crate) async fn admin_roles(
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

pub(crate) async fn admin_permissions(
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

pub(crate) async fn admin_participants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(channel): axum::extract::Path<String>,
) -> Result<Json<Vec<Participant>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    Ok(Json(list_presence(&state.valkey, &channel).await?))
}

pub(crate) async fn admin_operations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "users:read")?;
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")
            .fetch_one(&state.database)
            .await?;
    let pending_notifications: i64 =
        sqlx::query_scalar("SELECT count(*) FROM notification_deliveries WHERE sent_at IS NULL")
            .fetch_one(&state.database)
            .await?;
    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE disabled_at IS NULL")
        .fetch_one(&state.database)
        .await?;
    let mut connection = valkey_commands()?;
    let _: String = redis::cmd("PING").query_async(&mut connection).await?;
    Ok(Json(
        serde_json::json!({"postgres":"ok", "valkey":"ok", "sessions":"valkey", "websockets": ACTIVE_WEBSOCKETS.load(Ordering::Relaxed), "outbox_pending": pending, "notification_deliveries_pending": pending_notifications, "active_users": users, "cache":"valkey"}),
    ))
}

pub(crate) async fn admin_bans(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "moderation:read")?;
    let rows = sqlx::query("SELECT b.id,b.user_id,u.username,c.name AS channel,b.reason,b.created_by,b.expires_at,b.created_at,b.revoked_at FROM user_bans b JOIN users u ON u.id=b.user_id LEFT JOIN channels c ON c.id=b.channel_id WHERE ($1::uuid IS NULL OR b.user_id=$1) ORDER BY b.created_at DESC,b.id DESC LIMIT $2")
        .bind(query.user).bind(query.limit.unwrap_or(100).clamp(1, 200)).fetch_all(&state.database).await?;
    Ok(Json(rows.into_iter().map(|row| serde_json::json!({
        "id": row.get::<Uuid,_>("id"), "user_id": row.get::<Uuid,_>("user_id"),
        "username": row.get::<String,_>("username"), "channel": row.get::<Option<String>,_>("channel"),
        "reason": row.get::<String,_>("reason"), "created_by": row.get::<Option<Uuid>,_>("created_by"),
        "expires_at": row.get::<Option<i64>,_>("expires_at"), "created_at": row.get::<i64,_>("created_at"),
        "revoked_at": row.get::<Option<i64>,_>("revoked_at")
    })).collect()))
}

pub(crate) async fn admin_create_ban(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<BanRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    let reason = request.reason.trim();
    if reason.is_empty() || reason.len() > 500 {
        return Err(AppError::bad_request("ban reason must be 1–500 characters"));
    }
    if let Some(expires_at) = request.expires_at
        && expires_at <= now_millis() as i64
    {
        return Err(AppError::bad_request("ban expiry must be in the future"));
    }
    let user_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND deleted_at IS NULL)")
            .bind(request.user_id)
            .fetch_one(&state.database)
            .await?;
    if !user_exists {
        return Err(RepositoryError::NotFound.into());
    }
    let channel_id: Option<Uuid> = if let Some(name) = request.channel.as_deref() {
        Some(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM channels WHERE name=$1 AND deleted_at IS NULL",
            )
            .bind(name)
            .fetch_optional(&state.database)
            .await?
            .ok_or(RepositoryError::NotFound)?,
        )
    } else {
        None
    };
    let id = Uuid::now_v7();
    let now = now_millis() as i64;
    sqlx::query("INSERT INTO user_bans (id,user_id,channel_id,reason,created_by,expires_at,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .bind(id).bind(request.user_id).bind(channel_id).bind(reason).bind(session.user.id).bind(request.expires_at).bind(now).execute(&state.database).await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'user.banned','user',$3,jsonb_build_object('reason',$4,'channel_id',$5),$6)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(request.user_id).bind(reason).bind(channel_id).bind(now).execute(&state.database).await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({"id": id}))))
}

pub(crate) async fn admin_revoke_ban(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    let mut tx = state.database.begin().await?;
    let row = sqlx::query(
        "UPDATE user_bans SET revoked_at=$1,revoked_by=$2 WHERE id=$3 AND revoked_at IS NULL RETURNING user_id",
    )
    .bind(now_millis() as i64)
    .bind(session.user.id)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let user_id: Uuid = row.ok_or(RepositoryError::NotFound)?.get("user_id");
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'user.ban_revoked','user',$3,jsonb_build_object('ban_id',$4),$5)")
        .bind(Uuid::now_v7())
        .bind(session.user.id)
        .bind(user_id)
        .bind(id)
        .bind(now_millis() as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn admin_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "channels:read")?;
    let search = query.q.unwrap_or_default();
    let rows = sqlx::query("SELECT c.id,c.name,c.kind,c.owner_user_id,c.description,c.created_at,c.archived_at,c.deleted_at,c.retention_days,c.posting_restricted,(SELECT count(*) FROM channel_members cm WHERE cm.channel_id=c.id) AS member_count FROM channels c WHERE ($1='' OR lower(c.name) LIKE lower('%'||$1||'%')) ORDER BY (c.name='main') DESC,c.name LIMIT $2").bind(search).bind(query.limit.unwrap_or(100).clamp(1,200)).fetch_all(&state.database).await?;
    let items = rows.into_iter().map(|row| serde_json::json!({"id":row.get::<Uuid,_>("id"),"name":row.get::<String,_>("name"),"kind":row.get::<String,_>("kind"),"owner_user_id":row.get::<Option<Uuid>,_>("owner_user_id"),"description":row.get::<String,_>("description"),"created_at":row.get::<i64,_>("created_at"),"archived_at":row.get::<Option<i64>,_>("archived_at"),"deleted_at":row.get::<Option<i64>,_>("deleted_at"),"retention_days":row.get::<i32,_>("retention_days"),"posting_restricted":row.get::<bool,_>("posting_restricted"),"member_count":row.get::<i64,_>("member_count")})).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({"items":items})))
}

pub(crate) async fn admin_create_channel(
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
    let retention_days = validate_retention_days(request.retention_days)?;
    if !state.repository.create_channel(&name).await? {
        return Err(AppError::bad_request("channel already exists"));
    }
    sqlx::query("UPDATE channels SET description=COALESCE($1,description),retention_days=COALESCE($2,retention_days),posting_restricted=COALESCE($3,posting_restricted) WHERE name=$4").bind(request.description).bind(retention_days).bind(request.posting_restricted).bind(&name).execute(&state.database).await?;
    let channel_id: Uuid = sqlx::query_scalar("SELECT id FROM channels WHERE name=$1")
        .bind(&name)
        .fetch_one(&state.database)
        .await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.created','channel',$3,$4)")
        .bind(Uuid::now_v7())
        .bind(session.user.id)
        .bind(channel_id)
        .bind(now_millis() as i64)
        .execute(&state.database)
        .await?;
    publish_system_message(
        &state,
        &name,
        format!("{} created this channel", session.user.username),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({"name":name}))))
}

pub(crate) async fn admin_update_channel(
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
    let retention_days = validate_retention_days(request.retention_days)?;
    sqlx::query("UPDATE channels SET name=COALESCE($1,name),description=COALESCE($2,description),retention_days=COALESCE($3,retention_days),posting_restricted=COALESCE($4,posting_restricted) WHERE id=$5").bind(name).bind(request.description).bind(retention_days).bind(request.posting_restricted).bind(id).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.updated','channel',$3,$4)").bind(Uuid::now_v7()).bind(session.user.id).bind(id).bind(now_millis() as i64).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

fn validate_retention_days(value: Option<i32>) -> Result<Option<i32>, AppError> {
    if value.is_some_and(|days| !(1..=3650).contains(&days)) {
        return Err(AppError::bad_request(
            "retention must be between 1 and 3650 days",
        ));
    }
    Ok(value)
}

pub(crate) async fn admin_channel_state(
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
    if matches!(action.as_str(), "archive" | "restore") {
        let message = if action == "archive" {
            "This channel was archived and is now read-only"
        } else {
            "This channel was restored"
        };
        publish_system_message(&state, &row.get::<String, _>("name"), message).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn admin_messages(
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

pub(crate) async fn admin_moderate_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((id, action)): axum::extract::Path<(Uuid, String)>,
    Json(request): Json<ModerationRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    let mut tx = state.database.begin().await?;
    let _row = sqlx::query("SELECT m.channel_id,c.name AS channel FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 FOR UPDATE")
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
    refresh_moderated_message(&state, id).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_moderated_message(state: &AppState, id: Uuid) {
    let row = match sqlx::query("SELECT m.id,c.name AS channel,m.username,CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 AND c.deleted_at IS NULL")
        .bind(id)
        .fetch_optional(&state.database)
        .await
    {
        Ok(Some(row)) => row,
        _ => return,
    };
    let channel: String = row.get("channel");
    let message = ChatMessage {
        id: row.get("id"),
        channel: channel.clone(),
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
    };
    let _ = broadcast(
        &state.valkey,
        &channel,
        &ServerEvent::MessageUpdated { message },
    )
    .await;
    if let Ok(mut connection) = valkey_commands() {
        let _: redis::RedisResult<usize> = connection.del(history_key(&channel)).await;
        let _: redis::RedisResult<usize> = connection.del(history_order_key(&channel)).await;
    }
}

pub(crate) async fn admin_message_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "moderation:read")?;
    let rows = sqlx::query("SELECT id,editor_user_id,previous_text,created_at FROM message_edit_history WHERE message_id=$1 ORDER BY created_at DESC,id DESC").bind(id).fetch_all(&state.database).await?;
    Ok(Json(rows.into_iter().map(|row| serde_json::json!({"id":row.get::<Uuid,_>("id"),"editor_user_id":row.get::<Option<Uuid>,_>("editor_user_id"),"previous_text":row.get::<String,_>("previous_text"),"created_at":row.get::<i64,_>("created_at")})).collect()))
}

pub(crate) async fn admin_bulk_moderate(
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
    let action = request.action;
    let reason = request.reason;
    let mut tx = state.database.begin().await?;
    let mut results = Vec::with_capacity(request.ids.len());
    let mut changed_ids = Vec::new();
    for id in request.ids {
        let result = if action == "delete" {
            sqlx::query("UPDATE messages SET deleted_at=$1,deleted_by=$2,deletion_reason=$3 WHERE id=$4 AND deleted_at IS NULL").bind(now_millis() as i64).bind(session.user.id).bind(reason.clone()).bind(id).execute(&mut *tx).await?
        } else {
            sqlx::query("UPDATE messages SET deleted_at=NULL,deleted_by=NULL,deletion_reason=NULL WHERE id=$1 AND deleted_at IS NOT NULL").bind(id).execute(&mut *tx).await?
        };
        results.push(serde_json::json!({"id": id, "updated": result.rows_affected() > 0}));
        if result.rows_affected() > 0 {
            sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,$3,'message',$4,jsonb_build_object('reason',$5,'bulk',true),$6)")
                .bind(Uuid::now_v7())
                .bind(session.user.id)
                .bind(format!("message.{action}"))
                .bind(id)
                .bind(reason.clone())
                .bind(now_millis() as i64)
                .execute(&mut *tx)
                .await?;
            changed_ids.push(id);
        }
    }
    tx.commit().await?;
    for id in changed_ids {
        refresh_moderated_message(&state, id).await;
    }
    Ok(Json(serde_json::json!({"results": results})))
}

pub(crate) async fn admin_assign_role(
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

pub(crate) async fn admin_remove_role(
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

pub(crate) async fn admin_audit(
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

#[cfg(test)]
mod policy_tests {
    use super::*;

    #[test]
    fn retention_policy_rejects_unbounded_values() {
        assert!(validate_retention_days(Some(0)).is_err());
        assert!(validate_retention_days(Some(3651)).is_err());
        assert_eq!(validate_retention_days(Some(90)).unwrap(), Some(90));
        assert_eq!(validate_retention_days(None).unwrap(), None);
    }
}

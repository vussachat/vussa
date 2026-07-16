use super::*;

pub(crate) fn recovery_token_hash(token: &str) -> String {
    hex::encode(sha2::Sha256::digest(token.as_bytes()))
}

fn client_ip(headers: &HeaderMap) -> String {
    headers
        .get("cf-connecting-ip")
        .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
        })
        .or_else(|| {
            headers
                .get("x-forwarded-for")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.split(',').next())
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn validate_username(username: &str) -> Result<(), AppError> {
    if username.len() < 2
        || username.len() > 40
        || !username
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AppError::bad_request("invalid username"));
    }
    let lower = username.to_lowercase();
    if lower == "admin"
        || lower == "system"
        || lower == "here"
        || lower == "channel"
        || lower == "everyone"
        || lower == "administrator"
        || lower == "moderator"
    {
        return Err(AppError::bad_request("username is reserved"));
    }
    Ok(())
}

pub(crate) async fn register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<RegisterRequest>,
) -> Result<(StatusCode, HeaderMap, Json<AuthUser>), AppError> {
    let email = request.email.trim().to_lowercase();
    let username = request.username.trim().to_string();
    if !email.contains('@') || email.len() > 320 {
        return Err(AppError::bad_request("invalid email"));
    }
    validate_username(&username)?;

    let ip = client_ip(&headers);
    let ip_hash = hex::encode(sha2::Sha256::digest(ip.as_bytes()));
    let register_ip_key = format!("chat:rate:register:ip:{ip_hash}");
    enforce_rate_limit(&state.valkey, &register_ip_key, 5, 3600).await?;

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

pub(crate) async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<AuthUser>), AppError> {
    let email_trimmed = request.email.trim().to_lowercase();

    let ip = client_ip(&headers);
    let ip_hash = hex::encode(sha2::Sha256::digest(ip.as_bytes()));
    let login_ip_key = format!("chat:rate:login:ip:{ip_hash}");
    enforce_rate_limit(&state.valkey, &login_ip_key, 20, 300).await?;

    let target_hash = hex::encode(sha2::Sha256::digest(email_trimmed.as_bytes()));
    let login_target_key = format!("chat:rate:login:target:{target_hash}");
    enforce_rate_limit(&state.valkey, &login_target_key, 5, 300).await?;

    let found = state
        .repository
        .find_user_for_login(request.email.trim())
        .await?;

    let (user, hash, disabled) = match found {
        Some((user, hash, disabled)) => (Some(user), hash, disabled),
        None => {
            static DUMMY_HASH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
            let dummy_hash = DUMMY_HASH.get_or_init(|| {
                crate::auth::hash_password_unchecked("dummy_password_for_user_enumeration_prevention").unwrap()
            }).clone();
            (None, dummy_hash, false)
        }
    };

    let verified = verify_password_coalesced(&state, user.as_ref().map(|u| u.id).unwrap_or_else(Uuid::default), request.password, hash).await?;

    if disabled {
        return Err(AppError::unauthorized("invalid credentials"));
    }
    let Some(user) = user else {
        return Err(AppError::unauthorized("invalid credentials"));
    };
    if !verified {
        return Err(AppError::unauthorized("invalid credentials"));
    }

    metrics::record_authentication();
    let (id, csrf) = create_session(&state.valkey, &user).await?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert("set-cookie", auth_cookie(id));
    response_headers.insert(CSRF_HEADER, HeaderValue::from_str(&csrf).unwrap());
    Ok((response_headers, Json(user)))
}

pub(crate) async fn request_recovery(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RecoveryRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let email = request.email.trim().to_lowercase();
    if email.is_empty() || email.len() > 320 {
        return Err(AppError::bad_request("invalid email"));
    }
    let key = format!(
        "chat:rate:recovery:{}",
        hex::encode(Sha256::digest(email.as_bytes()))
    );
    enforce_rate_limit(&state.valkey, &key, 5, 3600).await?;
    let generic = || {
        Json(
            serde_json::json!({"message": "If the account exists, recovery instructions will be sent."}),
        )
    };
    let Some(row) = sqlx::query("SELECT id,email FROM users WHERE lower(email)=lower($1) AND disabled_at IS NULL AND deleted_at IS NULL")
        .bind(&email)
        .fetch_optional(&state.database)
        .await?
    else {
        return Ok(generic());
    };
    let user_id: Uuid = row.get("id");
    let token = hex::encode({
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        bytes
    });
    let now = state.clock.now_millis() as i64;
    let mut tx = state.database.begin().await?;
    sqlx::query("DELETE FROM account_recovery_tokens WHERE user_id=$1 AND used_at IS NULL")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO account_recovery_tokens (id,user_id,token_hash,expires_at,created_at) VALUES ($1,$2,$3,$4,$5)")
        .bind(Uuid::now_v7())
        .bind(user_id)
        .bind(recovery_token_hash(&token))
        .bind(now + 30 * 60 * 1000)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if let Err(error) = state
        .recovery_notifier
        .notify(row.get("email"), &token)
        .await
    {
        tracing::error!(?error, user_id = %user_id, "account recovery delivery failed");
    }
    Ok(generic())
}

pub(crate) async fn reset_recovery(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RecoveryResetRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let token = request.token.trim();
    if token.len() != 64 || !token.chars().all(|value| value.is_ascii_hexdigit()) {
        return Err(AppError::unauthorized("invalid or expired recovery token"));
    }
    let hash = recovery_token_hash(token);
    let password_hash = password_hash(&request.password)?;
    let now = state.clock.now_millis() as i64;
    let mut tx = state.database.begin().await?;
    let user_id: Uuid = sqlx::query_scalar("SELECT user_id FROM account_recovery_tokens WHERE token_hash=$1 AND used_at IS NULL AND expires_at>$2 FOR UPDATE")
        .bind(hash)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| AppError::unauthorized("invalid or expired recovery token"))?;
    sqlx::query("UPDATE users SET password_hash=$1,role_version=role_version+1,updated_at=$2 WHERE id=$3 AND deleted_at IS NULL")
        .bind(password_hash)
        .bind(now)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE account_recovery_tokens SET used_at=$1 WHERE token_hash=$2")
        .bind(now)
        .bind(recovery_token_hash(token))
        .execute(&mut *tx)
        .await?;
    record_audit(
        &mut tx,
        AuditEvent {
            actor: Some(user_id),
            action: "user.password_recovered",
            target_type: "user",
            target_id: user_id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    queue_auth_invalidation(&mut tx, user_id, now).await?;
    tx.commit().await?;
    Ok(Json(
        serde_json::json!({"message": "password reset successfully"}),
    ))
}

pub(crate) async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let mut connection = state.valkey.connection()?;
    let _: usize = connection.del(session_key(session.id)).await?;
    let _: usize = connection
        .srem(
            format!("vussa:user_sessions:{}", session.user.id),
            session.id.to_string(),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn me(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<AuthUser>), AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(CSRF_HEADER, HeaderValue::from_str(&session.csrf).unwrap());
    Ok((response_headers, Json(session.user)))
}

pub(crate) async fn update_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<AccountUpdateRequest>,
) -> Result<Json<AuthUser>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let username = request.username.trim();
    validate_username(username)?;
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
    let mut connection = state.valkey.connection()?;
    let _: usize = connection
        .hset(session_key(session.id), "user", serialized)
        .await?;
    Ok(Json(user))
}

pub(crate) async fn export_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    // Sessions are intentionally held in Valkey, not PostgreSQL. Export all
    // messages authored by the user by joining on the durable username, while
    // preserving deleted-message metadata.
    let rows = sqlx::query("SELECT c.name AS channel,m.id,m.text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.owner_user_id=$1 ORDER BY m.created_at")
        .bind(session.user.id).fetch_all(&state.database).await?;
    let messages = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "channel": row.get::<String,_>("channel"), "id": row.get::<Uuid,_>("id"),
                "text": row.get::<String,_>("text"), "created_at": row.get::<i64,_>("created_at"),
                "edited": row.get::<bool,_>("edited"), "deleted": row.get::<bool,_>("deleted")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!({
        "user": session.user, "messages": messages,
        "exported_at": now_millis()
    })))
}

pub(crate) async fn delete_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let now = state.clock.now_millis() as i64;
    let mut tx = state.database.begin().await?;
    sqlx::query("UPDATE users SET deleted_at=$1,disabled_at=$1,role_version=role_version+1,updated_at=$1 WHERE id=$2 AND deleted_at IS NULL")
        .bind(now).bind(session.user.id).execute(&mut *tx).await?;
    record_audit(
        &mut tx,
        AuditEvent {
            actor: Some(session.user.id),
            action: "user.self_deleted",
            target_type: "user",
            target_id: session.user.id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    tx.commit().await?;
    let mut connection = state.valkey.connection()?;
    let sessions_key = format!("vussa:user_sessions:{}", session.user.id);
    let ids: Vec<String> = connection.smembers(&sessions_key).await?;
    for id in ids {
        if let Ok(id) = Uuid::parse_str(&id) {
            let _: usize = connection.del(session_key(id)).await?;
        }
    }
    let _: usize = connection.del(&sessions_key).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn change_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<PasswordChangeRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let Some((_, current_hash, disabled)) = state
        .repository
        .find_user_for_login(&session.user.username)
        .await?
    else {
        return Err(AppError::unauthorized("account unavailable"));
    };
    if disabled
        || !verify_password_coalesced(
            &state,
            session.user.id,
            request.current_password,
            current_hash,
        )
        .await?
    {
        return Err(AppError::unauthorized("current password is incorrect"));
    }
    let hash = password_hash(&request.new_password)?;
    let now = state.clock.now_millis() as i64;
    sqlx::query("UPDATE users SET password_hash=$1,updated_at=$2,role_version=role_version+1 WHERE id=$3 AND deleted_at IS NULL")
        .bind(hash).bind(now).bind(session.user.id).execute(&state.database).await?;
    let mut connection = state.valkey.connection()?;
    let sessions_key = format!("vussa:user_sessions:{}", session.user.id);
    let sessions: Vec<String> = connection.smembers(&sessions_key).await?;
    for id in sessions {
        if id != session.id.to_string() {
            let Ok(session_id) = Uuid::parse_str(&id) else {
                continue;
            };
            let _: usize = connection.del(session_key(session_id)).await?;
            let _: usize = connection.srem(&sessions_key, id).await?;
        }
    }
    record_audit_pool(
        &state.database,
        AuditEvent {
            actor: Some(session.user.id),
            action: "user.password_changed",
            target_type: "user",
            target_id: session.user.id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_sessions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<SessionView>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let mut connection = state.valkey.connection()?;
    let ids: Vec<String> = connection
        .smembers(format!("vussa:user_sessions:{}", session.user.id))
        .await?;
    let mut result = Vec::new();
    for id in ids {
        let Ok(id) = Uuid::parse_str(&id) else {
            continue;
        };
        let exists: bool = connection.exists(session_key(id)).await?;
        if exists {
            result.push(SessionView {
                id,
                current: id == session.id,
            });
        }
    }
    Ok(Json(result))
}

pub(crate) async fn revoke_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    if id == session.id {
        return Err(AppError::bad_request(
            "use logout to revoke the current session",
        ));
    }
    let mut connection = state.valkey.connection()?;
    let _: usize = connection.del(session_key(id)).await?;
    let _: usize = connection
        .srem(
            format!("vussa:user_sessions:{}", session.user.id),
            id.to_string(),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

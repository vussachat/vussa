use super::*;

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
    invite_member(&state, &session.user, &name, request.user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn remove_channel_member(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path((name, user_id)): axum::extract::Path<(String, Uuid)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    remove_member(&state, &session.user, &name, user_id).await?;
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
    record_audit_pool(
        &state.database,
        AuditEvent {
            actor: Some(session.user.id),
            action: "channel.member_left",
            target_type: "channel",
            target_id: row.get("id"),
            metadata: serde_json::json!({"user_id": session.user.id}),
            created_at: state.clock.now_millis() as i64,
        },
    )
    .await?;
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
    record_audit_pool(
        &state.database,
        AuditEvent {
            actor: Some(session.user.id),
            action: "channel.moderator_promoted",
            target_type: "channel",
            target_id: channel_id,
            metadata: serde_json::json!({"user_id": user_id}),
            created_at: state.clock.now_millis() as i64,
        },
    )
    .await?;
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
    record_audit_pool(
        &state.database,
        AuditEvent {
            actor: Some(session.user.id),
            action: "channel.moderator_demoted",
            target_type: "channel",
            target_id: channel_id,
            metadata: serde_json::json!({"user_id": user_id}),
            created_at: state.clock.now_millis() as i64,
        },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

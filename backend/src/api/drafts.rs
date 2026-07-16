use super::*;

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

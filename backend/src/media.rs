use super::*;

pub(crate) async fn get_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<ChatMessage>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let row = sqlx::query("SELECT m.id,c.name AS channel,m.username,CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR EXISTS(SELECT 1 FROM channel_members cm WHERE cm.channel_id=c.id AND cm.user_id=$2))")
        .bind(id).bind(session.user.id).fetch_optional(&state.database).await?.ok_or(RepositoryError::NotFound)?;
    let channel: String = row.get("channel");
    ensure_channel_access(&state.database, &channel, session.user.id).await?;
    Ok(Json(ChatMessage {
        id: row.get("id"),
        channel,
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

async fn ensure_message_access(
    database: &PgPool,
    message_id: Uuid,
    user_id: Uuid,
) -> Result<(), AppError> {
    let channel = sqlx::query_scalar::<_, String>("SELECT c.name FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel_id=c.id AND cm.user_id=$2))")
        .bind(message_id)
        .bind(user_id)
        .fetch_optional(database)
        .await?
        .ok_or(RepositoryError::NotFound)?;
    ensure_channel_access(database, &channel, user_id).await
}

pub(crate) async fn message_permalink(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    ensure_message_access(&state.database, id, session.user.id).await?;
    let origin = headers
        .get("origin")
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
        .unwrap_or("");
    Ok(Json(serde_json::json!({"url": permalink_url(origin, id)})))
}

fn permalink_url(origin: &str, id: Uuid) -> String {
    format!("{origin}/?message={id}")
}

pub(crate) async fn save_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    ensure_message_access(&state.database, id, session.user.id).await?;
    sqlx::query("INSERT INTO saved_messages (user_id,message_id,created_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(session.user.id).bind(id).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn unsave_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    sqlx::query("DELETE FROM saved_messages WHERE user_id=$1 AND message_id=$2")
        .bind(session.user.id)
        .bind(id)
        .execute(&state.database)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_saved_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<Vec<ChatMessage>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let rows = sqlx::query("SELECT m.id,c.name AS channel,m.username,m.text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM saved_messages s JOIN messages m ON m.id=s.message_id JOIN channels c ON c.id=m.channel_id WHERE s.user_id=$1 AND c.deleted_at IS NULL AND (c.kind='public' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel_id=c.id AND cm.user_id=$1)) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $3) AND (b.channel_id IS NULL OR b.channel_id=c.id)) ORDER BY s.created_at DESC,s.message_id DESC LIMIT $2")
        .bind(session.user.id).bind(query.limit.unwrap_or(100).clamp(1, 200)).bind(now_millis() as i64).fetch_all(&state.database).await?;
    Ok(Json(
        rows.into_iter()
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
            .collect(),
    ))
}

pub(crate) async fn report_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<MessageReportRequest>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    let reason = request.reason.trim();
    if reason.is_empty() || reason.len() > 500 {
        return Err(AppError::bad_request(
            "report reason must be 1–500 characters",
        ));
    }
    ensure_message_access(&state.database, request.message_id, session.user.id).await?;
    let report_id: Uuid = sqlx::query("INSERT INTO message_reports (id,message_id,reporter_user_id,reason,created_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (message_id,reporter_user_id) DO UPDATE SET reason=EXCLUDED.reason,status='open',resolved_at=NULL,resolved_by=NULL RETURNING id")
        .bind(Uuid::now_v7()).bind(request.message_id).bind(session.user.id).bind(reason).bind(now_millis() as i64).fetch_one(&state.database).await?.get("id");
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'report.created','message_report',$3,jsonb_build_object('message_id',$4,'reason',$5),$6)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(report_id).bind(request.message_id).bind(reason).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_message_reports(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_permission(&session.user, "moderation:read")?;
    let rows = sqlx::query("SELECT r.id,r.message_id,r.reporter_user_id,r.reason,r.status,r.created_at,r.resolved_at,m.text,c.name AS channel,m.username FROM message_reports r JOIN messages m ON m.id=r.message_id JOIN channels c ON c.id=m.channel_id WHERE ($1::text IS NULL OR r.status=$1) ORDER BY r.created_at DESC,r.id DESC LIMIT $2")
        .bind(query.status).bind(query.limit.unwrap_or(100).clamp(1, 200)).fetch_all(&state.database).await?;
    Ok(Json(rows.into_iter().map(|row| serde_json::json!({
        "id": row.get::<Uuid,_>("id"), "message_id": row.get::<Uuid,_>("message_id"),
        "reporter_user_id": row.get::<Uuid,_>("reporter_user_id"), "reason": row.get::<String,_>("reason"),
        "status": row.get::<String,_>("status"), "created_at": row.get::<i64,_>("created_at"),
        "resolved_at": row.get::<Option<i64>,_>("resolved_at"), "text": row.get::<String,_>("text"),
        "channel": row.get::<String,_>("channel"), "username": row.get::<String,_>("username")
    })).collect()))
}

pub(crate) async fn update_message_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, action)): Path<(Uuid, String)>,
) -> Result<StatusCode, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "moderation:write")?;
    let (status, resolved_at, resolved_by) = match action.as_str() {
        "resolve" => ("resolved", Some(now_millis() as i64), Some(session.user.id)),
        "reopen" => ("open", None, None),
        _ => return Err(AppError::bad_request("unknown report action")),
    };
    let result = sqlx::query(
        "UPDATE message_reports SET status=$1,resolved_at=$2,resolved_by=$3 WHERE id=$4",
    )
    .bind(status)
    .bind(resolved_at)
    .bind(resolved_by)
    .bind(id)
    .execute(&state.database)
    .await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,$3,'message_report',$4,jsonb_build_object('status',$5),$6)")
        .bind(Uuid::now_v7()).bind(session.user.id).bind(format!("report.{action}"))
        .bind(id).bind(status).bind(now_millis() as i64).execute(&state.database).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn upload_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<FileUploadResponse>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    require_csrf(&headers, &session)?;
    require_permission(&session.user, "chat:write")?;
    let globally_banned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM user_bans WHERE user_id=$1 AND channel_id IS NULL AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2))")
        .bind(session.user.id)
        .bind(now_millis() as i64)
        .fetch_one(&state.database)
        .await?;
    if globally_banned {
        return Err(AppError::forbidden("conversation access denied"));
    }
    enforce_rate_limit(&format!("chat:rate:upload:{}", session.user.id), 30, 60).await?;
    let mut original_name = "upload.bin".to_string();
    let mut content_type = "application/octet-stream".to_string();
    let mut bytes = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| AppError::bad_request("invalid multipart body"))?
    {
        if field.name() != Some("file") {
            continue;
        }
        if let Some(name) = field.file_name() {
            original_name = sanitize_upload_name(name);
        }
        if let Some(value) = field.content_type() {
            content_type = value.to_string();
        }
        let data = field
            .bytes()
            .await
            .map_err(|_| AppError::bad_request("could not read upload"))?;
        if data.len() > MAX_FILE_BYTES {
            return Err(AppError::bad_request("file exceeds the 25 MiB limit"));
        }
        bytes = Some(data);
        break;
    }
    let bytes = bytes.ok_or_else(|| AppError::bad_request("multipart field 'file' is required"))?;
    match state
        .scanner
        .scan(&original_name, &content_type, &bytes)
        .await
    {
        Ok(()) => {}
        Err(ScanError::Rejected) => return Err(AppError::bad_request("file rejected by scanner")),
        Err(ScanError::Unavailable(error)) => {
            tracing::error!(%error, "file scanner unavailable");
            return Err(AppError::service_unavailable("file scanner unavailable"));
        }
    }
    let id = Uuid::now_v7();
    let storage_key = format!("{id}.bin");
    state
        .blob_store
        .put(&storage_key, &bytes)
        .await
        .map_err(|_| AppError::service_unavailable("file storage unavailable"))?;
    let checksum = hex::encode(Sha256::digest(&bytes));
    if let Err(error) = sqlx::query("INSERT INTO files (id,uploader_user_id,storage_key,original_name,content_type,size_bytes,checksum,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(id).bind(session.user.id).bind(&storage_key).bind(&original_name).bind(&content_type).bind(bytes.len() as i64).bind(&checksum).bind(state.clock.now_millis() as i64)
        .execute(&state.database).await
    {
        let _ = state.blob_store.delete(&storage_key).await;
        return Err(error.into());
    }
    Ok(Json(FileUploadResponse {
        id,
        name: original_name,
        content_type,
        size_bytes: bytes.len() as u64,
        download_url: format!("/api/v1/files/{id}"),
    }))
}

pub(crate) fn sanitize_upload_name(name: &str) -> String {
    name.rsplit(['/', '\\'])
        .next()
        .unwrap_or("upload.bin")
        .chars()
        .filter(|character| !character.is_control())
        .take(255)
        .collect::<String>()
        .trim()
        .to_string()
}

pub(crate) async fn download_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let row = sqlx::query("SELECT f.storage_key,f.original_name,f.content_type FROM files f WHERE f.id=$1 AND f.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM user_bans global_ban WHERE global_ban.user_id=$2 AND global_ban.revoked_at IS NULL AND (global_ban.expires_at IS NULL OR global_ban.expires_at > $3) AND global_ban.channel_id IS NULL) AND ((f.uploader_user_id=$2 AND NOT EXISTS (SELECT 1 FROM user_bans uploader_ban WHERE uploader_ban.user_id=$2 AND uploader_ban.revoked_at IS NULL AND (uploader_ban.expires_at IS NULL OR uploader_ban.expires_at > $3))) OR EXISTS (SELECT 1 FROM message_files mf JOIN messages m ON m.id=mf.message_id JOIN channels c ON c.id=m.channel_id LEFT JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=$2 WHERE mf.file_id=f.id AND m.deleted_at IS NULL AND c.deleted_at IS NULL AND (c.kind='public' OR cm.user_id IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM user_bans channel_ban WHERE channel_ban.user_id=$2 AND channel_ban.revoked_at IS NULL AND (channel_ban.expires_at IS NULL OR channel_ban.expires_at > $3) AND (channel_ban.channel_id IS NULL OR channel_ban.channel_id=c.id))))")
        .bind(id).bind(session.user.id).bind(now_millis() as i64).fetch_optional(&state.database).await?.ok_or(RepositoryError::NotFound)?;
    let storage_key: String = row.get("storage_key");
    let data = state
        .blob_store
        .get(&storage_key)
        .await
        .map_err(|_| AppError::service_unavailable("file storage unavailable"))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", row.get::<String, _>("content_type"))
        .header(
            "content-disposition",
            format!(
                "attachment; filename=\"{}\"",
                row.get::<String, _>("original_name")
            )
            .replace('"', ""),
        )
        .body(Body::from(data))
        .map_err(|_| AppError::service_unavailable("could not construct file response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permalink_urls_are_stable_and_origin_scoped() {
        let id = Uuid::nil();
        assert_eq!(
            permalink_url("https://chat.example.test", id),
            "https://chat.example.test/?message=00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            permalink_url("", id),
            "/?message=00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn upload_names_are_basename_only_and_control_free() {
        assert_eq!(
            sanitize_upload_name("..\\private/\u{0000}notes.txt"),
            "notes.txt"
        );
        assert_eq!(sanitize_upload_name("  report.txt  "), "report.txt");
    }
}

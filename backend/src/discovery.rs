use super::*;

pub(crate) async fn health() -> &'static str {
    "ok"
}

pub(crate) async fn live() -> &'static str {
    "ok"
}
pub(crate) async fn startup() -> &'static str {
    "ok"
}
pub(crate) async fn metrics() -> ([(HeaderName, HeaderValue); 1], String) {
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
pub(crate) async fn ready(State(state): State<Arc<AppState>>) -> Result<&'static str, StatusCode> {
    if SHUTTING_DOWN.load(Ordering::Acquire) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    state
        .cache_health
        .ping()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state
        .database_health
        .ping()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok("ok")
}

pub(crate) async fn list_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<Channel>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let names = state.repository.list_channels(session.user.id).await?;
    Ok(Json(
        names.into_iter().map(|name| Channel { name }).collect(),
    ))
}

pub(crate) async fn list_unread(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let rows = sqlx::query(
        "SELECT c.name,COUNT(m.id)::BIGINT AS unread_count,MAX(m.created_at) AS latest_created_at FROM channels c LEFT JOIN channel_members cm ON cm.channel_id=c.id AND cm.user_id=$1 LEFT JOIN channel_reads cr ON cr.channel_id=c.id AND cr.user_id=$1 LEFT JOIN messages m ON m.channel_id=c.id AND m.deleted_at IS NULL AND m.created_at > COALESCE(cr.last_read_created_at,0) AND m.username <> $2 WHERE c.deleted_at IS NULL AND c.archived_at IS NULL AND (c.kind='public' OR cm.user_id IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $3) AND (b.channel_id IS NULL OR b.channel_id=c.id)) GROUP BY c.id,c.name ORDER BY c.name",
    )
    .bind(session.user.id)
    .bind(&session.user.username)
    .bind(now_millis() as i64)
    .fetch_all(&state.database)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| {
                serde_json::json!({
                    "channel": row.get::<String, _>("name"),
                    "unread_count": row.get::<i64, _>("unread_count"),
                    "latest_created_at": row.get::<Option<i64>, _>("latest_created_at"),
                })
            })
            .collect(),
    ))
}

pub(crate) async fn list_conversations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ConversationSummary>>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    Ok(Json(
        list_visible_conversations(&state.database, session.user.id).await?,
    ))
}

pub(crate) async fn search_users(
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

pub(crate) async fn search_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AdminListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let session = load_session(&headers, &state.valkey).await?;
    let term = query.q.unwrap_or_default().trim().to_string();
    if term.is_empty() || term.len() > 200 {
        return Ok(Json(serde_json::json!({"items": [], "next": null})));
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let rows = sqlx::query("SELECT m.id,c.name AS channel,m.username,m.text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids,ts_headline('simple',m.text,plainto_tsquery('simple',$2),'StartSel=<mark>,StopSel=</mark>') AS highlighted FROM messages m JOIN channels c ON c.id=m.channel_id WHERE c.deleted_at IS NULL AND c.archived_at IS NULL AND m.deleted_at IS NULL AND (c.kind='public' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel_id=c.id AND cm.user_id=$1)) AND NOT EXISTS (SELECT 1 FROM user_bans b WHERE b.user_id=$1 AND b.revoked_at IS NULL AND (b.expires_at IS NULL OR b.expires_at > $9) AND (b.channel_id IS NULL OR b.channel_id=c.id)) AND ($3::text IS NULL OR c.name=$3) AND ($4::bigint IS NULL OR m.created_at >= $4) AND ($5::bigint IS NULL OR m.created_at <= $5) AND ($6::bigint IS NULL OR ($7::uuid IS NOT NULL AND (m.created_at,m.id) < ($6,$7))) AND (m.search_vector @@ plainto_tsquery('simple',$2) OR m.text ILIKE '%'||$2||'%') ORDER BY m.created_at DESC,m.id DESC LIMIT $8")
        .bind(session.user.id).bind(&term).bind(query.channel).bind(query.from).bind(query.to).bind(query.before_created_at).bind(query.before_id).bind(limit + 1).bind(now_millis() as i64).fetch_all(&state.database).await?;
    let has_more = rows.len() > limit as usize;
    let mut items = rows
        .into_iter()
        .map(|row| {
            let text: String = row.get("text");
            MessageSearchResult {
                snippet: text.clone(),
                highlighted: row.get("highlighted"),
                message: ChatMessage {
                    id: row.get("id"),
                    channel: row.get("channel"),
                    username: row.get("username"),
                    text,
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
                },
            }
        })
        .collect::<Vec<_>>();
    if items.len() > limit as usize {
        items.truncate(limit as usize);
    }
    let next = if has_more {
        items.last().map(|item| {
            serde_json::json!({
                "before_created_at": item.message.created_at,
                "before_id": item.message.id,
            })
        })
    } else {
        None
    };
    Ok(Json(serde_json::json!({"items": items, "next": next})))
}

pub(crate) async fn link_preview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<LinkPreviewQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _session = load_session(&headers, &state.valkey).await?;
    let url = validate_preview_url(&query.url)?;
    let preview_address = ensure_public_preview_host(&url).await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(4))
        .resolve(url.host_str().unwrap_or_default(), preview_address)
        .build()
        .map_err(|_| AppError::service_unavailable("preview client unavailable"))?;
    let response = client
        .get(url.clone())
        .header("accept", "text/html,application/xhtml+xml")
        .send()
        .await
        .map_err(|_| AppError::service_unavailable("preview fetch failed"))?;
    if !response.status().is_success() {
        return Err(AppError::bad_request("preview target was not successful"));
    }
    if response.content_length().unwrap_or(0) > 256 * 1024 {
        return Err(AppError::bad_request("preview target is too large"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AppError::service_unavailable("preview fetch failed"))?;
        if bytes.len() + chunk.len() > 256 * 1024 {
            return Err(AppError::bad_request("preview target is too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let html = String::from_utf8_lossy(&bytes);
    Ok(Json(serde_json::json!({
        "url": url.as_str(),
        "title": html_tag_value(&html, "title"),
        "description": html_meta_value(&html, "description"),
        "image_url": html_meta_value(&html, "og:image"),
    })))
}

pub(crate) fn validate_preview_url(raw: &str) -> Result<reqwest::Url, AppError> {
    if raw.len() > 2048 {
        return Err(AppError::bad_request("preview URL is too long"));
    }
    let url = raw
        .parse::<reqwest::Url>()
        .map_err(|_| AppError::bad_request("invalid preview URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.username().is_empty() && url.password().is_some()
        || !url.username().is_empty()
    {
        return Err(AppError::bad_request(
            "preview URL must be a public HTTP URL",
        ));
    }
    Ok(url)
}

pub(crate) async fn ensure_public_preview_host(
    url: &reqwest::Url,
) -> Result<std::net::SocketAddr, AppError> {
    let host = url
        .host_str()
        .ok_or_else(|| AppError::bad_request("preview URL has no host"))?;
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return Err(AppError::forbidden("preview host is not public"));
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| AppError::bad_request("preview host could not be resolved"))?;
    let mut selected = None;
    for address in addresses {
        if !is_public_ip(address.ip()) {
            return Err(AppError::forbidden("preview host is not public"));
        }
        selected.get_or_insert(address);
    }
    selected.ok_or_else(|| AppError::bad_request("preview host could not be resolved"))
}

pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_loopback()
                && !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
        }
        IpAddr::V6(ip) => {
            !ip.is_loopback()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
        }
    }
}

pub(crate) fn html_tag_value(html: &str, tag: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let start = lower.find(&format!("<{tag}"))?;
    let content_start = lower[start..].find('>')? + start + 1;
    let end = lower[content_start..].find(&format!("</{tag}>"))? + content_start;
    let value = html[content_start..end].trim();
    (!value.is_empty()).then(|| value.chars().take(300).collect())
}

pub(crate) fn html_meta_value(html: &str, property: &str) -> Option<String> {
    let lower = html.to_lowercase();
    for marker in [
        format!("property=\"{property}\""),
        format!("name=\"{property}\""),
        format!("property='{property}'"),
        format!("name='{property}'"),
    ] {
        if let Some(start) = lower.find(&marker) {
            let end = lower[start..].find('>')? + start;
            let tag = &html[start..=end];
            let tag_lower = tag.to_lowercase();
            let content = tag_lower.find("content=")? + "content=".len();
            let quote = tag_lower.as_bytes().get(content).copied()? as char;
            if quote != '\"' && quote != '\'' {
                continue;
            }
            let value_start = content + 1;
            let value_end = tag_lower[value_start..].find(quote)? + value_start;
            let value = tag[value_start..value_end].trim();
            if !value.is_empty() {
                return Some(value.chars().take(500).collect());
            }
        }
    }
    None
}

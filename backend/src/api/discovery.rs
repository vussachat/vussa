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
pub(crate) async fn metrics(
    State(state): State<Arc<AppState>>,
) -> ([(HeaderName, HeaderValue); 1], String) {
    ::metrics::gauge!("vussa_active_websockets")
        .set(ACTIVE_WEBSOCKETS.load(Ordering::Relaxed) as f64);
    if let Ok(pending) = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM outbox_events WHERE published_at IS NULL",
    )
    .fetch_one(&state.database)
    .await
    {
        ::metrics::gauge!("vussa_outbox_pending").set(pending as f64);
    }
    if let Ok(pending) = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM notification_deliveries WHERE sent_at IS NULL",
    )
    .fetch_one(&state.database)
    .await
    {
        ::metrics::gauge!("vussa_notification_deliveries_pending").set(pending as f64);
    }
    let body = state.prometheus.render();
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
    Query(query): Query<UserSearchQuery>,
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
    Query(query): Query<MessageSearchQuery>,
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
        .map(|row| -> Result<_, sqlx::Error> {
            let message = ChatMessage::try_from_row(&row)?;
            Ok(MessageSearchResult {
                snippet: message.text.clone(),
                highlighted: row.try_get("highlighted")?,
                message,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
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
        .connect_timeout(Duration::from_secs(2))
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
    let metadata = preview_metadata(&html, &url);
    Ok(Json(serde_json::json!({
        "url": url.as_str(),
        "title": metadata.title,
        "description": metadata.description,
        "image_url": metadata.image_url,
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

pub(crate) struct PreviewMetadata {
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) image_url: Option<String>,
}

pub(crate) fn preview_metadata(html: &str, base_url: &reqwest::Url) -> PreviewMetadata {
    use scraper::{Html, Selector};

    let document = Html::parse_document(html);
    let title_selector = Selector::parse("title").expect("constant selector is valid");
    let meta_selector = Selector::parse("meta").expect("constant selector is valid");
    let title = document
        .select(&title_selector)
        .next()
        .map(|node| node.text().collect::<Vec<_>>().join(" "))
        .and_then(|value| bounded_text(&value, 300));
    let mut description = None;
    let mut image = None;
    for element in document.select(&meta_selector) {
        let attributes = element.value();
        let key = attributes
            .attr("property")
            .or_else(|| attributes.attr("name"))
            .unwrap_or_default()
            .to_ascii_lowercase();
        let Some(content) = attributes.attr("content") else {
            continue;
        };
        if description.is_none() && matches!(key.as_str(), "description" | "og:description") {
            description = bounded_text(content, 500);
        }
        if image.is_none() && key == "og:image" {
            image = base_url
                .join(content.trim())
                .ok()
                .filter(|url| matches!(url.scheme(), "http" | "https"))
                .map(|url| url.to_string());
        }
    }
    PreviewMetadata {
        title,
        description,
        image_url: image,
    }
}

fn bounded_text(value: &str, limit: usize) -> Option<String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    (!normalized.is_empty()).then(|| normalized.chars().take(limit).collect())
}

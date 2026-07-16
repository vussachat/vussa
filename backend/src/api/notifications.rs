use super::*;

pub(crate) async fn list_notifications(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
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
    let now = clock.now_millis() as i64;
    let mut recipients = Vec::with_capacity(rows.len());
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
        recipients.push((
            Uuid::now_v7(),
            row.get::<Uuid, _>("id"),
            row.get::<Uuid, _>("channel_id"),
            row.get::<String, _>("email"),
            kind.to_string(),
            body,
            row.get::<bool, _>("email_enabled"),
            row.get::<bool, _>("browser_enabled"),
        ));
    }
    if recipients.is_empty() {
        return Ok(());
    }

    let mut transaction = database.begin().await?;
    let notification_rows = sqlx::query(
        r#"
        INSERT INTO notifications
            (id, user_id, actor_user_id, kind, message_id, channel_id, body, created_at)
        SELECT input.id, input.user_id, $7, input.kind, $8, input.channel_id, input.body, $9
        FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::text[])
            AS input(id, user_id, channel_id, email, kind, body)
        ON CONFLICT (user_id, message_id, kind)
        DO UPDATE SET body = EXCLUDED.body
        RETURNING id, user_id, kind
        "#,
    )
    .bind(recipients.iter().map(|item| item.0).collect::<Vec<_>>())
    .bind(recipients.iter().map(|item| item.1).collect::<Vec<_>>())
    .bind(recipients.iter().map(|item| item.2).collect::<Vec<_>>())
    .bind(
        recipients
            .iter()
            .map(|item| item.3.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        recipients
            .iter()
            .map(|item| item.4.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        recipients
            .iter()
            .map(|item| item.5.clone())
            .collect::<Vec<_>>(),
    )
    .bind(actor.id)
    .bind(message.id)
    .bind(message.created_at as i64)
    .fetch_all(&mut *transaction)
    .await?;

    let notification_ids = notification_rows
        .into_iter()
        .map(|row| {
            (
                (row.get::<Uuid, _>("user_id"), row.get::<String, _>("kind")),
                row.get::<Uuid, _>("id"),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut delivery_ids = Vec::new();
    let mut delivered_notifications = Vec::new();
    let mut delivered_users = Vec::new();
    let mut channels = Vec::new();
    let mut emails = Vec::new();
    let mut kinds = Vec::new();
    let mut bodies = Vec::new();
    for (_, user_id, _, email, kind, body, email_enabled, browser_enabled) in recipients {
        let Some(notification_id) = notification_ids.get(&(user_id, kind.clone())).copied() else {
            continue;
        };
        for channel in [
            email_enabled.then_some("email"),
            browser_enabled.then_some("browser"),
        ]
        .into_iter()
        .flatten()
        {
            delivery_ids.push(Uuid::now_v7());
            delivered_notifications.push(notification_id);
            delivered_users.push(user_id);
            channels.push(channel.to_string());
            emails.push(email.clone());
            kinds.push(kind.clone());
            bodies.push(body.clone());
        }
    }
    if !delivery_ids.is_empty() {
        let delivery_times = vec![now; delivered_users.len()];
        sqlx::query(
            r#"
            INSERT INTO notification_deliveries
                (id, notification_id, user_id, channel, email, kind, body, next_attempt_at)
            SELECT *
            FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::bigint[])
            ON CONFLICT (notification_id, channel) DO NOTHING
            "#,
        )
        .bind(delivery_ids)
        .bind(delivered_notifications)
        .bind(delivered_users)
        .bind(channels)
        .bind(emails)
        .bind(kinds)
        .bind(bodies)
        .bind(delivery_times)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
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

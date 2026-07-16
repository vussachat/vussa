use super::*;

pub(crate) async fn load_thread(
    database: &PgPool,
    channel: &str,
    root_message_id: Uuid,
    before: Option<(u64, Uuid)>,
    limit: i64,
) -> Result<Vec<ChatMessage>, AppError> {
    let rows = sqlx::query("SELECT m.id,c.name AS channel,m.username,CASE WHEN m.deleted_at IS NULL THEN m.text ELSE '' END AS text,m.created_at,m.edited,m.deleted_at IS NOT NULL AS deleted,m.root_message_id,(SELECT COUNT(*) FROM messages replies WHERE replies.root_message_id=m.id) AS reply_count,m.metadata,m.mentions,m.client_id,COALESCE(ARRAY(SELECT mf.file_id FROM message_files mf WHERE mf.message_id=m.id),ARRAY[]::uuid[]) AS file_ids FROM messages m JOIN channels c ON c.id=m.channel_id WHERE c.name=$1 AND c.deleted_at IS NULL AND m.root_message_id=$2 AND ($3::bigint IS NULL OR (m.created_at,m.id)<($3,$4)) ORDER BY m.created_at DESC,m.id DESC LIMIT $5")
        .bind(channel).bind(root_message_id).bind(before.map(|v| v.0 as i64)).bind(before.map(|v| v.1)).bind(limit)
        .fetch_all(database).await?;
    let mut messages = rows
        .iter()
        .map(ChatMessage::try_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    messages.reverse();
    Ok(messages)
}

pub(crate) fn normalize_emoji(raw: &str) -> Result<String, AppError> {
    let emoji = raw.trim();
    if emoji.is_empty() || emoji.len() > 64 || emoji.chars().any(char::is_control) {
        return Err(AppError::bad_request(
            "emoji must be 1–64 printable characters",
        ));
    }
    Ok(emoji.to_string())
}

pub(crate) async fn broadcast_reaction(
    valkey: &ValkeyPool,
    database: &PgPool,
    channel: &str,
    message_id: Uuid,
    emoji: &str,
) -> Result<(), AppError> {
    let rows = sqlx::query(
        "SELECT user_id FROM message_reactions WHERE message_id=$1 AND emoji=$2 ORDER BY user_id",
    )
    .bind(message_id)
    .bind(emoji)
    .fetch_all(database)
    .await?;
    let reaction = ReactionSummary {
        message_id,
        emoji: emoji.to_string(),
        user_ids: rows.into_iter().map(|row| row.get("user_id")).collect(),
    };
    broadcast(
        valkey,
        channel,
        &ServerEvent::ReactionUpdated {
            channel: channel.to_string(),
            reaction,
        },
    )
    .await
}

pub(crate) fn history_key(channel: &str) -> String {
    format!("chat:history:{channel}:messages")
}

pub(crate) fn history_order_key(channel: &str) -> String {
    format!("chat:history:{channel}:order")
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) async fn send_joined_with_history(
    socket: &mut WebSocket,
    channel: &str,
    client: &ValkeyPool,
    repository: &dyn ChatRepository,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::Joined {
            name: channel.to_string(),
        },
    )
    .await?;
    send_history_page(socket, channel, client, repository, None).await
}

pub(crate) async fn send_history_page(
    socket: &mut WebSocket,
    channel: &str,
    client: &ValkeyPool,
    repository: &dyn ChatRepository,
    before: Option<(u64, Uuid)>,
) -> Result<(), AppError> {
    let cache_before = before.map(|(created_at, id)| (created_at as isize, id));
    let cache_limit = if before.is_none() {
        HOT_HISTORY_LIMIT
    } else {
        HISTORY_PAGE_SIZE
    };
    let mut messages = load_hot_history_before(client, channel, cache_before).await?;
    let mut source = HistorySource::Cache;
    let mut has_more = messages.len() >= cache_limit;
    if messages.len() < cache_limit {
        source = HistorySource::Database;
        let database_messages = repository
            .load_messages(
                channel,
                before.map(|(created_at, id)| (created_at as i64, id)),
                (cache_limit + 1) as i64,
            )
            .await?;
        has_more = database_messages.len() > cache_limit;
        if before.is_none() {
            let cache_start = database_messages.len().saturating_sub(HOT_HISTORY_LIMIT);
            hydrate_hot_history(client, channel, &database_messages[cache_start..]).await?;
        }
        messages = database_messages;
    }
    messages.sort_by_key(|message| (message.created_at, message.id));
    if before.is_none() && messages.len() > HISTORY_PAGE_SIZE {
        messages = messages.split_off(messages.len() - HISTORY_PAGE_SIZE);
    } else if before.is_some() && messages.len() > HISTORY_PAGE_SIZE {
        messages.truncate(HISTORY_PAGE_SIZE);
    }
    let event = if before.is_some() {
        ServerEvent::HistoryPage {
            channel: channel.to_string(),
            messages,
            source,
            has_more,
        }
    } else {
        ServerEvent::History {
            channel: channel.to_string(),
            messages,
            source,
            has_more,
        }
    };
    send_event(socket, &event).await
}

pub(crate) async fn load_hot_history(
    valkey: &ValkeyPool,
    channel: &str,
) -> Result<Vec<ChatMessage>, AppError> {
    let mut connection = valkey.connection()?;
    let ids: Vec<String> = connection
        .zrange(
            history_order_key(channel),
            -(HOT_HISTORY_LIMIT as isize),
            -1,
        )
        .await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let payloads: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
        .arg(history_key(channel))
        .arg(&ids)
        .query_async(&mut connection)
        .await?;
    let messages = payloads
        .into_iter()
        .flatten()
        .filter_map(|payload| bitcode::decode::<StoredMessage>(&payload).ok())
        .map(StoredMessage::into_message)
        .collect();
    Ok(messages)
}

pub(crate) async fn load_hot_history_before(
    client: &ValkeyPool,
    channel: &str,
    before: Option<(isize, Uuid)>,
) -> Result<Vec<ChatMessage>, AppError> {
    if before.is_none() {
        return load_hot_history(client, channel).await;
    }
    let (created_at, id) = before.expect("checked above");
    let mut connection = client.connection()?;
    let ids: Vec<String> = redis::cmd("ZREVRANGEBYSCORE")
        .arg(history_order_key(channel))
        .arg(created_at)
        .arg("-inf")
        .arg("LIMIT")
        .arg(0)
        .arg(HOT_HISTORY_LIMIT)
        .query_async(&mut connection)
        .await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let payloads: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
        .arg(history_key(channel))
        .arg(ids)
        .query_async(&mut connection)
        .await?;
    let mut messages = payloads
        .into_iter()
        .flatten()
        .filter_map(|payload| bitcode::decode::<StoredMessage>(&payload).ok())
        .map(StoredMessage::into_message)
        .filter(|message| before_history_cursor(message, created_at, id))
        .collect::<Vec<_>>();
    messages.sort_by_key(|message| (message.created_at, message.id));
    if messages.len() > HISTORY_PAGE_SIZE {
        messages = messages.split_off(messages.len() - HISTORY_PAGE_SIZE);
    }
    Ok(messages)
}

fn before_history_cursor(message: &ChatMessage, created_at: isize, id: Uuid) -> bool {
    (message.created_at as isize, message.id) < (created_at, id)
}

pub(crate) async fn hydrate_hot_history(
    valkey: &ValkeyPool,
    channel: &str,
    messages: &[ChatMessage],
) -> Result<(), AppError> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut connection = valkey.connection()?;
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    for message in messages {
        let record = bitcode::encode(&StoredMessage::from_message(message.clone(), Uuid::nil()));
        pipeline
            .cmd("HSET")
            .arg(history_key(channel))
            .arg(message.id.to_string())
            .arg(record)
            .ignore()
            .cmd("ZADD")
            .arg(history_order_key(channel))
            .arg(message.created_at)
            .arg(message.id.to_string())
            .ignore();
    }
    pipeline.query_async::<()>(&mut connection).await?;
    Ok(())
}

pub(crate) async fn store_message(
    valkey: &ValkeyPool,
    message: &ChatMessage,
    owner_session: Uuid,
) -> Result<(), AppError> {
    let record = bitcode::encode(&StoredMessage::from_message(message.clone(), owner_session));
    let mut connection = valkey.connection()?;
    redis::pipe()
        .atomic()
        .cmd("HSET")
        .arg(history_key(&message.channel))
        .arg(message.id.to_string())
        .arg(record)
        .ignore()
        .cmd("ZADD")
        .arg(history_order_key(&message.channel))
        .arg(message.created_at)
        .arg(message.id.to_string())
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;

    let old_ids: Vec<String> = connection
        .zrange(
            history_order_key(&message.channel),
            0,
            -((HOT_HISTORY_LIMIT + 1) as isize),
        )
        .await?;
    if !old_ids.is_empty() {
        redis::pipe()
            .cmd("ZREM")
            .arg(history_order_key(&message.channel))
            .arg(&old_ids)
            .ignore()
            .cmd("HDEL")
            .arg(history_key(&message.channel))
            .arg(&old_ids)
            .ignore()
            .query_async::<()>(&mut connection)
            .await?;
    }
    Ok(())
}

pub(crate) async fn update_hot_message(
    client: &ValkeyPool,
    message: &ChatMessage,
) -> Result<(), AppError> {
    store_message(client, message, Uuid::nil()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(created_at: u64, id: Uuid) -> ChatMessage {
        ChatMessage {
            id,
            channel: "main".into(),
            username: "user".into(),
            text: "text".into(),
            created_at,
            edited: false,
            deleted: false,
            root_message_id: None,
            reply_count: 0,
            metadata: serde_json::json!({}),
            mentions: Vec::new(),
            client_id: None,
            file_ids: Vec::new(),
        }
    }

    #[test]
    fn history_cursor_preserves_uuid_tie_breaking() {
        let lower = Uuid::from_u128(1);
        let cursor = Uuid::from_u128(2);
        assert!(before_history_cursor(&message(100, lower), 100, cursor));
        assert!(!before_history_cursor(&message(100, cursor), 100, cursor));
        assert!(!before_history_cursor(&message(101, lower), 100, cursor));
    }
}

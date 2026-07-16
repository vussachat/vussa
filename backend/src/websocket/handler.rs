use super::*;

pub(crate) async fn websocket(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, AppError> {
    if !websocket_origin_allowed(
        &headers,
        env::var("CORS_ORIGIN")
            .ok()
            .filter(|origin| !origin.trim().is_empty())
            .as_deref(),
    ) {
        return Err(AppError::forbidden("websocket origin is not allowed"));
    }
    let session = load_session(&headers, &state.valkey).await?;
    ensure_channel_access(&state.database, MAIN_CHANNEL, session.user.id).await?;
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, session)))
}

fn websocket_origin_allowed(headers: &HeaderMap, configured_origin: Option<&str>) -> bool {
    let Some(origin) = headers.get("origin").and_then(|value| value.to_str().ok()) else {
        return true;
    };
    if let Some(configured_origin) = configured_origin {
        return origin == configured_origin;
    }
    let Some(host) = headers.get("host").and_then(|value| value.to_str().ok()) else {
        return false;
    };
    origin == format!("http://{host}") || origin == format!("https://{host}")
}

struct ActiveWebSocket;

impl Drop for ActiveWebSocket {
    fn drop(&mut self) {
        ACTIVE_WEBSOCKETS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, session: Session) {
    ACTIVE_WEBSOCKETS.fetch_add(1, Ordering::Relaxed);
    let _active_websocket = ActiveWebSocket;
    let session_id = session.id;
    let username = session.user.username.clone();
    let participant = participant_for_user(&state.database, &session.user).await;
    let mut channel = MAIN_CHANNEL.to_string();

    if send_event(
        &mut socket,
        &ServerEvent::Welcome {
            username: username.clone(),
        },
    )
    .await
    .is_err()
        || send_channels(&mut socket, state.repository.as_ref(), session.user.id)
            .await
            .is_err()
        || send_private_conversations(&mut socket, &state.database, session.user.id)
            .await
            .is_err()
    {
        return;
    }

    let mut control_rx = state.rooms.subscribe_control();
    let mut room_rx = match state.rooms.subscribe(&channel).await {
        Ok(receiver) => receiver,
        Err(error) => {
            error!(?error, "could not subscribe to main room");
            return;
        }
    };
    if send_joined_with_history(
        &mut socket,
        &channel,
        &state.valkey,
        state.repository.as_ref(),
    )
    .await
    .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    if send_event(
        &mut socket,
        &ServerEvent::Members {
            channel: channel.clone(),
            members: channel_members(&state.database, &channel)
                .await
                .unwrap_or_default(),
        },
    )
    .await
    .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    if refresh_presence(&state.valkey, &channel, &participant)
        .await
        .is_err()
        || sync_presence(&mut socket, &state.valkey, &channel)
            .await
            .is_err()
    {
        state.rooms.release(&channel).await;
        return;
    }
    let _ = broadcast(
        &state.valkey,
        &channel,
        &ServerEvent::ParticipantJoined {
            channel: channel.clone(),
            participant: participant.clone(),
        },
    )
    .await;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)), if SHUTTING_DOWN.load(Ordering::Acquire) => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
            Some(result) = socket.next() => {
                match result {
                    Ok(Message::Text(text)) => {
                        let previous_channel = channel.clone();
                        match handle_client_event(&mut socket, &mut channel, &username, &session.user, session_id, &state, &text).await {
                            Ok(Some(new_channel)) => {
                                state.rooms.release(&previous_channel).await;
                                if let Err(error) = remove_presence(&state.valkey, &previous_channel, session.user.id).await {
                                    tracing::debug!(?error, channel = %previous_channel, user_id = %session.user.id, "presence cleanup on channel switch failed");
                                }
                                let _ = broadcast(&state.valkey, &previous_channel, &ServerEvent::ParticipantLeft { channel: previous_channel.clone(), user_id: session.user.id }).await;
                                channel = new_channel;
                                match state.rooms.subscribe(&channel).await {
                                    Ok(receiver) => {
                                        room_rx = receiver;
                                        if send_joined_with_history(&mut socket, &channel, &state.valkey, state.repository.as_ref()).await.is_err() {
                                            break;
                                        }
                                        let _ = send_event(&mut socket, &ServerEvent::Members { channel: channel.clone(), members: channel_members(&state.database, &channel).await.unwrap_or_default() }).await;
                                        let participant = participant_for_user(&state.database, &session.user).await;
                                        let _ = refresh_presence(&state.valkey, &channel, &participant).await;
                                        let _ = sync_presence(&mut socket, &state.valkey, &channel).await;
                                        let _ = broadcast(&state.valkey, &channel, &ServerEvent::ParticipantJoined { channel: channel.clone(), participant: participant.clone() }).await;
                                    }
                                    Err(error) => {
                                        let _ = send_event(&mut socket, &ServerEvent::Error { message: error.to_string() }).await;
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                let _ = send_event(&mut socket, &ServerEvent::Error { message: error.to_string() }).await;
                                if error.status == StatusCode::UNAUTHORIZED {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
            Ok(message) = control_rx.recv() => {
                if socket.send(message).await.is_err() { break; }
            }
            Ok(message) = room_rx.recv() => {
                if socket.send(message).await.is_err() { break; }
            }
            else => break,
        }
    }
    if let Err(error) = remove_presence(&state.valkey, &channel, session.user.id).await {
        tracing::debug!(?error, %channel, user_id = %session.user.id, "presence cleanup on disconnect failed");
    }
    let _ = broadcast(
        &state.valkey,
        &channel,
        &ServerEvent::ParticipantLeft {
            channel: channel.clone(),
            user_id: session.user.id,
        },
    )
    .await;
    state.rooms.release(&channel).await;
}

pub(crate) async fn handle_client_event(
    socket: &mut WebSocket,
    channel: &mut String,
    username: &str,
    user: &AuthUser,
    session_id: Uuid,
    state: &AppState,
    text: &str,
) -> Result<Option<String>, AppError> {
    let event: ClientEvent =
        serde_json::from_str(text).map_err(|_| AppError::bad_request("invalid event"))?;
    let mut switch_to = None;
    match event {
        ClientEvent::ListChannels => {
            send_channels(socket, state.repository.as_ref(), user.id).await?;
            send_private_conversations(socket, &state.database, user.id).await?;
        }
        ClientEvent::CreateChannel { name } => {
            create_public_channel(state, user, &name).await?;
        }
        ClientEvent::CreatePrivateChannel { name } => {
            create_private_conversation(state, user, &name).await?;
            send_private_conversations(socket, &state.database, user.id).await?;
        }
        ClientEvent::OpenDirect { user_id } => {
            let (_, direct_name) = open_direct(state, user, user_id).await?;
            send_private_conversations(socket, &state.database, user.id).await?;
            *channel = direct_name.clone();
            switch_to = Some(direct_name);
        }
        ClientEvent::InviteMember {
            channel: target,
            user_id,
        } => {
            invite_member(state, user, &target, user_id).await?;
        }
        ClientEvent::RemoveMember {
            channel: target,
            user_id,
        } => {
            remove_member(state, user, &target, user_id).await?;
        }
        ClientEvent::JoinChannel { name } => {
            let name = normalize_channel_name(&name).or_else(|_| {
                if name == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            ensure_channel_access(&state.database, &name, user.id).await?;
            *channel = name.clone();
            switch_to = Some(name.clone());
        }
        ClientEvent::DeleteChannel { name } => {
            let name = delete_channel_service(state, user, &name).await?;
            if *channel == name {
                *channel = MAIN_CHANNEL.to_string();
                switch_to = Some(MAIN_CHANNEL.to_string());
            }
        }
        ClientEvent::SendMessage {
            text,
            root_message_id,
            client_id,
            file_ids,
        } => {
            require_permission(user, "chat:write")?;
            enforce_rate_limit(&state.valkey, &message_rate_key(user.id), 60, 60).await?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            ensure_channel_posting_access(&state.database, channel, user.id, &user.permissions)
                .await?;
            let text = text.trim();
            if text.is_empty() || text.len() > 2000 {
                return Err(AppError::bad_request("message must be 1–2000 characters"));
            }
            if file_ids.len() > 10 {
                return Err(AppError::bad_request(
                    "a message can contain at most 10 files",
                ));
            }
            if !file_ids.is_empty() {
                let owned_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE id=ANY($1) AND uploader_user_id=$2 AND deleted_at IS NULL")
                    .bind(&file_ids).bind(user.id).fetch_one(&state.database).await?;
                if owned_files != file_ids.len() as i64 {
                    return Err(AppError::forbidden("one or more files are not available"));
                }
            }
            let client_id = client_id.filter(|value| !value.trim().is_empty());
            if let Some(client_id) = client_id.as_deref()
                && let Some(existing) = state
                    .repository
                    .find_message_by_client_id(channel, session_id, client_id)
                    .await?
            {
                if existing.channel != channel.as_str()
                    || existing.text != text
                    || existing.root_message_id != root_message_id
                    || existing.file_ids != file_ids
                {
                    return Err(AppError::bad_request(
                        "client_id was already used for a different message",
                    ));
                }
                // The message may have been committed immediately before a
                // transient notification failure. Reconcile notifications on
                // every idempotent retry; the database uniqueness key makes
                // this safe for concurrent retries and already-complete rows.
                create_message_notifications(state, user, &existing).await?;
                send_event(socket, &ServerEvent::Message { message: existing }).await?;
                return Ok(switch_to);
            }
            let message = ChatMessage {
                id: Uuid::now_v7(),
                channel: channel.clone(),
                username: username.to_string(),
                text: text.to_string(),
                created_at: now_millis(),
                edited: false,
                deleted: false,
                root_message_id,
                reply_count: 0,
                metadata: serde_json::json!({
                    "root_message_id": root_message_id.map(|id| id.to_string()),
                    "file_ids": file_ids.clone(),
                    "mention_scope": mention_scope(text),
                    "online_user_ids": if mention_scope(text) == Some("here") {
                        list_presence(&state.valkey, channel)
                            .await
                            .unwrap_or_default()
                            .into_iter()
                            .filter(|participant| participant.user_id != user.id)
                            .map(|participant| participant.user_id.to_string())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    },
                }),
                mentions: extract_mentioned_usernames(text, &user.username),
                client_id,
                file_ids: file_ids.clone(),
            };
            if let Err(error) = state
                .repository
                .save_message(&message, session_id, user.id)
                .await
            {
                if let Some(client_id) = message.client_id.as_deref()
                    && let Some(existing) = state
                        .repository
                        .find_message_by_client_id(channel, session_id, client_id)
                        .await?
                {
                    if existing.channel != message.channel
                        || existing.text != message.text
                        || existing.root_message_id != message.root_message_id
                        || existing.file_ids != message.file_ids
                    {
                        return Err(AppError::bad_request(
                            "client_id was already used for a different message",
                        ));
                    }
                    create_message_notifications(state, user, &existing).await?;
                    send_event(socket, &ServerEvent::Message { message: existing }).await?;
                    return Ok(switch_to);
                }
                return Err(error.into());
            }
            if let Err(error) = create_message_notifications(state, user, &message).await {
                error!(?error, message_id = %message.id, "mention notification creation failed");
                if let Err(queue_error) = sqlx::query("INSERT INTO outbox_events (id,topic,payload,created_at) SELECT $1,'message.notifications',jsonb_build_object('actor_id',$2::text,'message_id',$3::text),$4 WHERE NOT EXISTS (SELECT 1 FROM outbox_events WHERE topic='message.notifications' AND payload->>'message_id'=$3::text AND published_at IS NULL)")
                    .bind(Uuid::now_v7())
                    .bind(user.id)
                    .bind(message.id)
                    .bind(now_millis() as i64)
                    .execute(&state.database)
                    .await
                {
                    error!(?queue_error, message_id = %message.id, "failed to queue notification retry");
                }
            }
            store_message(&state.valkey, &message, session_id).await?;
            let event = ServerEvent::Message { message };
            broadcast(&state.valkey, channel, &event).await?;
        }
        ClientEvent::EditMessage { id, text } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let text = text.trim();
            if text.is_empty() || text.len() > 2000 {
                return Err(AppError::bad_request("message must be 1–2000 characters"));
            }
            let message = state
                .repository
                .edit_message(channel, id, session_id, text)
                .await?;
            update_hot_message(&state.valkey, &message).await?;
            broadcast(
                &state.valkey,
                channel,
                &ServerEvent::MessageUpdated { message },
            )
            .await?;
        }
        ClientEvent::DeleteMessage { id } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let message = state
                .repository
                .delete_message(
                    channel,
                    id,
                    session_id,
                    user.permissions
                        .iter()
                        .any(|permission| permission == "chat:moderate"),
                )
                .await?;
            update_hot_message(&state.valkey, &message).await?;
            broadcast(
                &state.valkey,
                channel,
                &ServerEvent::MessageUpdated { message },
            )
            .await?;
        }
        ClientEvent::AddReaction { message_id, emoji } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let emoji = normalize_emoji(&emoji)?;
            sqlx::query("INSERT INTO message_reactions (message_id,user_id,emoji,created_at) SELECT m.id,$1,$2,$3 FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$4 AND c.name=$5 AND c.deleted_at IS NULL ON CONFLICT DO NOTHING")
                .bind(user.id).bind(&emoji).bind(now_millis() as i64).bind(message_id).bind(channel.as_str())
                .execute(&state.database).await?;
            broadcast_reaction(&state.valkey, &state.database, channel, message_id, &emoji).await?;
        }
        ClientEvent::RemoveReaction { message_id, emoji } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let emoji = normalize_emoji(&emoji)?;
            sqlx::query("DELETE FROM message_reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3 AND EXISTS (SELECT 1 FROM messages m JOIN channels c ON c.id=m.channel_id WHERE m.id=$1 AND c.name=$4 AND c.deleted_at IS NULL)")
                .bind(message_id).bind(user.id).bind(&emoji).bind(channel.as_str())
                .execute(&state.database).await?;
            broadcast_reaction(&state.valkey, &state.database, channel, message_id, &emoji).await?;
        }
        ClientEvent::Typing { typing } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            broadcast(
                &state.valkey,
                channel,
                &ServerEvent::Typing {
                    channel: channel.clone(),
                    user_id: user.id,
                    username: username.to_string(),
                    typing,
                },
            )
            .await?;
        }
        ClientEvent::MarkRead {
            message_id,
            created_at,
        } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            sqlx::query("INSERT INTO channel_reads (user_id,channel_id,last_read_created_at,last_read_message_id,updated_at) SELECT $1,id,$2,$3,$4 FROM channels WHERE name=$5 ON CONFLICT (user_id,channel_id) DO UPDATE SET last_read_created_at=GREATEST(channel_reads.last_read_created_at,EXCLUDED.last_read_created_at),last_read_message_id=CASE WHEN EXCLUDED.last_read_created_at >= channel_reads.last_read_created_at THEN EXCLUDED.last_read_message_id ELSE channel_reads.last_read_message_id END,updated_at=EXCLUDED.updated_at")
                .bind(user.id).bind(created_at as i64).bind(message_id).bind(now_millis() as i64).bind(channel.as_str())
                .execute(&state.database).await?;
            send_event(
                socket,
                &ServerEvent::ReadStateUpdated {
                    channel: channel.clone(),
                    user_id: user.id,
                    message_id,
                    created_at,
                },
            )
            .await?;
        }
        ClientEvent::LoadHistory {
            channel: requested_channel,
            before_created_at,
            before_id,
        } => {
            let requested_channel = normalize_channel_name(&requested_channel).or_else(|_| {
                if requested_channel == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            if requested_channel != *channel {
                // A scroll event from the previous view can arrive while a
                // channel switch is completing. The request is stale, not an
                // invalid user action, so discard it without surfacing an
                // error toast.
                return Ok(switch_to);
            }
            ensure_channel_access(&state.database, &requested_channel, user.id).await?;
            send_history_page(
                socket,
                &requested_channel,
                &state.valkey,
                state.repository.as_ref(),
                Some((before_created_at, before_id)),
            )
            .await?;
        }
        ClientEvent::LoadThread {
            message_id,
            before_created_at,
            before_id,
        } => {
            require_permission(user, "chat:write")?;
            ensure_channel_access(&state.database, channel, user.id).await?;
            let messages = load_thread(
                &state.database,
                channel,
                message_id,
                before_created_at.zip(before_id),
                HISTORY_PAGE_SIZE as i64,
            )
            .await?;
            let has_more = messages.len() == HISTORY_PAGE_SIZE;
            send_event(
                socket,
                &ServerEvent::ThreadHistory {
                    root_message_id: message_id,
                    messages,
                    has_more,
                },
            )
            .await?;
        }
        ClientEvent::Heartbeat => {
            let mut connection = state.valkey.connection()?;
            let active: bool = connection.exists(session_key(session_id)).await?;
            if !active {
                return Err(AppError::unauthorized("session expired"));
            }
            let participant = participant_for_user(&state.database, user).await;
            // Heartbeats are best-effort: a transient sync write must not be
            // reported as a rejected chat action. The next heartbeat or
            // reconnect will reconcile the channel state.
            // Heartbeats stay entirely in Valkey. Channel access was checked
            // when the socket joined, and presence reconciliation is sent on
            // connect/channel switch rather than on every refresh.
            let _ = refresh_presence(&state.valkey, channel, &participant).await;
            let _ = broadcast(
                &state.valkey,
                channel,
                &ServerEvent::ParticipantJoined {
                    channel: channel.clone(),
                    participant,
                },
            )
            .await;
        }
    }
    Ok(switch_to)
}

async fn participant_for_user(database: &PgPool, user: &AuthUser) -> Participant {
    let profile = sqlx::query(
        "SELECT display_name, CASE WHEN status_expires_at IS NULL OR status_expires_at > $2 THEN custom_status ELSE '' END AS custom_status FROM users WHERE id=$1 AND deleted_at IS NULL",
    )
    .bind(user.id)
    .bind(crate::now_millis() as i64)
    .fetch_optional(database)
    .await
    .ok()
    .flatten();
    Participant {
        user_id: user.id,
        username: user.username.clone(),
        display_name: profile
            .as_ref()
            .map(|row| row.get("display_name"))
            .unwrap_or_else(|| user.username.clone()),
        custom_status: profile
            .as_ref()
            .map(|row| row.get("custom_status"))
            .unwrap_or_default(),
        roles: user.roles.clone(),
        online: true,
    }
}

pub(crate) async fn send_channels(
    socket: &mut WebSocket,
    repository: &dyn ChatRepository,
    user_id: Uuid,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::Channels {
            channels: repository.list_channels(user_id).await?,
        },
    )
    .await
}

pub(crate) async fn send_private_conversations(
    socket: &mut WebSocket,
    pool: &PgPool,
    user_id: Uuid,
) -> Result<(), AppError> {
    let conversations = list_visible_conversations(pool, user_id)
        .await?
        .into_iter()
        .filter(|conversation| conversation.kind != "public")
        .collect();
    send_event(socket, &ServerEvent::PrivateConversations { conversations }).await
}

#[cfg(test)]
mod tests {
    use super::websocket_origin_allowed;
    use crate::next_room_retry_delay;
    use axum::http::{HeaderMap, HeaderValue};
    use std::time::Duration;

    #[test]
    fn websocket_origin_requires_same_host_or_configured_origin() {
        let mut same_host = HeaderMap::new();
        same_host.insert("origin", HeaderValue::from_static("https://chat.example"));
        same_host.insert("host", HeaderValue::from_static("chat.example"));
        assert!(websocket_origin_allowed(&same_host, None));

        let mut cross_origin = same_host.clone();
        cross_origin.insert("origin", HeaderValue::from_static("https://evil.example"));
        assert!(!websocket_origin_allowed(&cross_origin, None));
        assert!(websocket_origin_allowed(
            &cross_origin,
            Some("https://evil.example")
        ));
    }

    #[test]
    fn websocket_clients_without_origin_remain_supported() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("chat.example"));
        assert!(websocket_origin_allowed(&headers, None));
    }

    #[test]
    fn room_reconnect_backoff_is_bounded() {
        assert_eq!(
            next_room_retry_delay(Duration::from_millis(250)),
            Duration::from_millis(500)
        );
        assert_eq!(
            next_room_retry_delay(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
    }
}

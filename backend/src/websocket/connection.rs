use super::*;

pub(crate) struct RoomManager {
    commands: mpsc::Sender<ManagerCommand>,
    control: broadcast::Sender<Message>,
}

enum ManagerCommand {
    Subscribe {
        channel: String,
        reply: oneshot::Sender<Result<broadcast::Receiver<Message>, String>>,
    },
    Release {
        channel: String,
    },
}

struct RoomEntry {
    sender: broadcast::Sender<Message>,
    clients: usize,
}

impl RoomManager {
    pub(crate) async fn start(client: &redis::Client) -> redis::RedisResult<Arc<Self>> {
        let room_event_capacity = env::var("WS_ROOM_EVENT_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4096)
            .clamp(128, 65_536);
        let pubsub = client.get_async_pubsub().await?;
        let (mut sink, stream) = pubsub.split();
        sink.subscribe("_control").await?;

        let (commands, command_rx) = mpsc::channel(128);
        let (control, _) = broadcast::channel(128);
        tokio::spawn(run_room_manager(
            client.clone(),
            sink,
            stream,
            command_rx,
            control.clone(),
            room_event_capacity,
        ));
        info!(room_event_capacity, "WebSocket room buffer configured");

        Ok(Arc::new(Self { commands, control }))
    }

    pub(crate) fn subscribe_control(&self) -> broadcast::Receiver<Message> {
        self.control.subscribe()
    }

    pub(crate) async fn subscribe(
        &self,
        channel: &str,
    ) -> Result<broadcast::Receiver<Message>, AppError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ManagerCommand::Subscribe {
                channel: channel.to_string(),
                reply,
            })
            .await
            .map_err(|_| AppError::bad_request("room manager is unavailable"))?;
        response
            .await
            .map_err(|_| AppError::bad_request("room manager is unavailable"))?
            .map_err(AppError::bad_request)
    }

    pub(crate) async fn release(&self, channel: &str) {
        let _ = self
            .commands
            .send(ManagerCommand::Release {
                channel: channel.to_string(),
            })
            .await;
    }
}

async fn run_room_manager(
    client: redis::Client,
    mut sink: redis::aio::PubSubSink,
    mut stream: redis::aio::PubSubStream,
    mut commands: mpsc::Receiver<ManagerCommand>,
    control: broadcast::Sender<Message>,
    room_event_capacity: usize,
) {
    let mut rooms: HashMap<String, RoomEntry> = HashMap::new();

    loop {
        let mut connection_lost = false;
        let mut command_closed = false;
        loop {
            tokio::select! {
                command = commands.recv() => {
                    match command {
                        Some(ManagerCommand::Subscribe { channel, reply }) => {
                            let entry = if let Some(entry) = rooms.get_mut(&channel) {
                                entry.clients += 1;
                                entry.sender.clone()
                            } else {
                                let sender = broadcast::channel(room_event_capacity).0;
                                if sink.subscribe(room_key(&channel)).await.is_err() {
                                    let _ = reply.send(Err("Valkey room subscription failed".to_string()));
                                    continue;
                                }
                                rooms.insert(channel.clone(), RoomEntry { sender: sender.clone(), clients: 1 });
                                sender
                            };
                            let _ = reply.send(Ok(entry.subscribe()));
                        }
                        Some(ManagerCommand::Release { channel }) => {
                            let should_unsubscribe = if let Some(entry) = rooms.get_mut(&channel) {
                                entry.clients = entry.clients.saturating_sub(1);
                                entry.clients == 0
                            } else {
                                false
                            };
                            if should_unsubscribe {
                                rooms.remove(&channel);
                                let _ = sink.unsubscribe(room_key(&channel)).await;
                            }
                        }
                        None => { command_closed = true; break; },
                    }
                }
                pubsub_message = stream.next() => {
                    match pubsub_message {
                        Some(pubsub_message) => {
                            let payload = pubsub_message.get_payload_bytes();
                            if let Ok(event) = serde_json::from_slice::<ServerEvent>(payload) {
                                // Redis already carries the canonical serialized
                                // event. Reuse one bytes-backed WebSocket frame for
                                // every local subscriber instead of serializing and
                                // allocating the same JSON once per connection.
                                let wire = Message::Text(
                                    String::from_utf8_lossy(payload).into_owned().into()
                                );
                                match &event {
                                    ServerEvent::Message { message } | ServerEvent::MessageUpdated { message } => {
                                        if let Some(entry) = rooms.get(&message.channel) {
                                            let _ = entry.sender.send(wire);
                                        }
                                    }
                                    ServerEvent::ReactionUpdated { channel, .. } => {
                                        if let Some(entry) = rooms.get(channel) {
                                            let _ = entry.sender.send(wire);
                                        }
                                    }
                                    ServerEvent::ChannelCreated { .. } | ServerEvent::ChannelDeleted { .. } => {
                                        let _ = control.send(wire);
                                    }
                                    ServerEvent::Participants { channel, .. }
                                    | ServerEvent::ParticipantJoined { channel, .. }
                                    | ServerEvent::ParticipantLeft { channel, .. }
                                    | ServerEvent::PresenceSync { channel, .. }
                                    | ServerEvent::Typing { channel, .. } => {
                                        if let Some(entry) = rooms.get(channel) {
                                            let _ = entry.sender.send(wire);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        None => { connection_lost = true; break; },
                    }
                }
            }
        }
        if command_closed || !connection_lost {
            break;
        }

        // Pub/Sub does not replay messages published while the connection is
        // down. Tell connected clients to reconnect so the WebSocket startup
        // path reloads the durable history before accepting new actions.
        let _ = control.send(Message::Text(
            r#"{"type":"error","message":"realtime connection interrupted"}"#.into(),
        ));

        let mut retry_delay = Duration::from_millis(250);
        loop {
            tokio::time::sleep(retry_delay).await;
            if let Ok(pubsub) = client.get_async_pubsub().await {
                let (mut candidate_sink, candidate_stream) = pubsub.split();
                if candidate_sink.subscribe("_control").await.is_err() {
                    retry_delay = next_room_retry_delay(retry_delay);
                    continue;
                }
                let mut restored = true;
                for channel in rooms.keys() {
                    if candidate_sink.subscribe(room_key(channel)).await.is_err() {
                        restored = false;
                        break;
                    }
                }
                if restored {
                    sink = candidate_sink;
                    stream = candidate_stream;
                    break;
                }
            }
            retry_delay = next_room_retry_delay(retry_delay);
        }
    }
}

fn next_room_retry_delay(current: Duration) -> Duration {
    (current * 2).min(Duration::from_secs(10))
}

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
                                let _ = remove_presence(&state.valkey, &previous_channel, session.user.id).await;
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
    let _ = remove_presence(&state.valkey, &channel, session.user.id).await;
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
            require_permission(user, "chat:write")?;
            ensure_not_globally_banned(&state.database, user.id).await?;
            let name = normalize_channel_name(&name)?;
            let mut tx = state.database.begin().await?;
            let channel_id = sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO channels (id,name,created_at) VALUES ($1,$2,$3)
                 ON CONFLICT (name) DO UPDATE SET deleted_at=NULL
                 WHERE channels.deleted_at IS NOT NULL
                 RETURNING id",
            )
            .bind(Uuid::now_v7())
            .bind(&name)
            .bind(now_millis() as i64)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::bad_request("channel already exists"))?;
            sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.created','channel',$3,$4)")
                .bind(Uuid::now_v7()).bind(user.id).bind(channel_id).bind(now_millis() as i64).execute(&mut *tx).await?;
            tx.commit().await?;
            publish_system_message(
                state,
                &name,
                format!("{} created this channel", user.username),
            )
            .await?;
            broadcast_control(&state.valkey, &ServerEvent::ChannelCreated { name }).await?;
        }
        ClientEvent::CreatePrivateChannel { name } => {
            require_permission(user, "chat:write")?;
            ensure_not_globally_banned(&state.database, user.id).await?;
            let name = normalize_channel_name(&name)?;
            let mut tx = state.database.begin().await?;
            let id = Uuid::now_v7();
            let now = now_millis() as i64;
            let result = sqlx::query("INSERT INTO channels (id,name,kind,owner_user_id,created_at) VALUES ($1,$2,'private',$3,$4) ON CONFLICT (name) DO NOTHING")
                .bind(id).bind(&name).bind(user.id).bind(now).execute(&mut *tx).await?;
            if result.rows_affected() == 0 {
                return Err(AppError::bad_request("channel already exists"));
            }
            sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'owner',$2,$3)")
                .bind(id).bind(user.id).bind(now).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.private_created','channel',$3,$4)")
                .bind(Uuid::now_v7()).bind(user.id).bind(id).bind(now).execute(&mut *tx).await?;
            tx.commit().await?;
            publish_system_message(
                state,
                &name,
                format!("{} created this private channel", user.username),
            )
            .await?;
            send_private_conversations(socket, &state.database, user.id).await?;
        }
        ClientEvent::OpenDirect { user_id } => {
            require_permission(user, "chat:write")?;
            ensure_not_globally_banned(&state.database, user.id).await?;
            if user_id == user.id {
                return Err(AppError::bad_request("you cannot message yourself"));
            }
            let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)").bind(user_id).fetch_one(&state.database).await?;
            if !target_exists {
                return Err(RepositoryError::NotFound.into());
            }
            let (first, second) = if user.id < user_id {
                (user.id, user_id)
            } else {
                (user_id, user.id)
            };
            let direct_key = format!("{first}:{second}");
            let mut tx = state.database.begin().await?;
            let now = now_millis() as i64;
            let id = Uuid::now_v7();
            let name = format!("dm_{id}");
            let inserted = sqlx::query("INSERT INTO channels (id,name,kind,direct_key,created_at) VALUES ($1,$2,'direct',$3,$4) ON CONFLICT (direct_key) WHERE kind='direct' AND deleted_at IS NULL DO NOTHING RETURNING id,name")
                .bind(id).bind(&name).bind(&direct_key).bind(now).fetch_optional(&mut *tx).await?;
            let (channel_id, direct_name, created) = if let Some(row) = inserted {
                (row.get::<Uuid, _>("id"), row.get::<String, _>("name"), true)
            } else {
                let row = sqlx::query("SELECT id,name FROM channels WHERE direct_key=$1 AND kind='direct' AND deleted_at IS NULL")
                    .bind(&direct_key).fetch_one(&mut *tx).await?;
                (
                    row.get::<Uuid, _>("id"),
                    row.get::<String, _>("name"),
                    false,
                )
            };
            for member in [first, second] {
                sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
                    .bind(channel_id).bind(member).bind(user.id).bind(now).execute(&mut *tx).await?;
            }
            if created {
                sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.direct_created','channel',$3,jsonb_build_object('user_id',$4),$5)")
                    .bind(Uuid::now_v7()).bind(user.id).bind(channel_id).bind(user_id).bind(now).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            send_private_conversations(socket, &state.database, user.id).await?;
            *channel = direct_name.clone();
            switch_to = Some(direct_name);
        }
        ClientEvent::InviteMember {
            channel: target,
            user_id,
        } => {
            require_permission(user, "chat:write")?;
            let channel_id = owned_private_channel(&state.database, &target, user.id).await?;
            let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)").bind(user_id).fetch_one(&state.database).await?;
            if !target_exists {
                return Err(RepositoryError::NotFound.into());
            }
            let result = sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
                .bind(channel_id).bind(user_id).bind(user.id).bind(now_millis() as i64).execute(&state.database).await?;
            if result.rows_affected() > 0 {
                let target_username: String =
                    sqlx::query_scalar("SELECT username FROM users WHERE id=$1")
                        .bind(user_id)
                        .fetch_one(&state.database)
                        .await?;
                publish_system_message(
                    state,
                    &target,
                    format!("{} invited {}", user.username, target_username),
                )
                .await?;
                let _ = broadcast(
                    &state.valkey,
                    &target,
                    &ServerEvent::Members {
                        channel: target.clone(),
                        members: channel_members(&state.database, &target).await?,
                    },
                )
                .await;
                sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_added','channel',$3,jsonb_build_object('user_id',$4),$5)")
                    .bind(Uuid::now_v7()).bind(user.id).bind(channel_id).bind(user_id).bind(now_millis() as i64).execute(&state.database).await?;
            }
        }
        ClientEvent::RemoveMember {
            channel: target,
            user_id,
        } => {
            require_permission(user, "chat:write")?;
            let channel_id = owned_private_channel(&state.database, &target, user.id).await?;
            let result = sqlx::query("DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2 AND membership_role <> 'owner'").bind(channel_id).bind(user_id).execute(&state.database).await?;
            if result.rows_affected() == 0 {
                return Err(RepositoryError::NotFound.into());
            }
            let target_username: String =
                sqlx::query_scalar("SELECT username FROM users WHERE id=$1")
                    .bind(user_id)
                    .fetch_optional(&state.database)
                    .await?
                    .unwrap_or_else(|| "a member".into());
            publish_system_message(
                state,
                &target,
                format!("{} removed {}", user.username, target_username),
            )
            .await?;
            let _ = broadcast(
                &state.valkey,
                &target,
                &ServerEvent::Members {
                    channel: target.clone(),
                    members: channel_members(&state.database, &target).await?,
                },
            )
            .await;
            sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,metadata,created_at) VALUES ($1,$2,'channel.member_removed','channel',$3,jsonb_build_object('user_id',$4),$5)")
                .bind(Uuid::now_v7()).bind(user.id).bind(channel_id).bind(user_id).bind(now_millis() as i64).execute(&state.database).await?;
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
            require_permission(user, "chat:moderate")?;
            let name = normalize_channel_name(&name).or_else(|_| {
                if name == MAIN_CHANNEL {
                    Ok(MAIN_CHANNEL.to_string())
                } else {
                    Err(AppError::bad_request("invalid channel"))
                }
            })?;
            if name == MAIN_CHANNEL {
                return Err(AppError::bad_request("the main channel cannot be removed"));
            }
            let mut tx = state.database.begin().await?;
            let channel_id = sqlx::query_scalar::<_, Uuid>(
                "UPDATE channels SET deleted_at=$1 WHERE name=$2 AND deleted_at IS NULL RETURNING id",
            )
            .bind(now_millis() as i64)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| AppError::bad_request("channel does not exist"))?;
            sqlx::query("INSERT INTO audit_events (id,actor_user_id,action,target_type,target_id,created_at) VALUES ($1,$2,'channel.deleted','channel',$3,$4)")
                .bind(Uuid::now_v7()).bind(user.id).bind(channel_id).bind(now_millis() as i64).execute(&mut *tx).await?;
            tx.commit().await?;
            broadcast_control(
                &state.valkey,
                &ServerEvent::ChannelDeleted { name: name.clone() },
            )
            .await?;
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
            enforce_rate_limit(&format!("chat:rate:message:{session_id}"), 60, 60).await?;
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
            let mut connection = valkey_commands()?;
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
        .into_iter()
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
        .collect::<Vec<_>>();
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
    valkey: &redis::Client,
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
    client: &redis::Client,
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
    client: &redis::Client,
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
    _client: &redis::Client,
    channel: &str,
) -> Result<Vec<ChatMessage>, AppError> {
    let mut connection = valkey_commands()?;
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
    client: &redis::Client,
    channel: &str,
    before: Option<(isize, Uuid)>,
) -> Result<Vec<ChatMessage>, AppError> {
    if before.is_none() {
        return load_hot_history(client, channel).await;
    }
    // The hot tier intentionally contains only the newest 300 messages. Read
    // the bounded tier once and compare the full (created_at, id) cursor in
    // Rust so messages sharing a millisecond are not skipped.
    let (created_at, id) = before.expect("checked above");
    let mut messages = load_hot_history(client, channel)
        .await?
        .into_iter()
        .filter(|message| (message.created_at as isize, message.id) < (created_at, id))
        .collect::<Vec<_>>();
    messages.sort_by_key(|message| (message.created_at, message.id));
    if messages.len() > HISTORY_PAGE_SIZE {
        messages = messages.split_off(messages.len() - HISTORY_PAGE_SIZE);
    }
    Ok(messages)
}

pub(crate) async fn hydrate_hot_history(
    _client: &redis::Client,
    channel: &str,
    messages: &[ChatMessage],
) -> Result<(), AppError> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut connection = valkey_commands()?;
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
    _client: &redis::Client,
    message: &ChatMessage,
    owner_session: Uuid,
) -> Result<(), AppError> {
    let record = bitcode::encode(&StoredMessage::from_message(message.clone(), owner_session));
    let mut connection = valkey_commands()?;
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
    client: &redis::Client,
    message: &ChatMessage,
) -> Result<(), AppError> {
    store_message(client, message, Uuid::nil()).await
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

pub(crate) async fn broadcast_control(
    client: &redis::Client,
    event: &ServerEvent,
) -> Result<(), AppError> {
    broadcast(client, "_control", event).await
}

pub(crate) fn room_key(channel: &str) -> String {
    format!("chat:room:{channel}")
}

pub(crate) fn presence_set_key(channel: &str) -> String {
    format!("chat:presence:{channel}:users")
}

pub(crate) fn presence_key(channel: &str, user_id: Uuid) -> String {
    format!("chat:presence:{channel}:{user_id}")
}

pub(crate) async fn refresh_presence(
    _client: &redis::Client,
    channel: &str,
    participant: &Participant,
) -> Result<(), AppError> {
    let mut connection = valkey_commands()?;
    let payload = serde_json::to_string(participant)
        .map_err(|_| AppError::bad_request("could not encode presence"))?;
    redis::pipe()
        .atomic()
        .cmd("SADD")
        .arg(presence_set_key(channel))
        .arg(participant.user_id.to_string())
        .ignore()
        .cmd("SETEX")
        .arg(presence_key(channel, participant.user_id))
        .arg(PRESENCE_TTL_SECONDS)
        .arg(payload)
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok(())
}

pub(crate) async fn remove_presence(
    _client: &redis::Client,
    channel: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    let mut connection = valkey_commands()?;
    redis::pipe()
        .atomic()
        .cmd("SREM")
        .arg(presence_set_key(channel))
        .arg(user_id.to_string())
        .ignore()
        .cmd("DEL")
        .arg(presence_key(channel, user_id))
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok(())
}

pub(crate) async fn list_presence(
    _client: &redis::Client,
    channel: &str,
) -> Result<Vec<Participant>, AppError> {
    let mut connection = valkey_commands()?;
    let ids: Vec<String> = connection.smembers(presence_set_key(channel)).await?;
    let mut participants = Vec::with_capacity(ids.len());
    for id in ids {
        let Ok(user_id) = Uuid::parse_str(&id) else {
            continue;
        };
        let key = presence_key(channel, user_id);
        let value: Option<String> = connection.get(&key).await?;
        match value.and_then(|value| serde_json::from_str::<Participant>(&value).ok()) {
            Some(participant) => participants.push(participant),
            None => {
                let _: usize = connection.srem(presence_set_key(channel), id).await?;
            }
        }
    }
    participants.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(participants)
}

pub(crate) async fn sync_presence(
    socket: &mut WebSocket,
    client: &redis::Client,
    channel: &str,
) -> Result<(), AppError> {
    send_event(
        socket,
        &ServerEvent::PresenceSync {
            channel: channel.to_string(),
            participants: list_presence(client, channel).await?,
        },
    )
    .await
}

pub(crate) async fn broadcast(
    _client: &redis::Client,
    channel: &str,
    event: &ServerEvent,
) -> Result<(), AppError> {
    let payload =
        serde_json::to_vec(event).map_err(|_| AppError::bad_request("could not encode event"))?;
    let mut connection = valkey_commands()?;
    let publish_channel = if channel == "_control" {
        channel.to_string()
    } else {
        format!("chat:room:{channel}")
    };
    let _: i32 = redis::cmd("PUBLISH")
        .arg(publish_channel)
        .arg(payload)
        .query_async(&mut connection)
        .await?;
    Ok(())
}

pub(crate) async fn send_event(
    socket: &mut WebSocket,
    event: &ServerEvent,
) -> Result<(), AppError> {
    let payload = serde_json::to_string(event)
        .map_err(|_| AppError::bad_request("could not encode event"))?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| AppError::bad_request("connection closed"))
}

#[cfg(test)]
mod tests {
    use super::{next_room_retry_delay, websocket_origin_allowed};
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

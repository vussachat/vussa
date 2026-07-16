use super::*;

pub(crate) struct RoomManager {
    commands: mpsc::Sender<ManagerCommand>,
    control: tokio::sync::broadcast::Sender<Message>,
}

enum ManagerCommand {
    Subscribe {
        channel: String,
        reply: oneshot::Sender<Result<tokio::sync::broadcast::Receiver<Message>, String>>,
    },
    Release {
        channel: String,
    },
}

struct RoomEntry {
    sender: tokio::sync::broadcast::Sender<Message>,
    clients: usize,
}

impl RoomManager {
    pub(crate) async fn start(valkey: &ValkeyPool) -> redis::RedisResult<Arc<Self>> {
        let room_event_capacity = env::var("WS_ROOM_EVENT_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4096)
            .clamp(128, 65_536);
        let client = valkey.client();
        let pubsub = client.get_async_pubsub().await?;
        let (mut sink, stream) = pubsub.split();
        sink.subscribe("_control").await?;

        let (commands, command_rx) = mpsc::channel(128);
        let (control, _) = tokio::sync::broadcast::channel(128);
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

    pub(crate) fn subscribe_control(&self) -> tokio::sync::broadcast::Receiver<Message> {
        self.control.subscribe()
    }

    pub(crate) async fn subscribe(
        &self,
        channel: &str,
    ) -> Result<tokio::sync::broadcast::Receiver<Message>, AppError> {
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
    control: tokio::sync::broadcast::Sender<Message>,
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
                                let sender = tokio::sync::broadcast::channel(room_event_capacity).0;
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

pub(crate) fn next_room_retry_delay(current: Duration) -> Duration {
    (current * 2).min(Duration::from_secs(10))
}

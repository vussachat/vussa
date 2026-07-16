use super::*;

pub(crate) fn presence_set_key(channel: &str) -> String {
    format!("chat:presence:{channel}:users")
}

pub(crate) fn presence_key(channel: &str, user_id: Uuid) -> String {
    format!("chat:presence:{channel}:{user_id}")
}

pub(crate) async fn refresh_presence(
    valkey: &ValkeyPool,
    channel: &str,
    participant: &Participant,
) -> Result<(), AppError> {
    let mut connection = valkey.connection()?;
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
    valkey: &ValkeyPool,
    channel: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    let mut connection = valkey.connection()?;
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
    valkey: &ValkeyPool,
    channel: &str,
) -> Result<Vec<Participant>, AppError> {
    let mut connection = valkey.connection()?;
    let ids: Vec<String> = connection.smembers(presence_set_key(channel)).await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = ids
        .iter()
        .map(|id| format!("chat:presence:{channel}:{id}"))
        .collect::<Vec<_>>();
    let values: Vec<Option<String>> = connection.mget(keys).await?;
    let mut participants = Vec::with_capacity(ids.len());
    let mut stale = Vec::new();
    for (id, value) in ids.into_iter().zip(values) {
        match value.and_then(|value| serde_json::from_str::<Participant>(&value).ok()) {
            Some(participant) => participants.push(participant),
            None => stale.push(id),
        }
    }
    if !stale.is_empty() {
        let _: usize = connection.srem(presence_set_key(channel), stale).await?;
    }
    participants.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(participants)
}

pub(crate) async fn sync_presence(
    socket: &mut WebSocket,
    client: &ValkeyPool,
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

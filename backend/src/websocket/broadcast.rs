use super::*;

pub(crate) async fn broadcast_control(
    client: &ValkeyPool,
    event: &ServerEvent,
) -> Result<(), AppError> {
    broadcast(client, "_control", event).await
}

pub(crate) fn room_key(channel: &str) -> String {
    format!("chat:room:{channel}")
}

pub(crate) async fn broadcast(
    valkey: &ValkeyPool,
    channel: &str,
    event: &ServerEvent,
) -> Result<(), AppError> {
    let payload =
        serde_json::to_vec(event).map_err(|_| AppError::bad_request("could not encode event"))?;
    let mut connection = valkey.connection()?;
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

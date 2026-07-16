use super::*;

const CLAIM_TIMEOUT_MILLIS: i64 = 60_000;

pub(crate) async fn run_outbox(pool: PgPool, valkey: ValkeyPool) {
    loop {
        let now = now_millis() as i64;
        match claim_events(&pool, now).await {
            Ok(rows) => {
                for row in rows {
                    let id = row.get("id");
                    let topic: String = row.get("topic");
                    let payload = row.get("payload");
                    let result = dispatch(&pool, &valkey, &topic, &payload).await;
                    if let Err(error) = finish_event(&pool, id, result.is_ok()).await {
                        tracing::warn!(?error, %id, %topic, "outbox completion update failed");
                    }
                    if let Err(error) = result {
                        tracing::warn!(?error, %id, %topic, "outbox event delivery failed");
                    }
                }
            }
            Err(error) => tracing::warn!(?error, "outbox claim query failed"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn claim_events(pool: &PgPool, now: i64) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    sqlx::query(
        r#"
        WITH next AS (
            SELECT id
            FROM outbox_events
            WHERE published_at IS NULL
              AND attempts < 5
              AND (claimed_at IS NULL OR claimed_at < $1 - $2)
            ORDER BY created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 32
        ), claimed AS (
            UPDATE outbox_events AS event
            SET claimed_at = $1, attempts = event.attempts + 1
            FROM next
            WHERE event.id = next.id
            RETURNING event.id, event.topic, event.payload
        )
        SELECT id, topic, payload FROM claimed
        "#,
    )
    .bind(now)
    .bind(CLAIM_TIMEOUT_MILLIS)
    .fetch_all(pool)
    .await
}

async fn dispatch(
    pool: &PgPool,
    valkey: &ValkeyPool,
    topic: &str,
    payload: &serde_json::Value,
) -> Result<(), AppError> {
    match topic {
        "auth.invalidate" => handle_auth_invalidate(valkey, payload).await,
        "message.notifications" => handle_notification_retry(pool, payload).await,
        _ => {
            tracing::error!(%topic, "unknown outbox topic");
            Ok(())
        }
    }
}

async fn handle_auth_invalidate(
    valkey: &ValkeyPool,
    payload: &serde_json::Value,
) -> Result<(), AppError> {
    let user_id = payload
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| AppError::bad_request("invalid auth invalidation payload"))?;
    let mut connection = valkey.connection()?;
    let session_ids: Vec<String> = connection
        .smembers(format!("vussa:user_sessions:{user_id}"))
        .await?;
    if !session_ids.is_empty() {
        let keys = session_ids
            .iter()
            .filter_map(|id| Uuid::parse_str(id).ok())
            .map(session_key)
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            let _: usize = connection.del(keys).await?;
        }
    }
    let _: usize = connection
        .del(format!("vussa:user_sessions:{user_id}"))
        .await?;
    let _: i32 = redis::cmd("PUBLISH")
        .arg("auth.invalidate")
        .arg(payload.to_string())
        .query_async(&mut connection)
        .await?;
    Ok(())
}

async fn handle_notification_retry(
    pool: &PgPool,
    payload: &serde_json::Value,
) -> Result<(), AppError> {
    let (actor_id, message_id) = notification_retry_ids(payload)?;
    retry_message_notifications(pool, actor_id, message_id).await
}

fn notification_retry_ids(payload: &serde_json::Value) -> Result<(Uuid, Uuid), AppError> {
    let parse_id = |field| {
        payload
            .get(field)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
    };
    parse_id("actor_id")
        .zip(parse_id("message_id"))
        .ok_or_else(|| AppError::bad_request("invalid notification retry payload"))
}

async fn finish_event(pool: &PgPool, id: Uuid, delivered: bool) -> Result<(), sqlx::Error> {
    if delivered {
        sqlx::query(
            "UPDATE outbox_events SET published_at=$1, claimed_at=NULL WHERE id=$2 AND published_at IS NULL",
        )
        .bind(now_millis() as i64)
        .bind(id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            "UPDATE outbox_events SET claimed_at=NULL WHERE id=$1 AND published_at IS NULL",
        )
        .bind(id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_payload_requires_two_valid_ids() {
        let id = Uuid::now_v7();
        assert_eq!(
            notification_retry_ids(&serde_json::json!({"actor_id": id, "message_id": id})).unwrap(),
            (id, id)
        );
        assert!(notification_retry_ids(&serde_json::json!({"actor_id": "invalid"})).is_err());
    }
}

use super::*;

pub(crate) async fn run_outbox(pool: PgPool, _client: redis::Client) {
    loop {
        let now = now_millis() as i64;
        let result = sqlx::query("WITH next AS (SELECT id FROM outbox_events WHERE published_at IS NULL AND (claimed_at IS NULL OR claimed_at < $1 - 60000) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 32), claimed AS (UPDATE outbox_events o SET claimed_at=$1,attempts=o.attempts+1 FROM next WHERE o.id=next.id RETURNING o.id,o.topic,o.payload) SELECT id,topic,payload FROM claimed")
            .bind(now)
            .fetch_all(&pool).await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let id: Uuid = row.get("id");
                    let topic: String = row.get("topic");
                    let payload: serde_json::Value = row.get("payload");
                    if topic == "message.notifications" {
                        let retry_ids = payload
                            .get("actor_id")
                            .and_then(|value| value.as_str())
                            .and_then(|value| Uuid::parse_str(value).ok())
                            .zip(
                                payload
                                    .get("message_id")
                                    .and_then(|value| value.as_str())
                                    .and_then(|value| Uuid::parse_str(value).ok()),
                            );
                        let result = match retry_ids {
                            Some((actor_id, message_id)) => {
                                retry_message_notifications(&pool, actor_id, message_id).await
                            }
                            None => {
                                Err(AppError::bad_request("invalid notification retry payload"))
                            }
                        };
                        if result.is_ok() {
                            if let Err(error) = sqlx::query("UPDATE outbox_events SET published_at=$1,claimed_at=NULL WHERE id=$2 AND published_at IS NULL").bind(now_millis() as i64).bind(id).execute(&pool).await {
                            tracing::warn!(?error, %id, "outbox publish acknowledgement failed");
                        }
                        } else {
                            if let Err(error) = sqlx::query("UPDATE outbox_events SET claimed_at=NULL WHERE id=$1 AND published_at IS NULL").bind(id).execute(&pool).await {
                            tracing::warn!(?error, %id, "outbox retry release failed");
                        }
                        }
                        continue;
                    }
                    if let Ok(mut connection) = valkey_commands() {
                        if topic == "auth.invalidate"
                            && let Some(user_id) =
                                payload.get("user_id").and_then(|value| value.as_str())
                        {
                            if let Ok(ids) = connection
                                .smembers::<_, Vec<String>>(format!(
                                    "vussa:user_sessions:{user_id}"
                                ))
                                .await
                            {
                                for session_id in ids {
                                    let _: redis::RedisResult<usize> = connection
                                        .del(session_key(
                                            Uuid::parse_str(&session_id).unwrap_or(Uuid::nil()),
                                        ))
                                        .await;
                                }
                            }
                            let _: redis::RedisResult<usize> = connection
                                .del(format!("vussa:user_sessions:{user_id}"))
                                .await;
                        }
                        let sent: redis::RedisResult<i32> = redis::cmd("PUBLISH")
                            .arg(&topic)
                            .arg(payload.to_string())
                            .query_async(&mut connection)
                            .await;
                        if sent.is_ok() {
                            if let Err(error) = sqlx::query("UPDATE outbox_events SET published_at=$1,claimed_at=NULL WHERE id=$2 AND published_at IS NULL").bind(now_millis() as i64).bind(id).execute(&pool).await {
                            tracing::warn!(?error, %id, "outbox publish acknowledgement failed");
                        }
                        } else {
                            if let Err(error) = sqlx::query("UPDATE outbox_events SET claimed_at=NULL WHERE id=$1 AND published_at IS NULL")
                                .bind(id)
                                .execute(&pool)
                                .await {
                            tracing::warn!(?error, %id, "outbox retry release failed");
                        }
                        }
                    }
                }
            }
            Err(error) => tracing::warn!(?error, "outbox claim query failed"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

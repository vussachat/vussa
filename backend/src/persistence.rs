use super::*;

pub(crate) struct AuditEvent<'a> {
    pub(crate) actor: Option<Uuid>,
    pub(crate) action: &'a str,
    pub(crate) target_type: &'a str,
    pub(crate) target_id: Uuid,
    pub(crate) metadata: serde_json::Value,
    pub(crate) created_at: i64,
}

pub(crate) async fn record_audit(
    connection: &mut sqlx::PgConnection,
    event: AuditEvent<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO audit_events
            (id, actor_user_id, action, target_type, target_id, metadata, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(event.actor)
    .bind(event.action)
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(event.metadata)
    .bind(event.created_at)
    .execute(connection)
    .await?;
    Ok(())
}

pub(crate) async fn record_audit_pool(
    pool: &PgPool,
    event: AuditEvent<'_>,
) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await?;
    record_audit(&mut connection, event).await
}

pub(crate) async fn queue_auth_invalidation(
    connection: &mut sqlx::PgConnection,
    user_id: Uuid,
    created_at: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO outbox_events (id, topic, payload, created_at)
        VALUES ($1, 'auth.invalidate', jsonb_build_object('user_id', $2::text), $3)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(created_at)
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_keeps_one_operation_timestamp() {
        let event = AuditEvent {
            actor: Some(Uuid::nil()),
            action: "resource.changed",
            target_type: "resource",
            target_id: Uuid::nil(),
            metadata: serde_json::json!({}),
            created_at: 42,
        };
        assert_eq!(event.created_at, 42);
    }
}

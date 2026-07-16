use super::*;

pub(crate) async fn create_public_channel(
    state: &AppState,
    actor: &AuthUser,
    raw_name: &str,
) -> Result<String, AppError> {
    require_permission(actor, "chat:write")?;
    ensure_not_globally_banned(&state.database, actor.id).await?;
    let name = normalize_channel_name(raw_name)?;
    let now = state.clock.now_millis() as i64;
    let mut transaction = state.database.begin().await?;
    let channel_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO channels (id, name, created_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET deleted_at = NULL
        WHERE channels.deleted_at IS NOT NULL
        RETURNING id
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(&name)
    .bind(now)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| AppError::bad_request("channel already exists"))?;
    record_audit(
        &mut transaction,
        AuditEvent {
            actor: Some(actor.id),
            action: "channel.created",
            target_type: "channel",
            target_id: channel_id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    publish_system_message(
        state,
        &name,
        format!("{} created this channel", actor.username),
    )
    .await?;
    broadcast_control(
        &state.valkey,
        &ServerEvent::ChannelCreated { name: name.clone() },
    )
    .await?;
    Ok(name)
}

pub(crate) async fn create_private_conversation(
    state: &AppState,
    actor: &AuthUser,
    raw_name: &str,
) -> Result<(Uuid, String), AppError> {
    require_permission(actor, "chat:write")?;
    ensure_not_globally_banned(&state.database, actor.id).await?;
    let name = normalize_channel_name(raw_name)?;
    let id = Uuid::now_v7();
    let now = state.clock.now_millis() as i64;
    let mut transaction = state.database.begin().await?;
    let result = sqlx::query(
        "INSERT INTO channels (id,name,kind,owner_user_id,created_at) VALUES ($1,$2,'private',$3,$4) ON CONFLICT (name) DO NOTHING",
    )
    .bind(id)
    .bind(&name)
    .bind(actor.id)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::bad_request("channel already exists"));
    }
    sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'owner',$2,$3)")
        .bind(id).bind(actor.id).bind(now).execute(&mut *transaction).await?;
    record_audit(
        &mut transaction,
        AuditEvent {
            actor: Some(actor.id),
            action: "channel.private_created",
            target_type: "channel",
            target_id: id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    publish_system_message(
        state,
        &name,
        format!("{} created this private channel", actor.username),
    )
    .await?;
    Ok((id, name))
}

pub(crate) async fn open_direct(
    state: &AppState,
    actor: &AuthUser,
    target_user_id: Uuid,
) -> Result<(Uuid, String), AppError> {
    require_permission(actor, "chat:write")?;
    ensure_not_globally_banned(&state.database, actor.id).await?;
    if target_user_id == actor.id {
        return Err(AppError::bad_request("you cannot message yourself"));
    }
    let target_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL)")
        .bind(target_user_id).fetch_one(&state.database).await?;
    if !target_exists {
        return Err(RepositoryError::NotFound.into());
    }
    let (first, second) = if actor.id < target_user_id {
        (actor.id, target_user_id)
    } else {
        (target_user_id, actor.id)
    };
    let direct_key = format!("{first}:{second}");
    let now = state.clock.now_millis() as i64;
    let proposed_id = Uuid::now_v7();
    let proposed_name = format!("dm_{proposed_id}");
    let mut transaction = state.database.begin().await?;
    let inserted = sqlx::query("INSERT INTO channels (id,name,kind,direct_key,created_at) VALUES ($1,$2,'direct',$3,$4) ON CONFLICT (direct_key) WHERE kind='direct' AND deleted_at IS NULL DO NOTHING RETURNING id,name")
        .bind(proposed_id).bind(&proposed_name).bind(&direct_key).bind(now).fetch_optional(&mut *transaction).await?;
    let (channel_id, name, created) = if let Some(row) = inserted {
        (row.get("id"), row.get("name"), true)
    } else {
        let row = sqlx::query("SELECT id,name FROM channels WHERE direct_key=$1 AND kind='direct' AND deleted_at IS NULL")
            .bind(&direct_key).fetch_one(&mut *transaction).await?;
        (row.get("id"), row.get("name"), false)
    };
    for member in [first, second] {
        sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
            .bind(channel_id).bind(member).bind(actor.id).bind(now).execute(&mut *transaction).await?;
    }
    if created {
        record_audit(
            &mut transaction,
            AuditEvent {
                actor: Some(actor.id),
                action: "channel.direct_created",
                target_type: "channel",
                target_id: channel_id,
                metadata: serde_json::json!({"user_id": target_user_id}),
                created_at: now,
            },
        )
        .await?;
    }
    transaction.commit().await?;
    Ok((channel_id, name))
}

pub(crate) async fn invite_member(
    state: &AppState,
    actor: &AuthUser,
    channel: &str,
    user_id: Uuid,
) -> Result<bool, AppError> {
    require_permission(actor, "chat:write")?;
    let channel_id = owned_private_channel(&state.database, channel, actor.id).await?;
    let now = state.clock.now_millis() as i64;
    let mut transaction = state.database.begin().await?;
    let target = sqlx::query_scalar::<_, String>(
        "SELECT username FROM users WHERE id=$1 AND disabled_at IS NULL AND deleted_at IS NULL",
    )
    .bind(user_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(RepositoryError::NotFound)?;
    let result = sqlx::query("INSERT INTO channel_members (channel_id,user_id,membership_role,invited_by,created_at) VALUES ($1,$2,'member',$3,$4) ON CONFLICT DO NOTHING")
        .bind(channel_id).bind(user_id).bind(actor.id).bind(now).execute(&mut *transaction).await?;
    if result.rows_affected() == 0 {
        transaction.commit().await?;
        return Ok(false);
    }
    record_audit(
        &mut transaction,
        AuditEvent {
            actor: Some(actor.id),
            action: "channel.member_added",
            target_type: "channel",
            target_id: channel_id,
            metadata: serde_json::json!({"user_id": user_id}),
            created_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    publish_system_message(
        state,
        channel,
        format!("{} invited {target}", actor.username),
    )
    .await?;
    broadcast_members(state, channel).await;
    Ok(true)
}

pub(crate) async fn remove_member(
    state: &AppState,
    actor: &AuthUser,
    channel: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    require_permission(actor, "chat:write")?;
    let channel_id = owned_private_channel(&state.database, channel, actor.id).await?;
    let now = state.clock.now_millis() as i64;
    let mut transaction = state.database.begin().await?;
    let result = sqlx::query("DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2 AND membership_role <> 'owner'")
        .bind(channel_id).bind(user_id).execute(&mut *transaction).await?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::NotFound.into());
    }
    let target = sqlx::query_scalar::<_, String>("SELECT username FROM users WHERE id=$1")
        .bind(user_id)
        .fetch_optional(&mut *transaction)
        .await?
        .unwrap_or_else(|| "a member".into());
    record_audit(
        &mut transaction,
        AuditEvent {
            actor: Some(actor.id),
            action: "channel.member_removed",
            target_type: "channel",
            target_id: channel_id,
            metadata: serde_json::json!({"user_id": user_id}),
            created_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    publish_system_message(
        state,
        channel,
        format!("{} removed {target}", actor.username),
    )
    .await?;
    broadcast_members(state, channel).await;
    Ok(())
}

async fn broadcast_members(state: &AppState, channel: &str) {
    match channel_members(&state.database, channel).await {
        Ok(members) => {
            if let Err(error) = broadcast(
                &state.valkey,
                channel,
                &ServerEvent::Members {
                    channel: channel.to_string(),
                    members,
                },
            )
            .await
            {
                tracing::debug!(?error, %channel, "member update broadcast failed");
            }
        }
        Err(error) => tracing::debug!(?error, %channel, "member refresh failed"),
    }
}

pub(crate) async fn delete_channel_service(
    state: &AppState,
    actor: &AuthUser,
    raw_name: &str,
) -> Result<String, AppError> {
    require_permission(actor, "chat:moderate")?;
    let name = normalize_channel_name(raw_name)?;
    if name == MAIN_CHANNEL {
        return Err(AppError::bad_request("the main channel cannot be removed"));
    }
    let now = state.clock.now_millis() as i64;
    let mut transaction = state.database.begin().await?;
    let id = sqlx::query_scalar::<_, Uuid>(
        "UPDATE channels SET deleted_at=$1 WHERE name=$2 AND deleted_at IS NULL RETURNING id",
    )
    .bind(now)
    .bind(&name)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(RepositoryError::NotFound)?;
    record_audit(
        &mut transaction,
        AuditEvent {
            actor: Some(actor.id),
            action: "channel.deleted",
            target_type: "channel",
            target_id: id,
            metadata: serde_json::json!({}),
            created_at: now,
        },
    )
    .await?;
    transaction.commit().await?;
    broadcast_control(
        &state.valkey,
        &ServerEvent::ChannelDeleted { name: name.clone() },
    )
    .await?;
    Ok(name)
}

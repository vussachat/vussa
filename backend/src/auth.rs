use super::*;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

mod recovery;

pub(crate) use recovery::{DisabledRecoveryNotifier, RecoveryNotifier, WebhookRecoveryNotifier};

pub(crate) fn password_hash(password: &str) -> Result<String, AppError> {
    if password.len() < 12 || password.len() > 200 {
        return Err(AppError::bad_request("password must be 12–200 characters"));
    }
    hash_password_unchecked(password)
}

pub(crate) fn hash_password_unchecked(password: &str) -> Result<String, AppError> {
    let salt = argon2::password_hash::SaltString::encode_b64(Uuid::now_v7().as_bytes())
        .map_err(|_| AppError::bad_request("could not create password salt"))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AppError::bad_request("could not hash password"))
}

pub(crate) fn verify_password(password: &str, encoded: &str) -> bool {
    PasswordHash::new(encoded)
        .ok()
        .map(|hash| {
            Argon2::default()
                .verify_password(password.as_bytes(), &hash)
                .is_ok()
        })
        .unwrap_or(false)
}

fn verification_key(user_id: Uuid, password: &str, encoded: &str) -> VerificationKey {
    let mut digest = Sha256::new();
    digest.update(user_id.as_bytes());
    digest.update(encoded.as_bytes());
    digest.update(password.as_bytes());
    VerificationKey(digest.finalize().into())
}

pub(crate) async fn verify_password_coalesced(
    state: &Arc<AppState>,
    user_id: Uuid,
    password: String,
    encoded: String,
) -> Result<bool, AppError> {
    let key = verification_key(user_id, &password, &encoded);
    let (mut receiver, new_flight) = {
        let mut flights = state.password_verification_flights.lock().await;
        if let Some(sender) = flights.get(&key) {
            (sender.subscribe(), None)
        } else {
            let (sender, receiver) = watch::channel(None);
            flights.insert(key, sender.clone());
            (receiver, Some(sender))
        }
    };
    if let Some(sender) = new_flight {
        let verifiers = state.password_verifiers.clone();
        let flights = state.password_verification_flights.clone();
        tokio::spawn(async move {
            let outcome =
                match tokio::time::timeout(Duration::from_secs(5), verifiers.acquire_owned()).await
                {
                    Ok(Ok(permit)) => match tokio::task::spawn_blocking(move || {
                        let verified = verify_password(&password, &encoded);
                        drop(permit);
                        verified
                    })
                    .await
                    {
                        Ok(verified) => VerificationOutcome::Verified(verified),
                        Err(_) => VerificationOutcome::Unavailable,
                    },
                    _ => VerificationOutcome::Overloaded,
                };
            let _ = sender.send(Some(outcome));
            flights.lock().await.remove(&key);
        });
    }
    loop {
        let outcome = *receiver.borrow();
        match outcome {
            Some(VerificationOutcome::Verified(value)) => return Ok(value),
            Some(VerificationOutcome::Overloaded) => {
                return Err(AppError::too_many_requests(
                    "authentication service is busy",
                ));
            }
            Some(VerificationOutcome::Unavailable) => {
                return Err(AppError::service_unavailable(
                    "authentication service unavailable",
                ));
            }
            None => receiver
                .changed()
                .await
                .map_err(|_| AppError::service_unavailable("authentication service unavailable"))?,
        }
    }
}

pub(crate) fn password_verifier_limit() -> usize {
    env::var("AUTH_VERIFY_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(4)
                .saturating_mul(2)
                .clamp(2, 16)
        })
}

pub(crate) fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get("cookie")?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == name).then(|| value.to_string())
        })
}

pub(crate) fn session_key(id: Uuid) -> String {
    format!("vussa:session:{id}")
}

pub(crate) fn message_rate_key(user_id: Uuid) -> String {
    format!("chat:rate:message:{user_id}")
}

pub(crate) async fn enforce_rate_limit(
    valkey: &ValkeyPool,
    key: &str,
    limit: i64,
    window_seconds: u64,
) -> Result<(), AppError> {
    let mut connection = valkey.connection()?;
    let count: i64 = connection.incr(key, 1).await?;
    if count == 1 {
        let _: bool = connection.expire(key, window_seconds as i64).await?;
    } else {
        use redis::AsyncCommands;
        let ttl: i64 = connection.ttl(key).await?;
        if ttl == -1 {
            let _: bool = connection.expire(key, window_seconds as i64).await?;
        }
    }
    if count > limit {
        return Err(AppError::too_many_requests("rate limit exceeded"));
    }
    Ok(())
}

pub(crate) async fn create_session(
    valkey: &ValkeyPool,
    user: &AuthUser,
) -> Result<(Uuid, String), AppError> {
    let id = Uuid::now_v7();
    let mut csrf_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut csrf_bytes);
    let csrf = hex::encode(csrf_bytes);
    let user_json = serde_json::to_string(user)
        .map_err(|_| AppError::bad_request("could not create session"))?;
    let mut connection = valkey.connection()?;
    redis::pipe()
        .atomic()
        .cmd("HSET")
        .arg(session_key(id))
        .arg("csrf")
        .arg(&csrf)
        .arg("user")
        .arg(user_json)
        .ignore()
        .cmd("EXPIRE")
        .arg(session_key(id))
        .arg(SESSION_TTL_SECONDS)
        .ignore()
        .cmd("SADD")
        .arg(format!("vussa:user_sessions:{}", user.id))
        .arg(id.to_string())
        .ignore()
        .query_async::<()>(&mut connection)
        .await?;
    Ok((id, csrf))
}

pub(crate) async fn load_session(
    headers: &HeaderMap,
    valkey: &ValkeyPool,
) -> Result<Session, AppError> {
    let raw = cookie_value(headers, "vussa_session")
        .ok_or_else(|| AppError::unauthorized("authentication required"))?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| AppError::unauthorized("authentication required"))?;
    let mut connection = valkey.connection()?;
    let values: Vec<Option<String>> = redis::cmd("HMGET")
        .arg(session_key(id))
        .arg(&["csrf", "user"])
        .query_async(&mut connection)
        .await?;
    let csrf = values
        .first()
        .and_then(Clone::clone)
        .ok_or_else(|| AppError::unauthorized("session expired"))?;
    let user: AuthUser = serde_json::from_str(
        values
            .get(1)
            .and_then(Clone::clone)
            .as_deref()
            .ok_or_else(|| AppError::unauthorized("session expired"))?,
    )
    .map_err(|_| AppError::unauthorized("session expired"))?;
    let _: bool = connection
        .expire(session_key(id), SESSION_TTL_SECONDS as i64)
        .await?;
    Ok(Session { id, csrf, user })
}

pub(crate) fn require_csrf(headers: &HeaderMap, session: &Session) -> Result<(), AppError> {
    let header_val = headers
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok());
    let Some(header_str) = header_val else {
        return Err(AppError::bad_request("invalid csrf token"));
    };
    if !constant_time_compare(header_str, &session.csrf) {
        return Err(AppError::bad_request("invalid csrf token"));
    }
    Ok(())
}

fn constant_time_compare(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a_bytes.iter().zip(b_bytes.iter()) {
        result |= x ^ y;
    }
    result == 0
}

pub(crate) fn require_permission(user: &AuthUser, permission: &str) -> Result<(), AppError> {
    if user.permissions.iter().any(|value| value == permission) {
        Ok(())
    } else {
        Err(AppError::forbidden("permission denied"))
    }
}

pub(crate) fn auth_cookie(id: Uuid) -> HeaderValue {
    let secure = if env::var("COOKIE_SECURE").unwrap_or_else(|_| "false".into()) == "true" {
        "; Secure"
    } else {
        ""
    };
    HeaderValue::from_str(&format!(
        "vussa_session={id}; Path=/; HttpOnly{secure}; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}"
    ))
    .expect("session cookie value is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_lookup_handles_multiple_values_without_cross_matching() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            "other=x; vussa_session=session-id; tail=y".parse().unwrap(),
        );
        assert_eq!(
            cookie_value(&headers, "vussa_session").as_deref(),
            Some("session-id")
        );
        assert_eq!(cookie_value(&headers, "missing"), None);
    }

    #[test]
    fn permission_checks_are_exact() {
        let user = AuthUser {
            id: Uuid::nil(),
            email: "a@b.test".into(),
            username: "a".into(),
            roles: vec![],
            permissions: vec!["chat:write".into()],
        };
        assert!(require_permission(&user, "chat:write").is_ok());
        assert!(require_permission(&user, "chat:moderate").is_err());
    }

    #[test]
    fn password_policy_rejects_invalid_lengths() {
        assert!(password_hash("short").is_err());
        assert!(password_hash(&"a".repeat(201)).is_err());
    }

    #[test]
    fn message_rate_limits_are_user_scoped() {
        let user_id = Uuid::now_v7();
        assert_eq!(
            message_rate_key(user_id),
            format!("chat:rate:message:{user_id}")
        );
    }
    #[test]
    fn password_hashes_verify_only_for_the_original_value() {
        let encoded = password_hash("correct horse battery staple").unwrap();
        assert!(verify_password("correct horse battery staple", &encoded));
        assert!(!verify_password("incorrect horse battery staple", &encoded));
    }
}

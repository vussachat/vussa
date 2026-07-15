use super::*;

async fn bootstrap_admin(repository: &PostgresRepository) -> Result<(), RepositoryError> {
    let (Some(email), Some(password)) = (
        env::var("ADMIN_EMAIL").ok(),
        env::var("ADMIN_PASSWORD").ok(),
    ) else {
        return Ok(());
    };
    let hash =
        password_hash(&password).map_err(|error| RepositoryError::Migration(error.to_string()))?;
    let mut tx = repository.pool.begin().await?;
    let id = Uuid::now_v7();
    let now = now_millis() as i64;
    let row = sqlx::query("INSERT INTO users (id,email,username,password_hash,created_at,updated_at) VALUES ($1,lower($2),$3,$4,$5,$5) ON CONFLICT (lower(email)) DO UPDATE SET password_hash=EXCLUDED.password_hash,disabled_at=NULL,updated_at=EXCLUDED.updated_at RETURNING id")
        .bind(id).bind(&email).bind("admin").bind(hash).bind(now).fetch_one(&mut *tx).await?;
    sqlx::query("INSERT INTO user_roles (user_id,role_id,assigned_at) SELECT $1,id,$2 FROM roles WHERE name='admin' ON CONFLICT DO NOTHING")
        .bind(row.get::<Uuid,_>("id")).bind(now).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// Build runtime dependencies and serve the application.
/// Keeping startup wiring separate makes the process entrypoint small and the
/// dependency graph explicit for operational and integration testing.
pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let valkey_url = env::var("VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let valkey = redis::Client::open(valkey_url)?;
    let valkey_pool_size = env::var("VALKEY_POOL_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
        .clamp(1, 64);
    let mut valkey_connections = Vec::with_capacity(valkey_pool_size);
    for _ in 0..valkey_pool_size {
        valkey_connections.push(valkey.get_multiplexed_async_connection().await?);
    }
    let valkey_connections = Arc::new(std::sync::RwLock::new(valkey_connections));
    VALKEY_COMMANDS
        .set(valkey_connections.clone())
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Valkey command pool already initialized",
            )
        })?;
    info!(valkey_pool_size, "Valkey command pool configured");
    tokio::spawn(recover_valkey_commands(valkey.clone(), valkey_connections));

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://vussa_chat:vussa_chat@127.0.0.1:5432/vussa_chat".into());
    let repository = PostgresRepository::connect(&database_url).await?;
    repository.ensure_main_channel().await?;
    repository.seed_authorization().await?;
    if env::var("SEED_TEST_ACCOUNTS").as_deref() == Ok("true") {
        repository.seed_test_accounts().await?;
    }
    bootstrap_admin(&repository).await?;
    if env::var("MIGRATIONS_ONLY").as_deref() == Ok("true") {
        info!("database migrations completed; exiting migration-only process");
        return Ok(());
    }

    tokio::spawn(outbox::run_outbox(repository.pool.clone(), valkey.clone()));
    let retention_repository = repository.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            if let Err(error) = retention_repository.prune_expired().await {
                error!(?error, "failed to prune expired messages");
            }
        }
    });

    let rooms = RoomManager::start(&valkey).await?;
    let blob_store: Arc<dyn BlobStore> = match env::var("STORAGE_BACKEND")
        .unwrap_or_else(|_| "filesystem".into())
        .as_str()
    {
        "s3" => Arc::new(storage::S3BlobStore::from_env()?),
        "filesystem" => Arc::new(FilesystemBlobStore::new(upload_dir())),
        backend => return Err(format!("unsupported STORAGE_BACKEND={backend}").into()),
    };
    let cleanup_repository = repository.clone();
    let cleanup_store = blob_store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            interval.tick().await;
            let cutoff = super::now_millis() as i64 - 60 * 60 * 1000;
            match cleanup_repository.claim_orphan_files(cutoff).await {
                Ok(files) => {
                    for (id, key) in files {
                        match cleanup_store.delete(&key).await {
                            Ok(()) => {
                                if let Err(error) =
                                    cleanup_repository.delete_file_metadata(id).await
                                {
                                    error!(?error, %id, "failed to remove deleted file metadata");
                                }
                            }
                            Err(error) => {
                                error!(?error, %id, "failed to remove orphaned file blob");
                            }
                        }
                    }
                }
                Err(error) => error!(?error, "failed to claim orphaned files"),
            }
        }
    });
    let scanner: Arc<dyn FileScanner> = if env::var("FILE_SCANNER_URL").is_ok() {
        Arc::new(HttpFileScanner::from_env().map_err(|error| {
            std::io::Error::other(format!("file scanner configuration failed: {error:?}"))
        })?)
    } else {
        Arc::new(NoopFileScanner)
    };
    let recovery_notifier: Arc<dyn RecoveryNotifier> = if env::var("RECOVERY_WEBHOOK_URL").is_ok() {
        Arc::new(WebhookRecoveryNotifier::from_env().map_err(|error| {
            std::io::Error::other(format!("recovery delivery configuration failed: {error:?}"))
        })?)
    } else {
        Arc::new(DisabledRecoveryNotifier)
    };
    let email_notifications = notification_sink(
        "NOTIFICATION_EMAIL_URL",
        "email",
        "email notification configuration failed",
    )?;
    let browser_notifications = notification_sink(
        "NOTIFICATION_BROWSER_URL",
        "browser",
        "browser notification configuration failed",
    )?;
    tokio::spawn(notification_delivery::run_notification_delivery(
        repository.pool.clone(),
        email_notifications.clone(),
        browser_notifications.clone(),
    ));
    let password_verifier_limit = password_verifier_limit();
    info!(
        password_verifier_limit,
        "password verification concurrency configured"
    );
    let state = Arc::new(AppState {
        valkey: valkey.clone(),
        cache_health: Arc::new(RedisCacheHealth::new(valkey.clone())),
        database_health: Arc::new(PostgresDatabaseHealth::new(repository.pool.clone())),
        database: repository.pool.clone(),
        repository,
        blob_store,
        scanner,
        recovery_notifier,
        clock: Arc::new(SystemClock),
        rooms,
        password_verifiers: Arc::new(Semaphore::new(password_verifier_limit)),
        password_verification_flights: Arc::new(TokioMutex::new(HashMap::new())),
    });
    let app = routes::build(state);
    let address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = TcpListener::bind(&address).await?;
    info!(%address, "chat backend listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn recover_valkey_commands(
    client: redis::Client,
    pool: Arc<std::sync::RwLock<Vec<redis::aio::MultiplexedConnection>>>,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let connections = match pool.read() {
            Ok(connections) => connections.clone(),
            Err(_) => continue,
        };
        for (index, mut connection) in connections.into_iter().enumerate() {
            let healthy = redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await
                .is_ok();
            if healthy {
                continue;
            }
            if let Ok(replacement) = client.get_multiplexed_async_connection().await
                && let Ok(mut writable) = pool.write()
                && let Some(slot) = writable.get_mut(index)
            {
                *slot = replacement;
            }
        }
    }
}

pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler")
                .recv()
                .await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    SHUTTING_DOWN.store(true, Ordering::Release);
    info!("shutdown signal received; draining HTTP and WebSocket connections");
}

fn notification_sink(
    variable: &'static str,
    kind: &'static str,
    context: &'static str,
) -> Result<Arc<dyn NotificationSink>, Box<dyn std::error::Error>> {
    if env::var(variable).is_ok() {
        Ok(Arc::new(
            WebhookNotificationSink::from_env(variable, kind)
                .map_err(|error| std::io::Error::other(format!("{context}: {error}")))?,
        ))
    } else {
        Ok(Arc::new(DisabledNotificationSink))
    }
}

use redis::AsyncCommands;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use uuid::Uuid;

async fn postgres_pool() -> Option<PgPool> {
    let url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var_os("CI").is_some() => {
            panic!("CI must provide DATABASE_URL for PostgreSQL integration tests")
        }
        Err(_) => return None,
    };
    Some(
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&url)
            .await
            .expect("DATABASE_URL is set but PostgreSQL is unavailable"),
    )
}

#[tokio::test]
async fn postgres_migrations_support_search_and_session_idempotency() {
    let Some(pool) = postgres_pool().await else {
        return;
    };
    sqlx::migrate!("../backend/migrations")
        .run(&pool)
        .await
        .expect("all migrations must apply to PostgreSQL");

    let suffix = Uuid::now_v7().simple().to_string();
    let user_id = Uuid::now_v7();
    let channel_id = Uuid::now_v7();
    let session_id = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let email = format!("integration-{suffix}@example.com");
    let username = format!("integration_{suffix}");
    let channel = format!("integration-{suffix}");
    let client_id = format!("client-{suffix}");

    sqlx::query(
        "INSERT INTO users (id,email,username,password_hash,created_at,updated_at)
         VALUES ($1,$2,$3,'test-hash',1,1)",
    )
    .bind(user_id)
    .bind(&email)
    .bind(&username)
    .execute(&pool)
    .await
    .expect("test user should be insertable");

    sqlx::query("INSERT INTO channels (id,name,created_at) VALUES ($1,$2,1)")
        .bind(channel_id)
        .bind(&channel)
        .execute(&pool)
        .await
        .expect("test channel should be insertable");

    sqlx::query(
        "INSERT INTO messages
         (id,channel_id,username,text,created_at,edited,owner_session,owner_user_id,client_id,metadata,mentions)
         VALUES ($1,$2,$3,'searchable integration message',2,FALSE,$4,$5,$6,'{}','{}')",
    )
    .bind(message_id)
    .bind(channel_id)
    .bind(&username)
    .bind(session_id)
    .bind(user_id)
    .bind(&client_id)
    .execute(&pool)
    .await
    .expect("message should be insertable");

    let search_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM messages
         WHERE channel_id=$1 AND search_vector @@ plainto_tsquery('simple', 'searchable')",
    )
    .bind(channel_id)
    .fetch_one(&pool)
    .await
    .expect("generated search vector should be queryable");
    assert_eq!(search_count, 1);

    let duplicate = sqlx::query(
        "INSERT INTO messages
         (id,channel_id,username,text,created_at,edited,owner_session,owner_user_id,client_id,metadata,mentions)
         VALUES ($1,$2,$3,'duplicate',3,FALSE,$4,$5,$6,'{}','{}')",
    )
    .bind(Uuid::now_v7())
    .bind(channel_id)
    .bind(&username)
    .bind(session_id)
    .bind(user_id)
    .bind(&client_id)
    .execute(&pool)
    .await;
    assert!(
        duplicate.is_err(),
        "session/client idempotency must be unique"
    );

    let row = sqlx::query("SELECT owner_user_id, client_id FROM messages WHERE id=$1")
        .bind(message_id)
        .fetch_one(&pool)
        .await
        .expect("inserted message should remain readable");
    assert_eq!(row.get::<Uuid, _>("owner_user_id"), user_id);
    assert_eq!(row.get::<String, _>("client_id"), client_id);

    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("integration fixtures should be removable");
}

#[tokio::test]
async fn valkey_supports_namespaced_ttl_state() {
    let url = match std::env::var("VALKEY_URL") {
        Ok(url) => url,
        Err(_) if std::env::var_os("CI").is_some() => {
            panic!("CI must provide VALKEY_URL for Valkey integration tests")
        }
        Err(_) => return,
    };
    let client = redis::Client::open(url).expect("VALKEY_URL must be valid");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("VALKEY_URL is set but Valkey is unavailable");
    let key = format!("chat:integration:{}:presence", Uuid::now_v7());

    connection
        .set_ex::<_, _, ()>(&key, "online", 30)
        .await
        .expect("Valkey should store TTL state");
    let value: Option<String> = connection
        .get(&key)
        .await
        .expect("Valkey should read TTL state");
    assert_eq!(value.as_deref(), Some("online"));

    let ttl: i64 = connection
        .ttl(&key)
        .await
        .expect("Valkey should report TTL state");
    assert!(ttl > 0 && ttl <= 30);
    connection
        .del::<_, ()>(&key)
        .await
        .expect("integration key should be removable");
}

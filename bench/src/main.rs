use anyhow::{Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{Instant as TokioInstant, MissedTickBehavior, interval_at, sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message},
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Api,
    Websocket,
    Mixed,
    Readonly,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "vussa-bench",
    version,
    about = "Standalone Vussa API and capacity benchmark"
)]
struct Args {
    #[arg(long, env = "VUSSA_BASE_URL", default_value = "http://127.0.0.1:3000")]
    base_url: String,
    #[arg(long, env = "VUSSA_WS_URL")]
    ws_url: Option<String>,
    #[arg(long, value_parser = parse_positive, conflicts_with = "capacity")]
    concurrency: Option<u32>,
    #[arg(long, conflicts_with = "concurrency")]
    capacity: bool,
    #[arg(long, default_value_t = 1024)]
    max_users: u32,
    #[arg(long, value_enum, default_value_t = Mode::Mixed)]
    mode: Mode,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 3)]
    warmup: u64,
    #[arg(
        long,
        default_value_t = 1.0,
        value_parser = parse_nonnegative_rate,
        help = "WebSocket messages per user per minute"
    )]
    message_rate: f64,
    #[arg(
        long,
        default_value_t = 1.0,
        value_parser = parse_nonnegative_rate,
        help = "REST requests per user per minute in mixed/API modes"
    )]
    api_rate: f64,
    #[arg(long, default_value_t = 5)]
    setup_interval_ms: u64,
    #[arg(long, default_value_t = 180)]
    setup_timeout: u64,
    #[arg(long, env = "VUSSA_NOFILE_LIMIT", default_value_t = 256_000)]
    nofile_limit: u64,
    #[arg(long, default_value = "test1")]
    admin_user: String,
    #[arg(long, default_value = "test1")]
    admin_password: String,
    #[arg(long, default_value = "test1@example.com")]
    admin_email: String,
    #[arg(long, default_value = "bench-results.json")]
    output: String,
    #[arg(long)]
    full_api: bool,
    #[arg(long, help = "Allow full_api to create and mutate benchmark fixtures")]
    allow_mutations: bool,
    #[arg(long)]
    readonly: bool,
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    failures: AtomicU64,
    auth_throttles: AtomicU64,
    setup_failures: AtomicU64,
    auth_successes: AtomicU64,
    ws_attempts: AtomicU64,
    ws_connected: AtomicU64,
    ws_failures: AtomicU64,
    ws_reconnects: AtomicU64,
    messages: AtomicU64,
    messages_acknowledged: AtomicU64,
    api_requests: AtomicU64,
    latencies: Mutex<BTreeMap<String, Vec<u64>>>,
    login_statuses: Mutex<BTreeMap<u16, u64>>,
    login_errors: Mutex<BTreeMap<String, u64>>,
    websocket_errors: Mutex<BTreeMap<String, u64>>,
    realtime_errors: Mutex<BTreeMap<String, u64>>,
    http_errors: Mutex<BTreeMap<String, u64>>,
    realtime_failures: AtomicU64,
}

struct SetupGate {
    ready: AtomicU64,
    released: AtomicBool,
    notify: Notify,
    deadline: Instant,
}

impl SetupGate {
    fn new(deadline: Instant) -> Self {
        Self {
            ready: AtomicU64::new(0),
            released: AtomicBool::new(false),
            notify: Notify::new(),
            deadline,
        }
    }
}

#[derive(Serialize)]
struct Report {
    mode: String,
    requested_concurrency: Option<u32>,
    capacity_estimate: Option<u32>,
    supported_users_lower_bound: Option<u32>,
    duration_seconds: u64,
    requests: u64,
    failures: u64,
    setup_failures: u64,
    auth_throttles: u64,
    error_rate: f64,
    authenticated_clients: u64,
    websocket_attempts: u64,
    requests_per_second: f64,
    websocket_connections: u64,
    websocket_failures: u64,
    websocket_reconnects: u64,
    messages_sent: u64,
    messages_acknowledged: u64,
    messages_unacknowledged: u64,
    api_requests: u64,
    api_operations_exercised: usize,
    login_statuses: BTreeMap<u16, u64>,
    login_errors: BTreeMap<String, u64>,
    websocket_errors: BTreeMap<String, u64>,
    realtime_errors: BTreeMap<String, u64>,
    http_errors: BTreeMap<String, u64>,
    realtime_failures: u64,
    endpoint_latency_ms: BTreeMap<String, Percentiles>,
    passed: bool,
    verdict: String,
}

#[derive(Serialize)]
struct Percentiles {
    count: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Deserialize)]
struct AuthResponse {
    id: Uuid,
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn parse_positive(raw: &str) -> std::result::Result<u32, String> {
    let value = raw
        .parse::<u32>()
        .map_err(|_| "concurrency must be one positive integer".to_string())?;
    if value == 0 {
        return Err("concurrency must be greater than zero".into());
    }
    Ok(value)
}

fn parse_nonnegative_rate(raw: &str) -> std::result::Result<f64, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| "message-rate must be a non-negative number".to_string())?;
    if !value.is_finite() || value < 0.0 {
        return Err("message-rate must be a non-negative number".into());
    }
    Ok(value)
}

fn raise_nofile_limit(requested: u64) -> Result<()> {
    if requested < 1024 {
        bail!("--nofile-limit must be at least 1024");
    }

    #[cfg(unix)]
    {
        let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: getrlimit initializes the provided structure on success.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) } != 0 {
            bail!(
                "could not read the process open-file limit: {}",
                std::io::Error::last_os_error()
            );
        }
        // SAFETY: getrlimit returned success and initialized limits.
        let mut limits = unsafe { limits.assume_init() };
        let requested = requested as libc::rlim_t;
        if limits.rlim_max != libc::RLIM_INFINITY && limits.rlim_max < requested {
            bail!(
                "benchmark needs an open-file hard limit of at least {requested}, but it is {}",
                limits.rlim_max
            );
        }
        limits.rlim_cur = requested;
        // SAFETY: the requested soft limit does not exceed the hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) } != 0 {
            bail!(
                "could not raise the process open-file limit to {requested}: {}",
                std::io::Error::last_os_error()
            );
        }
        let mut effective = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: getrlimit initializes the provided structure on success.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, effective.as_mut_ptr()) } != 0 {
            bail!(
                "could not verify the process open-file limit: {}",
                std::io::Error::last_os_error()
            );
        }
        // SAFETY: getrlimit returned success and initialized effective.
        let effective = unsafe { effective.assume_init() };
        if effective.rlim_cur < requested {
            bail!(
                "process open-file limit is {}, below requested {requested}",
                effective.rlim_cur
            );
        }
        println!(
            "Benchmark open-file limit: soft={} hard={}",
            effective.rlim_cur,
            if effective.rlim_max == libc::RLIM_INFINITY {
                "unlimited".to_string()
            } else {
                effective.rlim_max.to_string()
            }
        );
    }

    #[cfg(not(unix))]
    {
        let _ = requested;
        println!("Benchmark open-file limit: platform-managed");
    }

    Ok(())
}

fn ws_url(args: &Args) -> String {
    args.ws_url.clone().unwrap_or_else(|| {
        let base = args
            .base_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        format!("{}/api/v1/ws", base.trim_end_matches('/'))
    })
}

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::default())
}

fn record(metrics: &Metrics, name: &str, elapsed: Duration, ok: bool) {
    metrics.requests.fetch_add(1, Ordering::Relaxed);
    if !ok {
        metrics.failures.fetch_add(1, Ordering::Relaxed);
    }
    record_latency(metrics, name, elapsed);
}

fn record_latency(metrics: &Metrics, name: &str, elapsed: Duration) {
    metrics
        .latencies
        .lock()
        .expect("latency lock")
        .entry(name.to_string())
        .or_default()
        .push(elapsed.as_millis() as u64);
}

async fn request(
    client: &Client,
    metrics: &Metrics,
    method: Method,
    url: String,
    body: Option<Value>,
    name: &str,
    csrf: Option<&str>,
) -> Result<Value> {
    request_with_cookie(client, metrics, method, url, body, name, csrf, None).await
}

#[allow(clippy::too_many_arguments)]
async fn request_with_cookie(
    client: &Client,
    metrics: &Metrics,
    method: Method,
    url: String,
    body: Option<Value>,
    name: &str,
    csrf: Option<&str>,
    cookie: Option<&str>,
) -> Result<Value> {
    let started = Instant::now();
    let mut builder = client.request(method, url);
    if let Some(value) = body {
        builder = builder.json(&value);
    }
    if let Some(token) = csrf {
        builder = builder.header("x-csrf-token", token);
    }
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    let response = builder.send().await;
    let result = match response {
        Ok(response) => {
            let status = response.status();
            let body = response.json::<Value>().await.unwrap_or(Value::Null);
            if status.is_success() {
                Ok(body)
            } else {
                Err(anyhow!("{} returned {}: {}", name, status, body))
            }
        }
        Err(error) => Err(error.into()),
    };
    if let Err(error) = &result {
        *metrics
            .http_errors
            .lock()
            .expect("HTTP error lock")
            .entry(format!("{error:#}"))
            .or_default() += 1;
    }
    metrics.api_requests.fetch_add(1, Ordering::Relaxed);
    record(metrics, name, started.elapsed(), result.is_ok());
    result
}

async fn login(
    client: &Client,
    metrics: &Metrics,
    args: &Args,
    username: &str,
    password: &str,
) -> Result<(String, Uuid)> {
    let body = json!({"email": format!("{username}@example.com"), "password": password});
    let started = Instant::now();
    let response = client
        .post(format!(
            "{}/api/v1/auth/login",
            args.base_url.trim_end_matches('/')
        ))
        .json(&body)
        .send()
        .await?;
    let csrf = response
        .headers()
        .get("x-csrf-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v))
        .collect::<Vec<_>>()
        .join("; ");
    let status = response.status();
    *metrics
        .login_statuses
        .lock()
        .expect("login status lock")
        .entry(status.as_u16())
        .or_default() += 1;
    let response_body = response.json::<Value>().await.unwrap_or(Value::Null);
    let parsed = serde_json::from_value::<AuthResponse>(response_body.clone());
    let ok = status.is_success() && parsed.is_ok();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        metrics.auth_throttles.fetch_add(1, Ordering::Relaxed);
    }
    record(metrics, "POST /auth/login", started.elapsed(), ok);
    if !ok {
        let message = response_body
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("invalid login response")
            .to_string();
        *metrics
            .login_errors
            .lock()
            .expect("login error lock")
            .entry(message.clone())
            .or_default() += 1;
        bail!("status:{status} login for {username} failed: {message}");
    }
    metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
    Ok((format!("{cookies}; x-csrf-token={csrf}"), parsed?.id))
}

async fn connect_socket(url: &str, cookie: &str) -> Result<Socket> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert("cookie", cookie.parse()?);
    let (socket, _) = connect_async(request).await?;
    Ok(socket)
}

async fn api_smoke(
    args: &Args,
    metrics: Arc<Metrics>,
    client: Client,
    _cookie: &str,
    csrf: &str,
    _admin_id: Uuid,
    mutate: bool,
) -> Result<()> {
    let root = args.base_url.trim_end_matches('/');
    let headers = [
        (Method::GET, "/api/v1/health", "GET /health"),
        (Method::GET, "/api/v1/health/live", "GET /health/live"),
        (Method::GET, "/api/v1/health/ready", "GET /health/ready"),
        (Method::GET, "/api/v1/health/startup", "GET /health/startup"),
        (Method::GET, "/api/v1/metrics", "GET /metrics"),
        (Method::GET, "/api/v1/auth/me", "GET /auth/me"),
        (Method::GET, "/api/v1/channels", "GET /channels"),
        (Method::GET, "/api/v1/conversations", "GET /conversations"),
        (
            Method::GET,
            "/api/v1/users/search?q=test",
            "GET /users/search",
        ),
        (
            Method::GET,
            "/api/v1/admin/users?limit=20",
            "GET /admin/users",
        ),
        (Method::GET, "/api/v1/admin/roles", "GET /admin/roles"),
        (
            Method::GET,
            "/api/v1/admin/permissions",
            "GET /admin/permissions",
        ),
        (
            Method::GET,
            "/api/v1/admin/audit?limit=20",
            "GET /admin/audit",
        ),
        (
            Method::GET,
            "/api/v1/admin/operations",
            "GET /admin/operations",
        ),
        (
            Method::GET,
            "/api/v1/admin/channels?limit=20",
            "GET /admin/channels",
        ),
        (
            Method::GET,
            "/api/v1/admin/messages?limit=20",
            "GET /admin/messages",
        ),
        (
            Method::GET,
            "/api/v1/admin/participants/main",
            "GET /admin/participants",
        ),
    ];
    for (method, path, name) in headers {
        request(
            &client,
            &metrics,
            method,
            format!("{root}{path}"),
            None,
            name,
            Some(csrf),
        )
        .await?;
    }
    if mutate && !args.readonly {
        let channel = format!("bench-{}", Uuid::now_v7().simple());
        request(
            &client,
            &metrics,
            Method::POST,
            format!("{root}/api/v1/admin/channels"),
            Some(json!({"name": channel, "description": "benchmark fixture"})),
            "POST /admin/channels",
            Some(csrf),
        )
        .await?;
        let channels = request(
            &client,
            &metrics,
            Method::GET,
            format!("{root}/api/v1/admin/channels?q={channel}"),
            None,
            "GET /admin/channels/search",
            Some(csrf),
        )
        .await?;
        if let Some(id) = channels
            .get("items")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_str())
        {
            request(
                &client,
                &metrics,
                Method::PATCH,
                format!("{root}/api/v1/admin/channels/{id}"),
                Some(json!({"description":"updated by benchmark"})),
                "PATCH /admin/channels/:id",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/channels/{id}/archive"),
                None,
                "POST /admin/channels/:id/archive",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/channels/{id}/restore"),
                None,
                "POST /admin/channels/:id/restore",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/channels/{id}/delete"),
                None,
                "POST /admin/channels/:id/delete",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/channels/{id}/undelete"),
                None,
                "POST /admin/channels/:id/undelete",
                Some(csrf),
            )
            .await?;
        }
        let unique = Uuid::now_v7().simple().to_string();
        let unique = &unique[..20];
        let registered = request(
            &Client::new(),
            &metrics,
            Method::POST,
            format!("{root}/api/v1/auth/register"),
            Some(json!({"email":format!("bench-{unique}@example.com"),"username":format!("bench{unique}"),"password":"benchmark-password-1234"})),
            "POST /auth/register",
            None,
        ).await?;
        let fixture_user_id = registered
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let account = request(
            &client,
            &metrics,
            Method::GET,
            format!("{root}/api/v1/auth/me"),
            None,
            "GET /auth/me/account-target",
            Some(csrf),
        )
        .await?;
        if let Some(username) = account.get("username").and_then(Value::as_str) {
            request(
                &client,
                &metrics,
                Method::PATCH,
                format!("{root}/api/v1/account"),
                Some(json!({"username":username})),
                "PATCH /account",
                Some(csrf),
            )
            .await?;
        }
        let public = format!("bench-public-{unique}");
        request(
            &client,
            &metrics,
            Method::POST,
            format!("{root}/api/v1/channels"),
            Some(json!({"name":public})),
            "POST /channels",
            Some(csrf),
        )
        .await?;
        let target = request(
            &client,
            &metrics,
            Method::GET,
            format!("{root}/api/v1/users/search?q=test2"),
            None,
            "GET /users/search/target",
            Some(csrf),
        )
        .await?;
        if let Some(target_id) = target
            .as_array()
            .and_then(|v| v.first())
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str)
        {
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/conversations/direct"),
                Some(json!({"user_id":target_id})),
                "POST /conversations/direct",
                Some(csrf),
            )
            .await?;
            let private = format!("bench-private-{unique}");
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/channels/private"),
                Some(json!({"name":private})),
                "POST /channels/private",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::GET,
                format!("{root}/api/v1/channels/{private}/members"),
                None,
                "GET /channels/:name/members",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/channels/{private}/members"),
                Some(json!({"user_id":target_id})),
                "POST /channels/:name/members",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::DELETE,
                format!("{root}/api/v1/channels/{private}/members/{target_id}"),
                None,
                "DELETE /channels/:name/members/:user",
                Some(csrf),
            )
            .await?;
        }
        request(
            &client,
            &metrics,
            Method::GET,
            format!("{root}/api/v1/admin/users?limit=20"),
            None,
            "GET /admin/users/targets",
            Some(csrf),
        )
        .await?;
        if let Some(target_id) = fixture_user_id.as_deref() {
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/users/{target_id}/disable"),
                None,
                "POST /admin/users/:id/disable",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/users/{target_id}/enable"),
                None,
                "POST /admin/users/:id/enable",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/users/{target_id}/password-reset"),
                Some(json!({"password":"benchmark-reset-1234"})),
                "POST /admin/users/:id/password-reset",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/users/{target_id}/invalidate-sessions"),
                None,
                "POST /admin/users/:id/invalidate-sessions",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/users/{target_id}/roles/moderator"),
                None,
                "POST /admin/users/:id/roles/:role",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::DELETE,
                format!("{root}/api/v1/admin/users/{target_id}/roles/moderator"),
                None,
                "DELETE /admin/users/:id/roles/:role",
                Some(csrf),
            )
            .await?;
        }
        let admin_messages = request(
            &client,
            &metrics,
            Method::GET,
            format!("{root}/api/v1/admin/messages?limit=1"),
            None,
            "GET /admin/messages/target",
            Some(csrf),
        )
        .await?;
        if let Some(message_id) = admin_messages
            .get("items")
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str)
        {
            request(
                &client,
                &metrics,
                Method::GET,
                format!("{root}/api/v1/admin/messages/{message_id}/history"),
                None,
                "GET /admin/messages/:id/history",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/messages/{message_id}/delete"),
                Some(json!({"reason":"benchmark"})),
                "POST /admin/messages/:id/delete",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/messages/{message_id}/restore"),
                Some(json!({"reason":"benchmark"})),
                "POST /admin/messages/:id/restore",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/messages/bulk-moderate"),
                Some(json!({"ids":[message_id],"action":"delete","reason":"benchmark"})),
                "POST /admin/messages/bulk-moderate",
                Some(csrf),
            )
            .await?;
            request(
                &client,
                &metrics,
                Method::POST,
                format!("{root}/api/v1/admin/messages/bulk-moderate"),
                Some(json!({"ids":[message_id],"action":"restore","reason":"benchmark"})),
                "POST /admin/messages/bulk-moderate/restore",
                Some(csrf),
            )
            .await?;
        }
        if let Some(target_id) = fixture_user_id.as_deref() {
            request(
                &client,
                &metrics,
                Method::DELETE,
                format!("{root}/api/v1/admin/users/{target_id}"),
                None,
                "DELETE /admin/users/:id",
                Some(csrf),
            )
            .await?;
        }
        request(
            &client,
            &metrics,
            Method::POST,
            format!("{root}/api/v1/auth/logout"),
            None,
            "POST /auth/logout",
            Some(csrf),
        )
        .await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ApiOperation {
    path: &'static str,
    name: &'static str,
}

const USER_API_OPERATIONS: &[ApiOperation] = &[
    ApiOperation {
        path: "/api/v1/health",
        name: "GET /health",
    },
    ApiOperation {
        path: "/api/v1/health/live",
        name: "GET /health/live",
    },
    ApiOperation {
        path: "/api/v1/health/ready",
        name: "GET /health/ready",
    },
    ApiOperation {
        path: "/api/v1/health/startup",
        name: "GET /health/startup",
    },
    ApiOperation {
        path: "/api/v1/metrics",
        name: "GET /metrics",
    },
    ApiOperation {
        path: "/api/v1/auth/me",
        name: "GET /auth/me",
    },
    ApiOperation {
        path: "/api/v1/channels",
        name: "GET /channels",
    },
    ApiOperation {
        path: "/api/v1/users/search?q=test",
        name: "GET /users/search",
    },
    ApiOperation {
        path: "/api/v1/conversations",
        name: "GET /conversations",
    },
    ApiOperation {
        path: "/api/v1/channels/main/members",
        name: "GET /channels/:name/members",
    },
];

const ADMIN_API_OPERATIONS: &[ApiOperation] = &[
    ApiOperation {
        path: "/api/v1/admin/users?limit=20",
        name: "GET /admin/users",
    },
    ApiOperation {
        path: "/api/v1/admin/roles",
        name: "GET /admin/roles",
    },
    ApiOperation {
        path: "/api/v1/admin/permissions",
        name: "GET /admin/permissions",
    },
    ApiOperation {
        path: "/api/v1/admin/audit?limit=20",
        name: "GET /admin/audit",
    },
    ApiOperation {
        path: "/api/v1/admin/operations",
        name: "GET /admin/operations",
    },
    ApiOperation {
        path: "/api/v1/admin/channels?limit=20",
        name: "GET /admin/channels",
    },
    ApiOperation {
        path: "/api/v1/admin/messages?limit=20",
        name: "GET /admin/messages",
    },
    ApiOperation {
        path: "/api/v1/admin/participants/main",
        name: "GET /admin/participants",
    },
];

fn rate_period(rate_per_minute: f64) -> Option<Duration> {
    (rate_per_minute > 0.0)
        .then(|| Duration::from_secs_f64((60.0 / rate_per_minute).clamp(0.001, 86_400.0)))
}

fn distributed_phase(period: Duration, user_number: u32) -> Duration {
    let micros = period.as_micros().clamp(1, u64::MAX as u128) as u64;
    let mixed = (user_number as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    Duration::from_micros(mixed % micros)
}

#[allow(clippy::too_many_arguments)]
async fn run_api_workload(
    args: Args,
    metrics: Arc<Metrics>,
    client: Client,
    cookie: String,
    csrf: String,
    user_number: u32,
    is_admin: bool,
    duration: Duration,
) {
    let Some(period) = rate_period(args.api_rate) else {
        return;
    };
    let deadline = TokioInstant::now() + duration;
    let mut next = TokioInstant::now() + distributed_phase(period, user_number);
    let operation_count = USER_API_OPERATIONS.len()
        + if is_admin {
            ADMIN_API_OPERATIONS.len()
        } else {
            0
        };
    let mut sequence = 0usize;
    while next < deadline {
        tokio::time::sleep_until(next).await;
        let index = (user_number as usize + sequence) % operation_count;
        let operation = if index < USER_API_OPERATIONS.len() {
            USER_API_OPERATIONS[index]
        } else {
            ADMIN_API_OPERATIONS[index - USER_API_OPERATIONS.len()]
        };
        let _ = request_with_cookie(
            &client,
            &metrics,
            Method::GET,
            format!("{}{}", args.base_url.trim_end_matches('/'), operation.path),
            None,
            operation.name,
            Some(&csrf),
            Some(&cookie),
        )
        .await;
        sequence += 1;
        next += period;
    }
}

async fn user_loop(
    args: Args,
    metrics: Arc<Metrics>,
    shared_login_client: Client,
    shared_traffic_client: Client,
    user_number: u32,
    duration: Duration,
    auth_gate: Arc<SetupGate>,
    traffic_gate: Arc<SetupGate>,
) {
    sleep(Duration::from_millis(
        args.setup_interval_ms.saturating_mul(user_number as u64),
    ))
    .await;
    if Instant::now() >= auth_gate.deadline {
        metrics.setup_failures.fetch_add(1, Ordering::Relaxed);
        wait_for_phase(&auth_gate).await;
        wait_for_phase(&traffic_gate).await;
        return;
    }
    let websocket_mode = !matches!(args.mode, Mode::Api | Mode::Readonly);
    let mut client = Some(shared_login_client);
    let username = format!("test{}", (user_number % 6) + 1);
    let mut authenticated = None;
    for attempt in 0..5u32 {
        let remaining = auth_gate.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(
            remaining.min(Duration::from_secs(30)),
            login(
                client.as_ref().expect("login client is available"),
                &metrics,
                &args,
                &username,
                &username,
            ),
        )
        .await
        {
            Ok(Ok(value)) => {
                authenticated = Some(value);
                break;
            }
            Ok(Err(error)) if error.to_string().contains("status:429") && attempt < 4 => {
                let backoff = Duration::from_millis(500u64.saturating_mul(1u64 << attempt));
                let jitter = Duration::from_millis(rand::rng().random_range(0..500));
                sleep(backoff + jitter).await;
            }
            _ => break,
        }
    }
    let Some((cookie_header, _)) = authenticated else {
        metrics.setup_failures.fetch_add(1, Ordering::Relaxed);
        wait_for_phase(&auth_gate).await;
        wait_for_phase(&traffic_gate).await;
        return;
    };
    // Release every clone of the login HTTP pool before WebSocket setup. The
    // separate traffic pool has not opened any connections yet.
    client.take();
    wait_for_phase(&auth_gate).await;
    let csrf = cookie_header
        .split("x-csrf-token=")
        .nth(1)
        .unwrap_or_default();
    let mut socket = None;
    if !matches!(args.mode, Mode::Api | Mode::Readonly) {
        sleep(Duration::from_millis(
            args.setup_interval_ms.saturating_mul(user_number as u64),
        ))
        .await;
        metrics.ws_attempts.fetch_add(1, Ordering::Relaxed);
        let remaining = traffic_gate
            .deadline
            .saturating_duration_since(Instant::now());
        match timeout(
            remaining.min(Duration::from_secs(30)),
            connect_socket(&ws_url(&args), &cookie_header),
        )
        .await
        {
            Ok(Ok(value)) => {
                socket = Some(value);
            }
            Ok(Err(error)) => {
                metrics.ws_failures.fetch_add(1, Ordering::Relaxed);
                metrics.setup_failures.fetch_add(1, Ordering::Relaxed);
                *metrics
                    .websocket_errors
                    .lock()
                    .expect("websocket error lock")
                    .entry(error.to_string())
                    .or_default() += 1;
            }
            Err(_) => {
                metrics.ws_failures.fetch_add(1, Ordering::Relaxed);
                metrics.setup_failures.fetch_add(1, Ordering::Relaxed);
                *metrics
                    .websocket_errors
                    .lock()
                    .expect("websocket error lock")
                    .entry("setup timeout".to_string())
                    .or_default() += 1;
            }
        }
    }
    // Every client is authenticated and has completed its WebSocket setup
    // before any client begins generating steady-state traffic.
    if let Some(ws) = socket.as_mut() {
        match wait_for_socket_phase(&traffic_gate, ws, &metrics).await {
            Ok(()) => {
                metrics.ws_connected.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                metrics.ws_failures.fetch_add(1, Ordering::Relaxed);
                metrics.setup_failures.fetch_add(1, Ordering::Relaxed);
                *metrics
                    .websocket_errors
                    .lock()
                    .expect("websocket error lock")
                    .entry(error)
                    .or_default() += 1;
                socket = None;
            }
        }
    } else {
        wait_for_phase(&traffic_gate).await;
    }
    if socket.is_none() && websocket_mode {
        return;
    }
    let csrf = csrf.to_string();

    // Setup guarantees every client is authenticated and every requested
    // WebSocket exists before traffic starts. Ramp initial traffic as well.
    if !duration.is_zero() {
        sleep(Duration::from_millis(
            args.setup_interval_ms.saturating_mul(user_number as u64),
        ))
        .await;
    }

    if !websocket_mode {
        run_api_workload(
            args,
            metrics,
            shared_traffic_client,
            cookie_header,
            csrf,
            user_number,
            username == "test1",
            duration,
        )
        .await;
        return;
    }

    let api_task = matches!(args.mode, Mode::Mixed).then(|| {
        tokio::spawn(run_api_workload(
            args.clone(),
            metrics.clone(),
            shared_traffic_client,
            cookie_header.clone(),
            csrf.clone(),
            user_number,
            username == "test1",
            duration,
        ))
    });

    let Some(socket) = socket.take() else {
        return;
    };
    let (mut writer, mut reader) = socket.split();
    let mut pending_messages = HashMap::new();
    let shutdown = sleep(duration);
    tokio::pin!(shutdown);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let message_period = rate_period(args.message_rate).unwrap_or(Duration::from_secs(86_400));
    let message_start = if args.message_rate > 0.0 {
        TokioInstant::now() + distributed_phase(message_period, user_number)
    } else {
        TokioInstant::now() + Duration::from_secs(86_400)
    };
    let mut message_tick = interval_at(message_start, message_period);
    message_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut connected = true;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            message = reader.next() => match message {
                Some(Ok(Message::Close(_))) => {
                    record_realtime_error(&metrics, "server closed the socket during traffic".to_string());
                    connected = false;
                    break;
                }
                Some(Ok(message)) => record_socket_frame(&metrics, &message, Some(&mut pending_messages)),
                Some(Err(error)) => {
                    record_realtime_error(&metrics, format!("socket receive failed: {error}"));
                    connected = false;
                    break;
                }
                None => {
                    record_realtime_error(&metrics, "socket closed without a close frame".to_string());
                    connected = false;
                    break;
                }
            },
            _ = heartbeat.tick() => {
                if let Err(error) = writer
                    .send(Message::Text(json!({"type":"heartbeat"}).to_string().into()))
                    .await
                {
                    record_realtime_error(&metrics, format!("heartbeat send failed: {error}"));
                    connected = false;
                    break;
                }
            }
            _ = message_tick.tick(), if args.message_rate > 0.0 => {
                let marker = format!("bench:{user_number}:{}", Uuid::now_v7());
                let started = Instant::now();
                match writer
                    .send(Message::Text(
                        json!({"type":"send_message","text":marker}).to_string().into(),
                    ))
                    .await
                {
                    Ok(()) => {
                        pending_messages.insert(marker, started);
                        metrics.messages.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        record_realtime_error(&metrics, format!("message send failed: {error}"));
                        connected = false;
                        break;
                    }
                }
            }
        }
    }
    if connected && !pending_messages.is_empty() {
        let acknowledgement_deadline = TokioInstant::now() + Duration::from_secs(5);
        while !pending_messages.is_empty() && TokioInstant::now() < acknowledgement_deadline {
            let remaining = acknowledgement_deadline.saturating_duration_since(TokioInstant::now());
            match timeout(remaining, reader.next()).await {
                Ok(Some(Ok(message))) => {
                    record_socket_frame(&metrics, &message, Some(&mut pending_messages));
                }
                Ok(Some(Err(error))) => {
                    record_realtime_error(
                        &metrics,
                        format!(
                            "socket receive failed while awaiting message acknowledgement: {error}"
                        ),
                    );
                    connected = false;
                    break;
                }
                Ok(None) => {
                    record_realtime_error(
                        &metrics,
                        "socket closed while awaiting message acknowledgement".to_string(),
                    );
                    connected = false;
                    break;
                }
                Err(_) => break,
            }
        }
    }
    if !pending_messages.is_empty() {
        let missing = pending_messages.len() as u64;
        metrics
            .realtime_failures
            .fetch_add(missing, Ordering::Relaxed);
        *metrics
            .realtime_errors
            .lock()
            .expect("realtime error lock")
            .entry("message acknowledgement timed out".to_string())
            .or_default() += missing;
    }
    if connected {
        let _ = writer.send(Message::Close(None)).await;
    }
    if let Some(task) = api_task {
        let _ = task.await;
    }
}

async fn wait_for_phase(gate: &SetupGate) {
    gate.ready.fetch_add(1, Ordering::Relaxed);
    gate.notify.notify_one();
    loop {
        if gate.released.load(Ordering::Acquire) {
            return;
        }
        let notified = gate.notify.notified();
        if gate.released.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

async fn wait_for_socket_phase(
    gate: &SetupGate,
    socket: &mut Socket,
    metrics: &Metrics,
) -> Result<(), String> {
    gate.ready.fetch_add(1, Ordering::Relaxed);
    gate.notify.notify_one();
    loop {
        if gate.released.load(Ordering::Acquire) {
            return Ok(());
        }
        let notified = gate.notify.notified();
        if gate.released.load(Ordering::Acquire) {
            return Ok(());
        }
        tokio::select! {
            _ = notified => {}
            message = socket.next() => match message {
                Some(Ok(message)) => record_socket_frame(metrics, &message, None),
                Some(Err(error)) => return Err(format!("closed during setup: {error}")),
                None => return Err("closed during setup without a close frame".to_string()),
            }
        }
    }
}

fn record_socket_frame(
    metrics: &Metrics,
    message: &Message,
    pending_messages: Option<&mut HashMap<String, Instant>>,
) {
    let Message::Text(text) = message else {
        return;
    };
    let text = text.as_str();
    // Ordinary room frames are by far the hottest benchmark path. Avoid
    // allocating a generic JSON tree for every message received by every
    // simulated client; only error frames require decoding. A sender's unique
    // marker can be matched directly in the serialized frame.
    if text.starts_with("{\"type\":\"error\"") {
        if let Ok(event) = serde_json::from_str::<Value>(text) {
            let message = event
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unspecified server error")
                .to_string();
            record_realtime_error(metrics, message);
        }
        return;
    }
    if let Some(pending_messages) = pending_messages {
        let acknowledged = pending_messages
            .keys()
            .find(|marker| text.contains(marker.as_str()))
            .cloned();
        if let Some(marker) = acknowledged
            && let Some(started) = pending_messages.remove(&marker)
        {
            metrics
                .messages_acknowledged
                .fetch_add(1, Ordering::Relaxed);
            record_latency(metrics, "WS message delivery", started.elapsed());
        }
    }
}

fn record_realtime_error(metrics: &Metrics, error: String) {
    metrics.realtime_failures.fetch_add(1, Ordering::Relaxed);
    *metrics
        .realtime_errors
        .lock()
        .expect("realtime error lock")
        .entry(error)
        .or_default() += 1;
}

fn percentile(values: &mut [u64], p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[((values.len() - 1) as f64 * p).round() as usize]
}

fn make_report(
    args: &Args,
    metrics: &Metrics,
    requested: Option<u32>,
    elapsed: Duration,
) -> Report {
    let requests = metrics.requests.load(Ordering::Relaxed);
    let auth_throttles = metrics.auth_throttles.load(Ordering::Relaxed);
    let failures = metrics
        .failures
        .load(Ordering::Relaxed)
        .saturating_sub(auth_throttles);
    let authenticated_clients = metrics
        .auth_successes
        .load(Ordering::Relaxed)
        .saturating_sub(1);
    let websocket_attempts = metrics.ws_attempts.load(Ordering::Relaxed);
    let websocket_failures = metrics.ws_failures.load(Ordering::Relaxed);
    let setup_failures = metrics.setup_failures.load(Ordering::Relaxed);
    let realtime_failures = metrics.realtime_failures.load(Ordering::Relaxed);
    let error_rate = if requests == 0 {
        1.0
    } else {
        failures as f64 / requests as f64
    };
    let endpoint_latency_ms = metrics
        .latencies
        .lock()
        .expect("latency lock")
        .iter()
        .map(|(name, values)| {
            let mut values = values.clone();
            (
                name.clone(),
                Percentiles {
                    count: values.len(),
                    p50: percentile(&mut values, 0.50),
                    p95: percentile(&mut values, 0.95),
                    p99: percentile(&mut values, 0.99),
                    max: values.iter().copied().max().unwrap_or_default(),
                },
            )
        })
        .collect();
    let api_operations_exercised = metrics
        .latencies
        .lock()
        .expect("latency lock")
        .keys()
        .filter(|name| name.as_str() != "POST /auth/login" && !name.starts_with("WS "))
        .count();
    let messages_sent = metrics.messages.load(Ordering::Relaxed);
    let messages_acknowledged = metrics.messages_acknowledged.load(Ordering::Relaxed);
    let websocket_error_rate = if websocket_attempts == 0 {
        0.0
    } else {
        websocket_failures as f64 / websocket_attempts as f64
    };
    let setup_success_rate = requested
        .map(|users| authenticated_clients as f64 / users.max(1) as f64)
        .unwrap_or(1.0);
    let websocket_success_rate = requested
        .filter(|_| websocket_attempts > 0)
        .map(|users| metrics.ws_connected.load(Ordering::Relaxed) as f64 / users.max(1) as f64)
        .unwrap_or(1.0);
    let passed = error_rate < 0.01
        && websocket_error_rate < 0.01
        && setup_success_rate >= 1.0
        && websocket_success_rate >= 1.0
        && realtime_failures == 0;
    let mode = format!("{:?}", args.mode).to_lowercase();
    let supported_users_lower_bound = passed.then_some(requested).flatten();
    let verdict = match (passed, requested) {
        (true, Some(users)) => {
            format!("PASS: this environment supports at least {users} concurrent {mode} users")
        }
        (true, None) => "PASS: capacity probe succeeded".to_string(),
        (false, Some(users)) => {
            format!("FAIL: this environment did not sustain {users} concurrent {mode} users")
        }
        (false, None) => "FAIL: environment saturated or unavailable".to_string(),
    };
    Report {
        mode,
        requested_concurrency: requested,
        capacity_estimate: None,
        supported_users_lower_bound,
        duration_seconds: elapsed.as_secs(),
        requests,
        failures,
        setup_failures,
        auth_throttles,
        error_rate,
        authenticated_clients,
        websocket_attempts,
        requests_per_second: requests as f64 / elapsed.as_secs_f64().max(0.001),
        websocket_connections: metrics.ws_connected.load(Ordering::Relaxed),
        websocket_failures,
        websocket_reconnects: metrics.ws_reconnects.load(Ordering::Relaxed),
        messages_sent,
        messages_acknowledged,
        messages_unacknowledged: messages_sent.saturating_sub(messages_acknowledged),
        api_requests: metrics.api_requests.load(Ordering::Relaxed),
        api_operations_exercised,
        login_statuses: metrics
            .login_statuses
            .lock()
            .expect("login status lock")
            .clone(),
        login_errors: metrics
            .login_errors
            .lock()
            .expect("login error lock")
            .clone(),
        websocket_errors: metrics
            .websocket_errors
            .lock()
            .expect("websocket error lock")
            .clone(),
        realtime_errors: metrics
            .realtime_errors
            .lock()
            .expect("realtime error lock")
            .clone(),
        http_errors: metrics.http_errors.lock().expect("HTTP error lock").clone(),
        realtime_failures,
        endpoint_latency_ms,
        passed,
        verdict,
    }
}

async fn run_once(args: &Args, users: u32) -> Result<Report> {
    raise_nofile_limit(args.nofile_limit)?;
    let started = Instant::now();
    let metrics = metrics();
    let client = Client::builder()
        .cookie_store(true)
        .pool_max_idle_per_host(32)
        .build()?;
    let (cookie, admin_id) = login(
        &client,
        &metrics,
        args,
        &args.admin_user,
        &args.admin_password,
    )
    .await?;
    let csrf = cookie
        .split("x-csrf-token=")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    if matches!(args.mode, Mode::Api | Mode::Readonly) || args.full_api {
        api_smoke(
            args,
            metrics.clone(),
            client,
            &cookie,
            &csrf,
            admin_id,
            args.full_api,
        )
        .await?;
    }
    sleep(Duration::from_secs(args.warmup)).await;
    let mut tasks = JoinSet::new();
    let setup_deadline = Instant::now() + Duration::from_secs(args.setup_timeout);
    let auth_gate = Arc::new(SetupGate::new(setup_deadline));
    let traffic_gate = Arc::new(SetupGate::new(setup_deadline));
    let shared_login_client = Client::builder()
        .pool_max_idle_per_host(128)
        .tcp_nodelay(true)
        .build()?;
    let shared_traffic_client = Client::builder()
        .pool_max_idle_per_host(256)
        .tcp_nodelay(true)
        .build()?;
    for number in 0..users {
        tasks.spawn(user_loop(
            args.clone(),
            metrics.clone(),
            shared_login_client.clone(),
            shared_traffic_client.clone(),
            number,
            Duration::from_secs(args.duration),
            auth_gate.clone(),
            traffic_gate.clone(),
        ));
    }
    while auth_gate.ready.load(Ordering::Acquire) < users as u64
        && Instant::now() < auth_gate.deadline
    {
        sleep(Duration::from_millis(10)).await;
    }
    // All WebSocket-mode workers have dropped their clone at this point.
    // Dropping the final handle closes the idle login connection pool before
    // thousands of long-lived sockets are opened.
    drop(shared_login_client);
    auth_gate.released.store(true, Ordering::Release);
    auth_gate.notify.notify_waiters();
    while traffic_gate.ready.load(Ordering::Acquire) < users as u64
        && Instant::now() < traffic_gate.deadline
    {
        sleep(Duration::from_millis(10)).await;
    }
    traffic_gate.released.store(true, Ordering::Release);
    traffic_gate.notify.notify_waiters();
    while tasks.join_next().await.is_some() {}
    Ok(make_report(args, &metrics, Some(users), started.elapsed()))
}

fn print_report(report: &Report) {
    println!("{}", report.verdict);
    println!(
        "users={} authenticated={} ws={}/{} ws_errors={} realtime_errors={} setup_errors={} throttles={} http_requests={} api_requests={} api_operations={} req/s={:.1} errors={} ({:.2}%) messages={}/{} acknowledged",
        report
            .requested_concurrency
            .map(|v| v.to_string())
            .unwrap_or_else(|| "capacity".into()),
        report.authenticated_clients,
        report.websocket_connections,
        report.websocket_attempts,
        report.websocket_failures,
        report.realtime_failures,
        report.setup_failures,
        report.auth_throttles,
        report.requests,
        report.api_requests,
        report.api_operations_exercised,
        report.requests_per_second,
        report.failures,
        report.error_rate * 100.0,
        report.messages_acknowledged,
        report.messages_sent
    );
    if !report.login_statuses.is_empty() {
        println!("login_statuses={:?}", report.login_statuses);
    }
    if !report.login_errors.is_empty() {
        println!("login_errors={:?}", report.login_errors);
    }
    if !report.websocket_errors.is_empty() {
        println!("websocket_errors={:?}", report.websocket_errors);
    }
    if !report.realtime_errors.is_empty() {
        println!("realtime_errors={:?}", report.realtime_errors);
    }
    if !report.http_errors.is_empty() {
        println!("http_errors={:?}", report.http_errors);
    }
    if let Some(users) = report.supported_users_lower_bound {
        println!(
            "actual supported capacity for this environment: at least {users} concurrent {} users",
            report.mode
        );
    }
    for (name, p) in &report.endpoint_latency_ms {
        println!(
            "{name:42} n={} p50={}ms p95={}ms p99={}ms max={}ms",
            p.count, p.p50, p.p95, p.p99, p.max
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !args.capacity && args.concurrency.is_none() {
        bail!("provide exactly one --concurrency N, or use --capacity");
    }
    if args.full_api && !args.allow_mutations {
        bail!("--full-api mutates data; add --allow-mutations or use --readonly");
    }
    if args.full_api && args.readonly {
        bail!("--full-api and --readonly cannot be combined");
    }
    if args.capacity && args.max_users == 0 {
        bail!("--max-users must be positive");
    }
    let mut best = None;
    if args.capacity {
        let mut level = 1;
        let mut reports = Vec::new();
        while level <= args.max_users {
            let report = run_once(&args, level).await?;
            print_report(&report);
            let passed = report.passed;
            reports.push(report);
            if !passed {
                break;
            }
            best = Some(level);
            level = level.saturating_mul(2);
            if level == 0 {
                break;
            }
        }
        println!(
            "actual supported capacity for this environment: {} concurrent mixed users",
            best.unwrap_or(0)
        );
        std::fs::write(
            &args.output,
            serde_json::to_vec_pretty(&json!({
                "capacity_estimate": best.unwrap_or(0),
                "runs": reports,
            }))?,
        )?;
    } else {
        let report = run_once(&args, args.concurrency.expect("validated concurrency")).await?;
        print_report(&report);
        std::fs::write(&args.output, serde_json::to_vec_pretty(&report)?)?;
        if !report.passed {
            std::process::exit(2);
        }
    }
    Ok(())
}

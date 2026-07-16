use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use axum::{
    body::Body,
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};

pub(crate) static AUTHENTICATIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACTIVE_WEBSOCKETS: AtomicU64 = AtomicU64::new(0);
pub(crate) static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn record_authentication() {
    AUTHENTICATIONS.fetch_add(1, Ordering::Relaxed);
    ::metrics::counter!("vussa_authentications_total").increment(1);
}

pub(crate) async fn track_http_request(request: Request<Body>, next: Next) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched")
        .to_string();
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();
    ::metrics::counter!(
        "vussa_http_requests_total",
        "method" => method,
        "route" => route.clone(),
        "status" => status
    )
    .increment(1);
    ::metrics::histogram!(
        "vussa_http_request_duration_seconds",
        "route" => route
    )
    .record(started.elapsed().as_secs_f64());
    response
}

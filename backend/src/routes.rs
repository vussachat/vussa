use super::*;
use axum::{Router, http::HeaderValue, middleware, routing::get};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

/// Assemble the HTTP and WebSocket surface in one place.
///
/// Keeping route registration separate from process startup makes the server
/// composition testable without binding a TCP listener or initializing a
/// second runtime-wide dependency pool.
pub(crate) fn build(state: Arc<AppState>) -> Router {
    let cors = match env::var("CORS_ORIGIN") {
        Ok(origin) => origin.parse::<HeaderValue>().map_or_else(
            |_| {
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any)
            },
            |origin| {
                CorsLayer::new()
                    .allow_origin(origin)
                    .allow_methods(Any)
                    .allow_headers(Any)
                    .allow_credentials(true)
            },
        ),
        Err(_) => CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    };
    Router::new()
        .nest(
            "/api/v1",
            Router::new()
                .route("/health", get(health))
                .route("/health/live", get(live))
                .route("/health/ready", get(ready))
                .route("/health/startup", get(startup))
                .route("/metrics", get(metrics))
                .route("/auth/register", axum::routing::post(register))
                .route("/auth/login", axum::routing::post(login))
                .route(
                    "/auth/recovery/request",
                    axum::routing::post(request_recovery),
                )
                .route("/auth/recovery/reset", axum::routing::post(reset_recovery))
                .route("/auth/logout", axum::routing::post(logout))
                .route("/auth/me", get(me))
                .route(
                    "/account",
                    axum::routing::patch(update_account).delete(delete_account),
                )
                .route("/account/export", get(export_account))
                .route("/account/password", axum::routing::patch(change_password))
                .route("/account/sessions", get(list_sessions))
                .route(
                    "/account/sessions/{id}",
                    axum::routing::delete(revoke_session),
                )
                .route("/profile", get(profile).patch(update_profile))
                .route("/admin/users", get(admin_users))
                .route(
                    "/admin/users/{user}/disable",
                    axum::routing::post(admin_disable_user),
                )
                .route(
                    "/admin/users/{user}/enable",
                    axum::routing::post(admin_enable_user),
                )
                .route(
                    "/admin/users/{user}",
                    axum::routing::delete(admin_delete_user),
                )
                .route(
                    "/admin/users/{user}/password-reset",
                    axum::routing::post(admin_reset_password),
                )
                .route(
                    "/admin/users/{user}/invalidate-sessions",
                    axum::routing::post(admin_invalidate_sessions),
                )
                .route(
                    "/admin/users/{user}/roles/{role}",
                    axum::routing::post(admin_assign_role),
                )
                .route(
                    "/admin/users/{user}/roles/{role}",
                    axum::routing::delete(admin_remove_role),
                )
                .route("/admin/audit", get(admin_audit))
                .route("/admin/roles", get(admin_roles))
                .route("/admin/permissions", get(admin_permissions))
                .route("/admin/participants/{channel}", get(admin_participants))
                .route("/admin/operations", get(admin_operations))
                .route("/admin/bans", get(admin_bans).post(admin_create_ban))
                .route("/admin/bans/{id}", axum::routing::delete(admin_revoke_ban))
                .route(
                    "/admin/channels",
                    get(admin_channels).post(admin_create_channel),
                )
                .route(
                    "/admin/channels/{id}",
                    axum::routing::patch(admin_update_channel),
                )
                .route(
                    "/admin/channels/{id}/{action}",
                    axum::routing::post(admin_channel_state),
                )
                .route("/admin/messages", get(admin_messages))
                .route(
                    "/admin/messages/bulk-moderate",
                    axum::routing::post(admin_bulk_moderate),
                )
                .route("/admin/messages/{id}/history", get(admin_message_history))
                .route(
                    "/admin/messages/{id}/{action}",
                    axum::routing::post(admin_moderate_message),
                )
                .route("/channels", get(list_channels).post(create_channel))
                .route("/unread", get(list_unread))
                .route("/users/search", get(search_users))
                .route("/messages/search", get(search_messages))
                .route("/link-preview", get(link_preview))
                .route("/messages/saved", get(list_saved_messages))
                .route("/messages/{id}", get(get_message))
                .route("/messages/{id}/permalink", get(message_permalink))
                .route(
                    "/messages/{id}/save",
                    axum::routing::post(save_message).delete(unsave_message),
                )
                .route("/reports", axum::routing::post(report_message))
                .route("/moderation/reports", get(list_message_reports))
                .route(
                    "/moderation/reports/{id}/{action}",
                    axum::routing::post(update_message_report),
                )
                .route(
                    "/files",
                    axum::routing::post(upload_file)
                        .layer(axum::extract::DefaultBodyLimit::max(MAX_FILE_BYTES)),
                )
                .route("/files/{id}", get(download_file))
                .route("/notifications", get(list_notifications))
                .route(
                    "/notifications/preferences",
                    get(notification_preferences).patch(update_notification_preferences),
                )
                .route("/notifications/config", get(notification_config))
                .route(
                    "/notifications/subscriptions",
                    get(notification_subscriptions).post(save_notification_subscription),
                )
                .route(
                    "/notifications/subscriptions/{id}",
                    axum::routing::delete(delete_notification_subscription),
                )
                .route(
                    "/notifications/{id}/read",
                    axum::routing::post(mark_notification_read),
                )
                .route("/conversations", get(list_conversations))
                .route(
                    "/conversations/direct",
                    axum::routing::post(open_direct_conversation),
                )
                .route(
                    "/channels/private",
                    axum::routing::post(create_private_channel),
                )
                .route(
                    "/channels/{name}/members",
                    get(list_channel_members).post(invite_channel_member),
                )
                .route(
                    "/channels/{name}/members/{user_id}",
                    axum::routing::delete(remove_channel_member),
                )
                .route(
                    "/channels/{name}/membership",
                    axum::routing::delete(leave_channel),
                )
                .route(
                    "/channels/{name}/owner",
                    axum::routing::post(transfer_channel_ownership),
                )
                .route(
                    "/channels/{name}/members/{user_id}/moderator",
                    axum::routing::post(promote_channel_moderator).delete(demote_channel_moderator),
                )
                .route(
                    "/channels/{name}/favorite",
                    axum::routing::post(favorite_channel).delete(unfavorite_channel),
                )
                .route("/favorites", get(list_favorite_channels))
                .route(
                    "/drafts/{channel}",
                    get(get_draft).put(update_draft).delete(delete_draft),
                )
                .route(
                    "/channels/{name}/invite-links",
                    axum::routing::post(create_invite_link),
                )
                .route(
                    "/invite-links/{token}/accept",
                    axum::routing::post(accept_invite_link),
                )
                .route("/ws", get(websocket))
                .layer(middleware::from_fn(metrics::track_http_request)),
        )
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

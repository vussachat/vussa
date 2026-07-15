use super::*;
use axum::{Router, routing::get};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

/// Assemble the HTTP and WebSocket surface in one place.
///
/// Keeping route registration separate from process startup makes the server
/// composition testable without binding a TCP listener or initializing a
/// second runtime-wide dependency pool.
pub(crate) fn build(state: Arc<AppState>) -> Router {
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
                .route("/auth/logout", axum::routing::post(logout))
                .route("/auth/me", get(me))
                .route("/account", axum::routing::patch(update_account))
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
                .route("/users/search", get(search_users))
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
                .route("/ws", get(websocket)),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

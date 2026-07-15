use super::{AuthUser, ChatMessage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateChannelRequest {
    pub(crate) name: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct CreateChannelResponse {
    pub(crate) name: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct DirectConversationRequest {
    pub(crate) user_id: Uuid,
}
#[derive(Debug, Deserialize)]
pub(crate) struct MemberRequest {
    pub(crate) user_id: Uuid,
}
#[derive(Debug, Serialize)]
pub(crate) struct UserSearchResult {
    pub(crate) id: Uuid,
    pub(crate) username: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct MessageSearchResult {
    pub(crate) message: ChatMessage,
    pub(crate) snippet: String,
    pub(crate) highlighted: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct FileUploadResponse {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) content_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) download_url: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct NotificationView {
    pub(crate) id: Uuid,
    pub(crate) kind: String,
    pub(crate) message_id: Option<Uuid>,
    pub(crate) channel_id: Option<Uuid>,
    pub(crate) body: String,
    pub(crate) created_at: i64,
    pub(crate) read_at: Option<i64>,
}
#[derive(Debug, Serialize)]
pub(crate) struct NotificationPreferencesView {
    pub(crate) mentions: bool,
    pub(crate) direct_messages: bool,
    pub(crate) channel_messages: bool,
    pub(crate) email_enabled: bool,
    pub(crate) browser_push_enabled: bool,
}
#[derive(Debug, Deserialize)]
pub(crate) struct NotificationPreferencesUpdate {
    pub(crate) mentions: Option<bool>,
    pub(crate) direct_messages: Option<bool>,
    pub(crate) channel_messages: Option<bool>,
    pub(crate) email_enabled: Option<bool>,
    pub(crate) browser_push_enabled: Option<bool>,
}
#[derive(Debug, Serialize)]
pub(crate) struct NotificationSubscriptionView {
    pub(crate) id: Uuid,
    pub(crate) endpoint: String,
    pub(crate) p256dh: String,
    pub(crate) auth: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct NotificationSubscriptionRequest {
    pub(crate) endpoint: String,
    pub(crate) p256dh: String,
    pub(crate) auth: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct ProfileView {
    pub(crate) id: Uuid,
    pub(crate) username: String,
    pub(crate) display_name: String,
    pub(crate) custom_status: String,
    pub(crate) status_expires_at: Option<i64>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct ProfileUpdateRequest {
    pub(crate) display_name: Option<String>,
    pub(crate) custom_status: Option<String>,
    pub(crate) status_expires_at: Option<i64>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct DraftUpdateRequest {
    pub(crate) body: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct MessageReportRequest {
    pub(crate) message_id: Uuid,
    pub(crate) reason: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct RegisterRequest {
    pub(crate) email: String,
    pub(crate) username: String,
    pub(crate) password: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct LoginRequest {
    pub(crate) email: String,
    pub(crate) password: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct RecoveryRequest {
    pub(crate) email: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct RecoveryResetRequest {
    pub(crate) token: String,
    pub(crate) password: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct AccountUpdateRequest {
    pub(crate) username: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct PasswordChangeRequest {
    pub(crate) current_password: String,
    pub(crate) new_password: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct SessionView {
    pub(crate) id: Uuid,
    pub(crate) current: bool,
}
#[derive(Debug, Deserialize, Default)]
pub(crate) struct AdminListQuery {
    pub(crate) q: Option<String>,
    pub(crate) after: Option<Uuid>,
    pub(crate) limit: Option<i64>,
    pub(crate) channel: Option<String>,
    pub(crate) user: Option<Uuid>,
    pub(crate) from: Option<i64>,
    pub(crate) to: Option<i64>,
    pub(crate) deleted: Option<bool>,
    pub(crate) actor: Option<Uuid>,
    pub(crate) action: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) target: Option<Uuid>,
    pub(crate) before_created_at: Option<i64>,
    pub(crate) before_id: Option<Uuid>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct LinkPreviewQuery {
    pub(crate) url: String,
}
#[derive(Debug, Serialize)]
pub(crate) struct AdminUserView {
    #[serde(flatten)]
    pub(crate) user: AuthUser,
    pub(crate) disabled_at: Option<i64>,
    pub(crate) deleted_at: Option<i64>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
}
#[derive(Debug, Deserialize)]
pub(crate) struct PasswordResetRequest {
    pub(crate) password: String,
}
#[derive(Debug, Deserialize)]
pub(crate) struct ChannelAdminRequest {
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) retention_days: Option<i32>,
    pub(crate) posting_restricted: Option<bool>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct ModerationRequest {
    pub(crate) reason: Option<String>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct BulkModerationRequest {
    pub(crate) ids: Vec<Uuid>,
    pub(crate) action: String,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InviteLinkRequest {
    pub(crate) expires_at: Option<i64>,
    pub(crate) max_uses: Option<i32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InviteLinkResponse {
    pub(crate) token: String,
    pub(crate) expires_at: Option<i64>,
    pub(crate) max_uses: i32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BanRequest {
    pub(crate) user_id: Uuid,
    pub(crate) channel: Option<String>,
    pub(crate) reason: String,
    pub(crate) expires_at: Option<i64>,
}

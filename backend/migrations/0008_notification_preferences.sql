CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    mentions BOOLEAN NOT NULL DEFAULT TRUE,
    direct_messages BOOLEAN NOT NULL DEFAULT TRUE,
    channel_messages BOOLEAN NOT NULL DEFAULT FALSE,
    email_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    browser_push_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at BIGINT NOT NULL
);

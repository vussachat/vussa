CREATE TABLE IF NOT EXISTS notification_subscriptions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    endpoint TEXT NOT NULL,
    p256dh TEXT NOT NULL,
    auth TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE (user_id, endpoint)
);

CREATE INDEX IF NOT EXISTS notification_subscriptions_user_idx
    ON notification_subscriptions (user_id, updated_at DESC, id);

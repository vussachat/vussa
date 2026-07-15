CREATE TABLE IF NOT EXISTS user_bans (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id UUID REFERENCES channels(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    expires_at BIGINT,
    created_at BIGINT NOT NULL,
    revoked_at BIGINT,
    revoked_by UUID REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS user_bans_active_lookup_idx
    ON user_bans (user_id, channel_id, expires_at)
    WHERE revoked_at IS NULL;

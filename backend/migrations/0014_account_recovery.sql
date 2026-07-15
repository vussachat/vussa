CREATE TABLE IF NOT EXISTS account_recovery_tokens (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    used_at BIGINT
);

CREATE INDEX IF NOT EXISTS account_recovery_active_idx
    ON account_recovery_tokens (token_hash, expires_at)
    WHERE used_at IS NULL;

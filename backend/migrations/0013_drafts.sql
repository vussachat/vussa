CREATE TABLE IF NOT EXISTS channel_drafts (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, channel_id),
    CONSTRAINT channel_drafts_body_length CHECK (char_length(body) <= 2000)
);

CREATE INDEX IF NOT EXISTS channel_drafts_updated_idx
    ON channel_drafts (user_id, updated_at DESC);

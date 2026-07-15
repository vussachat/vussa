CREATE TABLE IF NOT EXISTS saved_messages (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, message_id)
);

CREATE TABLE IF NOT EXISTS message_reports (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    reporter_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at BIGINT NOT NULL,
    resolved_at BIGINT,
    resolved_by UUID REFERENCES users(id) ON DELETE SET NULL,
    UNIQUE (message_id, reporter_user_id)
);

CREATE INDEX IF NOT EXISTS message_reports_queue_idx
    ON message_reports (status, created_at DESC, id);

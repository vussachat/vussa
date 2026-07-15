ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at BIGINT;

ALTER TABLE channels ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
ALTER TABLE channels ADD COLUMN IF NOT EXISTS archived_at BIGINT;
ALTER TABLE channels ADD COLUMN IF NOT EXISTS retention_days INTEGER NOT NULL DEFAULT 90;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_at BIGINT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_by UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS deletion_reason TEXT;

CREATE TABLE IF NOT EXISTS message_edit_history (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    editor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    previous_text TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS users_admin_search_idx
    ON users (lower(username), id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS users_admin_email_idx
    ON users (lower(email), id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS channels_admin_state_idx
    ON channels (deleted_at, archived_at, name, id);
CREATE INDEX IF NOT EXISTS messages_admin_search_idx
    ON messages (created_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS message_edit_history_message_idx
    ON message_edit_history (message_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS audit_events_filter_idx
    ON audit_events (action, target_type, created_at DESC, id DESC);

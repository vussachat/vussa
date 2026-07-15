CREATE TABLE IF NOT EXISTS message_reactions (
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    emoji TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (message_id, user_id, emoji),
    CONSTRAINT message_reactions_emoji_check CHECK (char_length(emoji) BETWEEN 1 AND 64)
);

CREATE INDEX IF NOT EXISTS message_reactions_message_idx
    ON message_reactions (message_id, emoji, created_at, user_id);

ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_id TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS root_message_id UUID REFERENCES messages(id) ON DELETE CASCADE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS custom_status TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS status_expires_at BIGINT;

CREATE INDEX IF NOT EXISTS messages_root_created_idx
    ON messages (root_message_id, created_at, id)
    WHERE root_message_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS messages_owner_client_idx
    ON messages (owner_session, client_id)
    WHERE client_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS channel_reads (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    last_read_created_at BIGINT NOT NULL,
    last_read_message_id UUID,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, channel_id)
);

CREATE TABLE IF NOT EXISTS files (
    id UUID PRIMARY KEY,
    uploader_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    storage_key TEXT NOT NULL UNIQUE,
    original_name TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    checksum TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    deleted_at BIGINT
);

CREATE INDEX IF NOT EXISTS files_uploader_created_idx
    ON files (uploader_user_id, created_at DESC, id);

CREATE TABLE IF NOT EXISTS message_files (
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    file_id UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    PRIMARY KEY (message_id, file_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    message_id UUID REFERENCES messages(id) ON DELETE CASCADE,
    channel_id UUID REFERENCES channels(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    read_at BIGINT
);

CREATE INDEX IF NOT EXISTS notifications_user_created_idx
    ON notifications (user_id, created_at DESC, id)
    WHERE read_at IS NULL;

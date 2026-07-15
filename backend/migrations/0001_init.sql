CREATE TABLE IF NOT EXISTS channels (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at BIGINT NOT NULL,
    deleted_at BIGINT
);

CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY,
    channel_id UUID NOT NULL REFERENCES channels(id),
    username TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    edited BOOLEAN NOT NULL DEFAULT FALSE,
    owner_session UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_channel_created_idx
    ON messages (channel_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS channels_active_name_idx
    ON channels (name)
    WHERE deleted_at IS NULL;

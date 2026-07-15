ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'public',
    ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS direct_key TEXT;

ALTER TABLE channels
    DROP CONSTRAINT IF EXISTS channels_kind_check;

ALTER TABLE channels
    ADD CONSTRAINT channels_kind_check CHECK (kind IN ('public', 'private', 'direct'));

CREATE UNIQUE INDEX IF NOT EXISTS channels_direct_key_idx
    ON channels (direct_key)
    WHERE kind = 'direct' AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS channel_members (
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    membership_role TEXT NOT NULL DEFAULT 'member',
    invited_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (channel_id, user_id),
    CONSTRAINT channel_members_role_check CHECK (membership_role IN ('owner', 'member'))
);

CREATE INDEX IF NOT EXISTS channel_members_user_idx
    ON channel_members (user_id, channel_id);
CREATE INDEX IF NOT EXISTS channel_members_channel_idx
    ON channel_members (channel_id, membership_role, user_id);
CREATE INDEX IF NOT EXISTS channels_private_owner_idx
    ON channels (owner_user_id, created_at DESC, id)
    WHERE kind IN ('private', 'direct') AND deleted_at IS NULL;

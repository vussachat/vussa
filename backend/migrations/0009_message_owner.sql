ALTER TABLE messages ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;

UPDATE messages m
SET owner_user_id = u.id
FROM users u
WHERE m.owner_user_id IS NULL AND lower(m.username) = lower(u.username);

CREATE INDEX IF NOT EXISTS messages_owner_user_created_idx
    ON messages (owner_user_id, created_at DESC, id DESC);

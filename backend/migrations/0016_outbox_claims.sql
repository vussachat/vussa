ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_at BIGINT;

CREATE INDEX IF NOT EXISTS outbox_claimed_idx
    ON outbox_events (claimed_at, created_at, id)
    WHERE published_at IS NULL;

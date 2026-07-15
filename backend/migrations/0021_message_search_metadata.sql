ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS mentions TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS search_vector TSVECTOR
    GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, text)) STORED;

CREATE INDEX IF NOT EXISTS messages_search_vector_idx
    ON messages USING GIN (search_vector);

CREATE INDEX IF NOT EXISTS messages_mentions_idx
    ON messages USING GIN (mentions);

ALTER TABLE channel_members
    DROP CONSTRAINT IF EXISTS channel_members_role_check;

ALTER TABLE channel_members
    ADD CONSTRAINT channel_members_role_check
    CHECK (membership_role IN ('owner', 'moderator', 'member'));

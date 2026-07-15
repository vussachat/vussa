CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    disabled_at BIGINT,
    role_version BIGINT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_idx ON users (lower(email));
CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_idx ON users (lower(username));
CREATE INDEX IF NOT EXISTS users_active_created_idx ON users (created_at DESC, id DESC)
    WHERE disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS roles (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS permissions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    assigned_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, role_id)
);

CREATE INDEX IF NOT EXISTS user_roles_user_idx ON user_roles (user_id, role_id);

CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY,
    actor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id UUID,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_events_created_idx ON audit_events (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS audit_events_actor_created_idx
    ON audit_events (actor_user_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at BIGINT NOT NULL,
    published_at BIGINT,
    attempts INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

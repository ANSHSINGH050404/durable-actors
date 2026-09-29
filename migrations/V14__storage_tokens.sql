CREATE TABLE durable_actors_storage_tokens (
    scope_key TEXT PRIMARY KEY,
    issuer TEXT NOT NULL,
    boundary JSONB NOT NULL,
    token TEXT,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    refresh_owner TEXT,
    refresh_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX durable_actors_storage_tokens_expiry ON durable_actors_storage_tokens (expires_at);

CREATE TABLE durable_actors_replication_groups (
    prefix TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'closing', 'archived')),
    ever_ready BOOLEAN NOT NULL DEFAULT FALSE,
    checkpoint TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE durable_actors_replication_pods (
    name TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('starting', 'ready', 'bound', 'retiring')),
    config TEXT NOT NULL,
    group_prefix TEXT REFERENCES durable_actors_replication_groups(prefix),
    slot INTEGER,
    UNIQUE (group_prefix, slot)
);
CREATE INDEX durable_actors_replication_spares ON durable_actors_replication_pods(zone, state) WHERE group_prefix IS NULL;

CREATE TABLE socket_gateways (
    id TEXT PRIMARY KEY,
    route TEXT NOT NULL,
    accepts_rooms BOOLEAN NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE socket_rooms (
    actor_key TEXT PRIMARY KEY,
    gateway_id TEXT NOT NULL REFERENCES socket_gateways(id) ON DELETE CASCADE
);
CREATE INDEX socket_rooms_gateway ON socket_rooms(gateway_id);

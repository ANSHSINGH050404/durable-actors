ALTER TABLE durable_actors_trace_projects ADD COLUMN dropped BIGINT NOT NULL DEFAULT 0;
ALTER TABLE durable_actors_trace_projects ADD COLUMN persistence_failed BOOLEAN NOT NULL DEFAULT FALSE;

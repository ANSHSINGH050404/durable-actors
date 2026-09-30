ALTER TABLE durable_actors_replication_groups
    DROP CONSTRAINT durable_actors_replication_groups_state_check;
ALTER TABLE durable_actors_replication_groups
    ADD CHECK (state IN ('creating', 'ready', 'switching', 'bucket', 'closing', 'archived'));

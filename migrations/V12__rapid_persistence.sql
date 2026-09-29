DROP TABLE durable_actors_replica_groups;
DELETE FROM durable_actors_spares WHERE kind = 'replica';
DELETE FROM durable_actors_pool_backoffs WHERE kind = 'replica';
ALTER TABLE durable_actors_spares DROP CONSTRAINT durable_actors_spares_kind_check;
ALTER TABLE durable_actors_spares ADD CONSTRAINT durable_actors_spares_kind_check CHECK (kind = 'actor');
ALTER TABLE durable_actors_pool_backoffs DROP CONSTRAINT durable_actors_pool_backoffs_kind_check;
ALTER TABLE durable_actors_pool_backoffs ADD CONSTRAINT durable_actors_pool_backoffs_kind_check CHECK (kind = 'actor');

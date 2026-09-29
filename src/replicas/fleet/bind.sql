INSERT INTO durable_actors_replication_pods (name, zone, state, config, group_prefix, slot)
SELECT config->>'name', config->>'zone',
    CASE WHEN config->>'placement' IS NULL THEN 'starting' ELSE 'bound' END,
    config::text, $1, (ordinality - 1)::integer
FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY AS pods(config, ordinality)
ON CONFLICT (name) DO UPDATE
SET state = EXCLUDED.state, group_prefix = EXCLUDED.group_prefix, slot = EXCLUDED.slot

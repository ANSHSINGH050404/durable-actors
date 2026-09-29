SELECT g.state, g.ever_ready, g.checkpoint,
    COALESCE((SELECT json_agg(p.config::json ORDER BY p.slot)
        FROM durable_actors_replication_pods p WHERE p.group_prefix = g.prefix), '[]'::json)
FROM durable_actors_replication_groups g
WHERE g.prefix = $1

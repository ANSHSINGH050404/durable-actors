INSERT INTO durable_actors_storage_tokens (scope_key,issuer,boundary,refresh_owner,refresh_until)
VALUES ($1,$2,$3,$4,clock_timestamp()+interval '30 seconds')
ON CONFLICT (scope_key) DO UPDATE SET
    last_used_at=CASE WHEN $5 THEN clock_timestamp() ELSE durable_actors_storage_tokens.last_used_at END,
    refresh_owner=CASE WHEN durable_actors_storage_tokens.expires_at<clock_timestamp()+interval '5 minutes'
        AND durable_actors_storage_tokens.refresh_until<=clock_timestamp() THEN $4 ELSE durable_actors_storage_tokens.refresh_owner END,
    refresh_until=CASE WHEN durable_actors_storage_tokens.expires_at<clock_timestamp()+interval '5 minutes'
        AND durable_actors_storage_tokens.refresh_until<=clock_timestamp() THEN clock_timestamp()+interval '30 seconds' ELSE durable_actors_storage_tokens.refresh_until END
RETURNING token,(extract(epoch FROM expires_at)*1000)::bigint,
    token IS NOT NULL AND expires_at>clock_timestamp()+interval '1 minute',refresh_owner

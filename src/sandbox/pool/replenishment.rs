use super::*;
use tokio_postgres::Transaction;

impl PoolStore {
    pub(super) async fn replenish(
        &self,
        key: &str,
        config: &PoolConfig,
        batch: u32,
    ) -> Result<Vec<String>> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext('durable-actors'), hashtext('spare-capacity'))",
            &[],
        )
        .await?;
        tx.execute("INSERT INTO durable_actors_pool_backoffs (pool_key, kind) VALUES ($1, $2) ON CONFLICT DO NOTHING", &[&key, &self.1.as_str()]).await?;
        let row = tx.query_one("SELECT retry_after <= clock_timestamp() AS can_build FROM durable_actors_pool_backoffs WHERE pool_key = $1", &[&key]).await?;
        trim(&tx, key, config.idle).await?;
        let count = if row.get::<_, bool>("can_build") {
            capacity(&tx, key, config).await?.min(batch)
        } else {
            0
        };
        let names = reserve(&tx, key, self.1, count).await?;
        tx.commit().await?;
        Ok(names)
    }

    pub(super) async fn build_failed(&self, key: &str, name: &str) -> Result<()> {
        self.0.execute("WITH backoff AS (UPDATE durable_actors_pool_backoffs SET failures = least(failures + 1, 6), retry_after = clock_timestamp() + make_interval(secs => power(2, least(failures, 5))) WHERE pool_key = $1 RETURNING pool_key) UPDATE durable_actors_spares SET status = 'retiring' WHERE name = $2 AND status = 'starting' AND pool_key IN (SELECT pool_key FROM backoff)", &[&key, &name]).await?;
        Ok(())
    }
}

async fn trim(tx: &Transaction<'_>, key: &str, target: u32) -> Result<()> {
    // Keep replacements in flight and retire the oldest ready spare only after the new one is ready.
    tx.execute("UPDATE durable_actors_spares SET status = 'retiring' WHERE status = 'ready' AND name IN (SELECT name FROM (SELECT name, row_number() OVER (ORDER BY expires_at DESC, name) AS rank FROM durable_actors_spares WHERE pool_key = $1 AND status = 'ready') spares WHERE rank > $2)", &[&key, &i64::from(target)]).await?;
    Ok(())
}

async fn capacity(tx: &Transaction<'_>, key: &str, config: &PoolConfig) -> Result<u32> {
    // Allow the build timeout for replacement, bounded by half the spare's lifetime.
    let replacement_seconds = (config.idle_ttl_seconds / 2).min(120);
    let row = tx.query_one("SELECT count(*) FILTER (WHERE pool_key = $1 AND expires_at > clock_timestamp() AND (status = 'starting' OR (status = 'ready' AND expires_at > clock_timestamp() + make_interval(secs => $2)))) AS available, count(*) FILTER (WHERE pool_key = $1) AS occupied, count(*) AS fleet, count(*) FILTER (WHERE status = 'starting') AS starting FROM durable_actors_spares WHERE status IN ('ready', 'starting') OR (status = 'retiring' AND host_id IS NULL AND handle IS NOT NULL)", &[&key, &f64::from(replacement_seconds)]).await?;
    let remaining = |limit: u32, field| {
        limit.saturating_sub(row.get::<_, i64>(field).min(i64::from(u32::MAX)) as u32)
    };
    Ok(remaining(config.idle, "available")
        .min(remaining(config.idle.saturating_mul(2), "occupied"))
        .min(remaining(config.fleet_maximum, "fleet"))
        .min(remaining(config.max_starting, "starting")))
}

async fn reserve(
    tx: &Transaction<'_>,
    key: &str,
    kind: SpareKind,
    count: u32,
) -> Result<Vec<String>> {
    let names: Vec<_> = (0..count)
        .map(|_| format!("do-spare-{}", uuid::Uuid::new_v4().simple()))
        .collect();
    if !names.is_empty() {
        tx.execute("INSERT INTO durable_actors_spares (name, pool_key, kind, status, expires_at) SELECT unnest($1::text[]), $2, $3, 'starting', clock_timestamp() + interval '120 seconds'", &[&names, &key, &kind.as_str()]).await?;
    }
    Ok(names)
}

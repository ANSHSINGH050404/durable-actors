use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{bucket::ReplicaPlacement, postgres::PostgresDatabase};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PodRecord {
    pub name: String,
    pub zone: String,
    pub uid: Option<String>,
    pub node: Option<String>,
    pub placement: Option<ReplicaPlacement>,
}
impl PodRecord {
    fn pending(zone: &str) -> Self {
        Self {
            name: format!("do-replica-{}", uuid::Uuid::new_v4().simple()),
            zone: zone.into(),
            uid: None,
            node: None,
            placement: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub object: String,
    pub digest: String,
}

#[derive(Clone)]
pub(super) struct GroupRecord {
    pub prefix: String,
    pub state: String,
    pub ever_ready: bool,
    pub checkpoint: Option<Checkpoint>,
    pub pods: Vec<PodRecord>,
}

#[derive(Clone)]
pub(super) struct Registry(PostgresDatabase);
impl Registry {
    pub fn new(database: PostgresDatabase) -> Self {
        Self(database)
    }

    pub async fn claim(&self, prefix: &str, zones: &[String]) -> Result<GroupRecord> {
        let mut connection = self.0.connection().await?;
        let tx = connection.transaction().await?;
        let lock = tx
            .prepare_cached("SELECT pg_advisory_xact_lock(hashtext('replica-group'), hashtext($1))")
            .await?;
        tx.query_one(&lock, &[&prefix]).await?;
        let select = tx.prepare_cached("SELECT state,ever_ready,checkpoint FROM durable_actors_replication_groups WHERE prefix=$1 FOR UPDATE").await?;
        let group = if let Some(row) = tx.query_opt(&select, &[&prefix]).await? {
            ensure!(
                matches!(row.get::<_, &str>(0), "creating" | "ready"),
                "replica group is permanently closed"
            );
            let pods = tx.prepare_cached("SELECT config FROM durable_actors_replication_pods WHERE group_prefix=$1 ORDER BY slot").await?;
            let pods = tx
                .query(&pods, &[&prefix])
                .await?
                .iter()
                .map(|row| serde_json::from_str(row.get(0)))
                .collect::<Result<Vec<_>, _>>()?;
            record(prefix, &row, pods)?
        } else {
            let insert = tx.prepare_cached("INSERT INTO durable_actors_replication_groups(prefix,state) VALUES($1,'creating') RETURNING state,ever_ready,checkpoint").await?;
            let row = tx.query_one(&insert, &[&prefix]).await?;
            record(prefix, &row, claim_pods(&tx, prefix, zones).await?)?
        };
        tx.commit().await?;
        Ok(group)
    }

    pub async fn lookup(&self, prefix: &str) -> Result<GroupRecord> {
        let row = self
            .0
            .query_opt(include_str!("lookup.sql"), &[&prefix])
            .await?
            .context("replica group missing")?;
        record(prefix, &row, serde_json::from_value(row.get(3))?)
    }

    pub async fn update_pod(&self, pod: &PodRecord) -> Result<()> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        let row = tx.query_opt("SELECT config,group_prefix,state FROM durable_actors_replication_pods WHERE name=$1 FOR UPDATE", &[&pod.name]).await?.context("replica registration was removed")?;
        let previous: PodRecord = serde_json::from_str(row.get(0))?;
        ensure!(
            previous.uid.is_none() || previous.uid == pod.uid,
            "replica pod was replaced"
        );
        ensure!(
            previous.placement.is_none() || previous.placement == pod.placement,
            "replica disk identity changed"
        );
        ensure!(row.get::<_, &str>(2) != "retiring", "replica is retiring");
        let state = if row.get::<_, Option<String>>(1).is_some() {
            "bound"
        } else {
            "ready"
        };
        if previous == *pod && row.get::<_, &str>(2) == state {
            tx.commit().await?;
            return Ok(());
        }
        tx.execute(
            "UPDATE durable_actors_replication_pods SET config=$2,state=$3 WHERE name=$1",
            &[&pod.name, &serde_json::to_string(pod)?, &state],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn ready(&self, prefix: &str) -> Result<()> {
        ensure!(self.0.execute("UPDATE durable_actors_replication_groups SET state='ready',ever_ready=TRUE,updated_at=clock_timestamp() WHERE prefix=$1 AND state IN ('creating','ready')", &[&prefix]).await? == 1, "replica group was closed during initialization");
        Ok(())
    }

    pub async fn close(&self, prefix: &str) -> Result<GroupRecord> {
        self.0.execute("INSERT INTO durable_actors_replication_groups(prefix,state) VALUES($1,'closing') ON CONFLICT(prefix) DO UPDATE SET state=CASE WHEN durable_actors_replication_groups.state='archived' THEN 'archived' ELSE 'closing' END,updated_at=clock_timestamp()", &[&prefix]).await?;
        self.lookup(prefix).await
    }

    pub async fn archived(&self, prefix: &str, checkpoint: Option<&Checkpoint>) -> Result<()> {
        let serialized = checkpoint.map(serde_json::to_string).transpose()?;
        let changed = self.0.execute("UPDATE durable_actors_replication_groups SET state='archived',checkpoint=$2,updated_at=clock_timestamp() WHERE prefix=$1 AND state='closing'", &[&prefix, &serialized]).await?;
        if changed == 0 {
            let previous = self.lookup(prefix).await?;
            ensure!(
                previous.state == "archived" && previous.checkpoint.as_ref() == checkpoint,
                "conflicting final replica checkpoints"
            );
        }
        Ok(())
    }

    pub async fn groups(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self.0.connection().await?.query("SELECT prefix FROM durable_actors_replication_groups WHERE starts_with(prefix,$1) ORDER BY prefix", &[&prefix]).await?.iter().map(|row| row.get(0)).collect())
    }

    pub async fn maintenance(&self) -> Result<Vec<String>> {
        Ok(self.0.connection().await?.query("WITH due AS (SELECT prefix FROM durable_actors_replication_groups g WHERE checked_at < clock_timestamp() - CASE WHEN state IN ('closing','archived') THEN interval '5 seconds' ELSE interval '30 seconds' END AND (state!='archived' OR EXISTS(SELECT 1 FROM durable_actors_replication_pods p WHERE p.group_prefix=g.prefix)) FOR UPDATE SKIP LOCKED) UPDATE durable_actors_replication_groups g SET checked_at=clock_timestamp() FROM due WHERE g.prefix=due.prefix RETURNING g.prefix", &[]).await?.iter().map(|row| row.get(0)).collect())
    }

    pub async fn registered_names(&self) -> Result<HashSet<String>> {
        Ok(self
            .0
            .connection()
            .await?
            .query("SELECT name FROM durable_actors_replication_pods", &[])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    pub async fn registered(&self, name: &str) -> Result<bool> {
        Ok(self
            .0
            .query_opt(
                "SELECT 1 FROM durable_actors_replication_pods WHERE name=$1",
                &[&name],
            )
            .await?
            .is_some())
    }

    pub async fn unassigned(&self) -> Result<Vec<PodRecord>> {
        self.0.connection().await?.query("SELECT config FROM durable_actors_replication_pods WHERE group_prefix IS NULL ORDER BY name", &[]).await?.iter().map(|row| serde_json::from_str(row.get(0)).map_err(Into::into)).collect()
    }

    pub async fn reserve_spares(
        &self,
        zones: &[String],
        target: usize,
        max_starting: usize,
    ) -> Result<()> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext('replica-pool'),0)",
            &[],
        )
        .await?;
        let rows = tx.query("SELECT zone,state,count(*) FROM durable_actors_replication_pods WHERE group_prefix IS NULL AND state!='retiring' GROUP BY zone,state", &[]).await?;
        let mut counts = BTreeMap::<String, usize>::new();
        let mut starting = 0;
        for row in rows {
            let count = usize::try_from(row.get::<_, i64>(2))?;
            *counts.entry(row.get(0)).or_default() += count;
            if row.get::<_, &str>(1) == "starting" {
                starting += count;
            }
        }
        let mut desired = BTreeMap::<&str, usize>::new();
        for i in 0..target {
            *desired.entry(&zones[i % zones.len()]).or_default() += 1;
        }
        for (zone, count) in desired {
            for _ in counts.get(zone).copied().unwrap_or_default()..count {
                if starting >= max_starting {
                    break;
                }
                let pod = PodRecord::pending(zone);
                tx.execute("INSERT INTO durable_actors_replication_pods(name,zone,state,config) VALUES($1,$2,'starting',$3)", &[&pod.name,&pod.zone,&serde_json::to_string(&pod)?]).await?;
                starting += 1;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn retire_spare(&self, name: &str) -> Result<bool> {
        Ok(self.0.execute("UPDATE durable_actors_replication_pods SET state='retiring' WHERE name=$1 AND group_prefix IS NULL", &[&name]).await? == 1)
    }

    pub async fn remove_pod(&self, name: &str) -> Result<()> {
        self.0.execute("DELETE FROM durable_actors_replication_pods p WHERE name=$1 AND ((group_prefix IS NULL AND state='retiring') OR EXISTS(SELECT 1 FROM durable_actors_replication_groups g WHERE g.prefix=p.group_prefix AND g.state='archived'))", &[&name]).await?;
        Ok(())
    }
}

async fn claim_pods(
    tx: &deadpool_postgres::Transaction<'_>,
    prefix: &str,
    zones: &[String],
) -> Result<Vec<PodRecord>> {
    let select = tx.prepare_cached("SELECT config FROM durable_actors_replication_pods WHERE group_prefix IS NULL AND state='ready' AND zone=$1 AND NOT ((config::jsonb->>'node') = ANY($2::text[])) ORDER BY name LIMIT 1 FOR UPDATE SKIP LOCKED").await?;
    let mut used_nodes: Vec<String> = Vec::new();
    let mut pods = Vec::with_capacity(zones.len());
    for zone in zones {
        let pod: PodRecord = match tx.query_opt(&select, &[zone, &used_nodes]).await? {
            Some(row) => serde_json::from_str(row.get(0))?,
            None => PodRecord::pending(zone),
        };
        if let Some(node) = &pod.node {
            used_nodes.push(node.clone());
        }
        pods.push(pod);
    }
    let bind = tx.prepare_cached(include_str!("bind.sql")).await?;
    tx.execute(&bind, &[&prefix, &serde_json::to_value(&pods)?])
        .await?;
    Ok(pods)
}

fn record(prefix: &str, row: &tokio_postgres::Row, pods: Vec<PodRecord>) -> Result<GroupRecord> {
    Ok(GroupRecord {
        prefix: prefix.into(),
        state: row.get(0),
        ever_ready: row.get(1),
        checkpoint: row
            .get::<_, Option<&str>>(2)
            .map(serde_json::from_str)
            .transpose()?,
        pods,
    })
}

#[cfg(test)]
#[path = "../../../tests/unit/replicas/registry.rs"]
mod tests;

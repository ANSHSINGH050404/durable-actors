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

    pub async fn lock(&self, prefix: &str) -> Result<GroupUpdate> {
        let client = deadpool_postgres::Object::take(self.0.connection().await?);
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtext('replica-transition'),hashtext($1))",
                &[&prefix],
            )
            .await?;
        Ok(GroupUpdate {
            client,
            prefix: prefix.into(),
        })
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
        self.0.connection().await?.query("SELECT config FROM durable_actors_replication_pods WHERE group_prefix IS NULL AND state!='retiring' ORDER BY name", &[]).await?.iter().map(|row| serde_json::from_str(row.get(0)).map_err(Into::into)).collect()
    }

    pub async fn retiring(&self) -> Result<Vec<PodRecord>> {
        self.0.connection().await?.query("SELECT config FROM durable_actors_replication_pods WHERE group_prefix IS NULL AND state='retiring'", &[]).await?.iter().map(|row| serde_json::from_str(row.get(0)).map_err(Into::into)).collect()
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
        let mut starting_by_zone = BTreeMap::<String, usize>::new();
        for row in rows {
            let count = usize::try_from(row.get::<_, i64>(2))?;
            *counts.entry(row.get(0)).or_default() += count;
            if row.get::<_, &str>(1) == "starting" {
                starting += count;
                *starting_by_zone.entry(row.get(0)).or_default() += count;
            }
        }
        let mut desired = BTreeMap::<&str, usize>::new();
        for i in 0..target {
            *desired.entry(&zones[i % zones.len()]).or_default() += 1;
        }
        let per_zone = max_starting.div_ceil(desired.len().max(1));
        for (zone, count) in desired {
            for _ in counts.get(zone).copied().unwrap_or_default()..count {
                if starting >= max_starting
                    || starting_by_zone.get(zone).copied().unwrap_or_default() >= per_zone
                {
                    break;
                }
                let pod = PodRecord::pending(zone);
                tx.execute("INSERT INTO durable_actors_replication_pods(name,zone,state,config) VALUES($1,$2,'starting',$3)", &[&pod.name,&pod.zone,&serde_json::to_string(&pod)?]).await?;
                starting += 1;
                *starting_by_zone.entry(zone.into()).or_default() += 1;
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

// All membership mutations use the lock-owning connection; losing it fences the transition.
pub(super) struct GroupUpdate {
    client: deadpool_postgres::ClientWrapper,
    prefix: String,
}

impl GroupUpdate {
    pub async fn ensure(&self) -> Result<()> {
        self.client.execute("INSERT INTO durable_actors_replication_groups(prefix,state) VALUES($1,'bucket') ON CONFLICT DO NOTHING", &[&self.prefix]).await?;
        Ok(())
    }

    pub async fn claim(&mut self, zones: &[String], eligible: &[String]) -> Result<()> {
        let tx = self.client.transaction().await?;
        let row = tx
            .query_opt(
                "SELECT state FROM durable_actors_replication_groups WHERE prefix=$1 FOR UPDATE",
                &[&self.prefix],
            )
            .await?;
        if let Some(row) = row {
            match row.get::<_, &str>(0) {
                "ready" | "creating" => return Ok(()),
                "bucket" => {}
                _ => anyhow::bail!("replica group is permanently closed or transitioning"),
            }
            tx.execute("UPDATE durable_actors_replication_groups SET state='creating',ever_ready=FALSE WHERE prefix=$1", &[&self.prefix]).await?;
        } else {
            tx.execute(
                "INSERT INTO durable_actors_replication_groups(prefix,state) VALUES($1,'creating')",
                &[&self.prefix],
            )
            .await?;
        }
        claim_pods(&tx, &self.prefix, zones, eligible).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn ready(&self) -> Result<()> {
        ensure!(self.client.execute("UPDATE durable_actors_replication_groups SET state='ready',ever_ready=TRUE,updated_at=clock_timestamp() WHERE prefix=$1 AND state='creating'", &[&self.prefix]).await? == 1, "replica group was closed during initialization");
        Ok(())
    }

    pub async fn switching(&self) -> Result<()> {
        ensure!(self.client.execute("UPDATE durable_actors_replication_groups SET state='switching',updated_at=clock_timestamp() WHERE prefix=$1 AND state IN ('creating','ready','switching','bucket')", &[&self.prefix]).await? == 1, "replica group is closed");
        Ok(())
    }

    pub async fn bucket(&mut self, checkpoint: Option<&Checkpoint>) -> Result<()> {
        let serialized = checkpoint.map(serde_json::to_string).transpose()?;
        let tx = self.client.transaction().await?;
        ensure!(tx.execute("UPDATE durable_actors_replication_groups SET state='bucket',ever_ready=FALSE,checkpoint=$2,updated_at=clock_timestamp() WHERE prefix=$1 AND state IN ('creating','switching','bucket')", &[&self.prefix, &serialized]).await? == 1, "replica group is closed");
        tx.execute("UPDATE durable_actors_replication_pods SET state='retiring',group_prefix=NULL,slot=NULL WHERE group_prefix=$1", &[&self.prefix]).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        ensure!(self.client.execute("UPDATE durable_actors_replication_groups SET checkpoint=$2,updated_at=clock_timestamp() WHERE prefix=$1 AND state='bucket'", &[&self.prefix, &serde_json::to_string(checkpoint)?]).await? == 1, "bucket writer was fenced");
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.client.execute("INSERT INTO durable_actors_replication_groups(prefix,state) VALUES($1,'closing') ON CONFLICT(prefix) DO UPDATE SET state=CASE WHEN durable_actors_replication_groups.state='archived' THEN 'archived' ELSE 'closing' END,updated_at=clock_timestamp()", &[&self.prefix]).await?;
        Ok(())
    }

    pub async fn archived(&self, checkpoint: Option<&Checkpoint>) -> Result<()> {
        let serialized = checkpoint.map(serde_json::to_string).transpose()?;
        ensure!(self.client.execute("UPDATE durable_actors_replication_groups SET state='archived',checkpoint=$2,updated_at=clock_timestamp() WHERE prefix=$1 AND state='closing'", &[&self.prefix, &serialized]).await? == 1, "replica group is not closing");
        Ok(())
    }
}

async fn claim_pods(
    tx: &deadpool_postgres::Transaction<'_>,
    prefix: &str,
    zones: &[String],
    eligible: &[String],
) -> Result<Vec<PodRecord>> {
    let select = tx.prepare_cached("SELECT config FROM durable_actors_replication_pods WHERE group_prefix IS NULL AND state='ready' AND zone=$1 AND name=ANY($3::text[]) AND NOT ((config::jsonb->>'node') = ANY($2::text[])) ORDER BY name LIMIT 1 FOR UPDATE SKIP LOCKED").await?;
    let mut used_nodes: Vec<String> = Vec::new();
    let mut pods = Vec::with_capacity(zones.len());
    for zone in zones {
        let pod: PodRecord = match tx
            .query_opt(&select, &[zone, &used_nodes, &eligible])
            .await?
        {
            Some(row) => serde_json::from_str(row.get(0))?,
            None => continue,
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

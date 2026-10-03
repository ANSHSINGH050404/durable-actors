use crate::{actor::ActorKey, postgres::PostgresDatabase};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::time::Instant;

pub(super) const GATEWAY_LEASE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GatewayOwner {
    pub id: String,
    pub route: String,
}

#[async_trait]
pub(super) trait SocketDirectory: Send + Sync {
    async fn owners(&self, project: &str) -> Result<Vec<GatewayOwner>>;
    async fn register(&self, owner: &GatewayOwner, accepts_rooms: bool) -> Result<()>;
    async fn renew(&self, owner: &GatewayOwner) -> Result<()>;
    async fn claim(&self, actor: &ActorKey, owner: &GatewayOwner) -> Result<GatewayOwner>;
    async fn lookup(&self, actor: &ActorKey) -> Result<Option<GatewayOwner>>;
}

pub(super) struct PostgresSocketDirectory {
    database: PostgresDatabase,
}

impl PostgresSocketDirectory {
    pub(super) fn new(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl SocketDirectory for PostgresSocketDirectory {
    async fn owners(&self, project: &str) -> Result<Vec<GatewayOwner>> {
        let prefix = format!("object.v4.{project}");
        Ok(self.database.connection().await?.query("SELECT DISTINCT g.id, g.route FROM socket_rooms r JOIN socket_gateways g ON g.id = r.gateway_id WHERE split_part(r.actor_key, ':', 1) = $1 AND g.expires_at > clock_timestamp()", &[&prefix]).await?.into_iter().map(|row| GatewayOwner { id: row.get(0), route: row.get(1) }).collect())
    }
    async fn register(&self, owner: &GatewayOwner, accepts_rooms: bool) -> Result<()> {
        self.database.execute("INSERT INTO socket_gateways (id, route, accepts_rooms, expires_at) VALUES ($1, $2, $3, clock_timestamp() + INTERVAL '30 seconds')", &[&owner.id, &owner.route, &accepts_rooms]).await?;
        Ok(())
    }

    async fn renew(&self, owner: &GatewayOwner) -> Result<()> {
        let updated = self.database.execute("UPDATE socket_gateways SET expires_at = clock_timestamp() + INTERVAL '30 seconds' WHERE id = $1 AND route = $2 AND expires_at > clock_timestamp()", &[&owner.id, &owner.route]).await?;
        ensure!(updated == 1, "gateway lease expired");
        Ok(())
    }

    async fn claim(&self, actor: &ActorKey, owner: &GatewayOwner) -> Result<GatewayOwner> {
        let key = actor.storage_key();
        self.database.execute("INSERT INTO socket_rooms (actor_key, gateway_id) SELECT $1, id FROM socket_gateways WHERE accepts_rooms AND expires_at > clock_timestamp() ORDER BY (id = $2) DESC, md5($1 || id) DESC LIMIT 1 ON CONFLICT (actor_key) DO UPDATE SET gateway_id = EXCLUDED.gateway_id WHERE NOT EXISTS (SELECT 1 FROM socket_gateways g WHERE g.id = socket_rooms.gateway_id AND g.expires_at > clock_timestamp())", &[&key.as_str(), &owner.id]).await?;
        self.lookup(actor)
            .await?
            .context("gateway ownership unavailable")
    }

    async fn lookup(&self, actor: &ActorKey) -> Result<Option<GatewayOwner>> {
        Ok(self.database.query_opt("SELECT g.id, g.route FROM socket_rooms r JOIN socket_gateways g ON g.id = r.gateway_id WHERE r.actor_key = $1 AND g.expires_at > clock_timestamp()", &[&actor.storage_key().as_str()]).await?.map(|row| GatewayOwner { id: row.get(0), route: row.get(1) }))
    }
}

#[derive(Default)]
pub(super) struct MemorySocketDirectory {
    state: Mutex<MemoryDirectoryState>,
}

#[derive(Default)]
struct MemoryDirectoryState {
    gateways: HashMap<String, (GatewayOwner, Instant, bool)>,
    rooms: HashMap<ActorKey, String>,
}

impl MemoryDirectoryState {
    fn gateway(&self, id: &str) -> Option<GatewayOwner> {
        self.gateways
            .get(id)
            .filter(|(_, deadline, _)| *deadline > Instant::now())
            .map(|(owner, _, _)| owner.clone())
    }
}

#[async_trait]
impl SocketDirectory for MemorySocketDirectory {
    async fn owners(&self, project: &str) -> Result<Vec<GatewayOwner>> {
        let state = self.state.lock().unwrap();
        let ids: std::collections::HashSet<_> = state
            .rooms
            .iter()
            .filter(|(actor, _)| actor.project_id == project)
            .map(|(_, id)| id)
            .collect();
        Ok(ids.into_iter().filter_map(|id| state.gateway(id)).collect())
    }
    async fn register(&self, owner: &GatewayOwner, accepts_rooms: bool) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        ensure!(
            !state.gateways.contains_key(&owner.id),
            "gateway already registered"
        );
        state.gateways.insert(
            owner.id.clone(),
            (owner.clone(), Instant::now() + GATEWAY_LEASE, accepts_rooms),
        );
        Ok(())
    }
    async fn renew(&self, owner: &GatewayOwner) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        ensure!(
            state.gateway(&owner.id).as_ref() == Some(owner),
            "gateway lease expired"
        );
        state.gateways.get_mut(&owner.id).unwrap().1 = Instant::now() + GATEWAY_LEASE;
        Ok(())
    }
    async fn claim(&self, actor: &ActorKey, owner: &GatewayOwner) -> Result<GatewayOwner> {
        let mut state = self.state.lock().unwrap();
        ensure!(
            state.gateway(&owner.id).as_ref() == Some(owner),
            "gateway lease expired"
        );
        if let Some(current) = state.rooms.get(actor).and_then(|id| state.gateway(id)) {
            return Ok(current);
        }
        let selected = state
            .gateways
            .values()
            .filter(|(_, deadline, eligible)| *eligible && *deadline > Instant::now())
            .max_by_key(|(candidate, _, _)| (candidate.id == owner.id, &candidate.id))
            .map(|(candidate, _, _)| candidate.clone())
            .context("no socket gateway is available")?;
        state.rooms.insert(actor.clone(), selected.id.clone());
        Ok(selected)
    }
    async fn lookup(&self, actor: &ActorKey) -> Result<Option<GatewayOwner>> {
        let state = self.state.lock().unwrap();
        Ok(state.rooms.get(actor).and_then(|id| state.gateway(id)))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/socket_directory.rs"]
mod tests;

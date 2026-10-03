use super::{admin::AdminService, socket_gateway::SocketGateway};
use crate::{
    host_leases::ActorSocketInventory,
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorInventorySnapshot, ActorResidency,
    },
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(super) struct GatewayInventoryReader {
    actors: Arc<dyn ActorInventoryReader>,
    gateway: Arc<SocketGateway>,
    authorization: String,
}

impl GatewayInventoryReader {
    pub(super) fn new(
        actors: Arc<dyn ActorInventoryReader>,
        gateway: Arc<SocketGateway>,
        api_key: Option<String>,
    ) -> Self {
        Self {
            actors,
            gateway,
            authorization: api_key
                .map(|key| format!("Bearer {key}"))
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl ActorInventoryReader for GatewayInventoryReader {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let (actors, sockets) = tokio::try_join!(
            self.actors.actor_inventory(project),
            self.gateway.inventory(project, &self.authorization)
        )?;
        Ok(ActorInventorySnapshot {
            actors: merge_rooms(actors.actors, sockets.rooms),
            connections_complete: actors.connections_complete && sockets.complete,
        })
    }
}

fn merge_rooms(
    actors: Vec<ActorInventory>,
    sockets: Vec<ActorSocketInventory>,
) -> Vec<ActorInventory> {
    let mut actors: std::collections::BTreeMap<_, _> = actors
        .into_iter()
        .map(|actor| (actor.actor_name.clone(), actor))
        .collect();
    let mut instances: std::collections::HashMap<_, _> = actors
        .values()
        .flat_map(|actor| {
            actor.instances.iter().enumerate().map(|(index, instance)| {
                ((actor.actor_name.clone(), instance.actor_id.clone()), index)
            })
        })
        .collect();
    for room in sockets {
        let key = (room.actor.actor_name.clone(), room.actor.actor_id.clone());
        let actor = actors
            .entry(room.actor.actor_name.clone())
            .or_insert_with(|| ActorInventory {
                actor_name: room.actor.actor_name,
                ..Default::default()
            });
        let index = *instances.entry(key).or_insert_with(|| {
            actor.dormant += 1;
            actor.instances.push(ActorInstanceOverview {
                actor_id: room.actor.actor_id,
                status: ActorResidency::Dormant,
                connections: vec![],
                waiting: None,
            });
            actor.instances.len() - 1
        });
        actor.instances[index].connections = room
            .connections
            .into_iter()
            .map(|connection| ActorConnectionInventory {
                id: connection.id,
                metadata: connection.metadata,
            })
            .collect();
    }
    actors.into_values().collect()
}

pub(super) struct SocketInventory {
    pub rooms: Vec<ActorSocketInventory>,
    pub complete: bool,
}

impl SocketGateway {
    pub(super) async fn inventory(
        &self,
        project: &str,
        authorization: &str,
    ) -> Result<SocketInventory> {
        self.ensure_authority()?;
        let owners = self.directory.owners(project).await?;
        let replies = futures_util::future::join_all(owners.into_iter().map(|owner| async move {
            if owner.id == self.owner.id {
                return Ok(self.registry.inventory(project).await);
            }
            self.http
                .post(format!(
                    "{}/internal/socket-inventory",
                    owner.route.trim_end_matches('/')
                ))
                .header(header::AUTHORIZATION, authorization)
                .timeout(std::time::Duration::from_secs(5))
                .json(&InventoryRequest {
                    project: project.to_owned(),
                    gateway_id: owner.id,
                })
                .send()
                .await?
                .error_for_status()?
                .json::<Vec<ActorSocketInventory>>()
                .await
                .map_err(anyhow::Error::from)
        }))
        .await;
        let mut inventory = SocketInventory {
            rooms: Vec::new(),
            complete: true,
        };
        for reply in replies {
            match reply {
                Ok(rooms) => inventory.rooms.extend(rooms),
                Err(error)
                    if error
                        .downcast_ref::<reqwest::Error>()
                        .and_then(reqwest::Error::status)
                        .is_some_and(|status| {
                            matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
                        }) =>
                {
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(%error, "socket inventory gateway unavailable");
                    inventory.complete = false;
                }
            }
        }
        self.ensure_authority()?;
        Ok(inventory)
    }
}

pub(super) fn router(gateway: Arc<SocketGateway>, admin: AdminService) -> Router {
    Router::new()
        .route("/internal/socket-inventory", axum::routing::post(inventory))
        .with_state((gateway, admin))
}

#[derive(Serialize, Deserialize)]
struct InventoryRequest {
    project: String,
    gateway_id: String,
}

async fn inventory(
    State((gateway, admin)): State<(Arc<SocketGateway>, AdminService)>,
    headers: HeaderMap,
    Json(request): Json<InventoryRequest>,
) -> Result<Json<Vec<ActorSocketInventory>>, (StatusCode, String)> {
    admin
        .authenticate(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|header| header.to_str().ok())
                .unwrap_or_default(),
        )
        .map_err(|error| (StatusCode::FORBIDDEN, format!("{error:#}")))?;
    super::admin::validate_component("project ID", &request.project, 64)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))?;
    let result = async {
        gateway.ensure_authority()?;
        ensure!(
            gateway.owner.id == request.gateway_id,
            "gateway ownership changed"
        );
        Ok(Json(gateway.registry.inventory(&request.project).await))
    }
    .await;
    result.map_err(|error: anyhow::Error| (StatusCode::SERVICE_UNAVAILABLE, format!("{error:#}")))
}

use crate::bucket::{ReplicaPlacement, ReplicaSet, SnapshotStore};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Group {
    pub prefix: String,
    pub replicas: Vec<ReplicaPlacement>,
    pub archived: bool,
    pub checkpoint: Option<super::fleet::Checkpoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum DirectoryCommand {
    Prepare { prefix: String },
    Lookup { prefix: String },
    Finish { prefix: String },
    Write { object: String, data: String },
    ReadArchive { object: String },
    ListArchive { prefix: String },
    Groups { prefix: String },
}
impl DirectoryCommand {
    pub fn scope(&self) -> &str {
        match self {
            Self::Prepare { prefix }
            | Self::Lookup { prefix }
            | Self::Finish { prefix }
            | Self::ListArchive { prefix }
            | Self::Groups { prefix } => prefix,
            Self::ReadArchive { object } | Self::Write { object, .. } => object,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct DirectoryReply {
    pub groups: Vec<Group>,
    pub data: Option<String>,
    pub keys: Vec<String>,
}

#[async_trait]
pub(crate) trait ReplicaDirectory: Send + Sync {
    async fn execute(&self, command: DirectoryCommand) -> Result<DirectoryReply>;
}

pub(crate) struct DedicatedSnapshots {
    directory: Arc<dyn ReplicaDirectory>,
    token: String,
    groups: Option<moka::future::Cache<String, (Vec<ReplicaPlacement>, Arc<ReplicaSet>)>>,
    writing: Option<tokio::sync::Mutex<()>>,
    http: reqwest::Client,
}
impl DedicatedSnapshots {
    pub fn new(directory: Arc<dyn ReplicaDirectory>, token: String) -> Result<Self> {
        Ok(Self {
            directory,
            token,
            writing: Some(tokio::sync::Mutex::new(())),
            http: super::client::ReplicaClient::http()?,
            groups: Some(
                moka::future::Cache::builder()
                    .time_to_idle(std::time::Duration::from_secs(300))
                    .build(),
            ),
        })
    }

    pub fn for_control_plane(directory: Arc<dyn ReplicaDirectory>, token: String) -> Result<Self> {
        Ok(Self {
            directory,
            token,
            writing: None,
            http: super::client::ReplicaClient::http()?,
            groups: None,
        })
    }

    async fn lock(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        match &self.writing {
            Some(writing) => Some(writing.lock().await),
            None => None,
        }
    }

    async fn cached(&self, prefix: &str) -> Option<Arc<ReplicaSet>> {
        match &self.groups {
            Some(groups) => groups.get(prefix).await.map(|(_, store)| store),
            None => None,
        }
    }

    async fn lookup(&self, prefix: &str) -> Result<Group> {
        self.directory
            .execute(DirectoryCommand::Lookup {
                prefix: prefix.into(),
            })
            .await?
            .groups
            .into_iter()
            .next()
            .context("replica group missing")
    }

    async fn live(&self, group: &Group) -> Result<Arc<ReplicaSet>> {
        ensure!(!group.archived, "replica group is archived");
        if let Some(groups) = &self.groups
            && let Some((replicas, store)) = groups.get(&group.prefix).await
            && replicas == group.replicas
        {
            return Ok(store);
        }
        let store = Arc::new(ReplicaSet::from_replicas(
            &group.replicas,
            self.token.clone(),
            self.http.clone(),
        )?);
        if let Some(groups) = &self.groups {
            groups
                .insert(
                    group.prefix.clone(),
                    (group.replicas.clone(), store.clone()),
                )
                .await;
        }
        Ok(store)
    }

    async fn archive_get(&self, object: &str) -> Result<Option<Bytes>> {
        Ok(self
            .directory
            .execute(DirectoryCommand::ReadArchive {
                object: object.into(),
            })
            .await?
            .data
            .map(|bytes| STANDARD.decode(bytes).map(Bytes::from))
            .transpose()?)
    }

    async fn archive_list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .directory
            .execute(DirectoryCommand::ListArchive {
                prefix: prefix.into(),
            })
            .await?
            .keys)
    }
}

#[async_trait]
impl SnapshotStore for DedicatedSnapshots {
    async fn prepare(&self, prefix: &str) -> Result<()> {
        let _writing = self.lock().await;
        let group = self
            .directory
            .execute(DirectoryCommand::Prepare {
                prefix: prefix.into(),
            })
            .await?
            .groups
            .into_iter()
            .next()
            .context("replica group missing")?;
        if !group.replicas.is_empty() {
            self.live(&group).await?;
        }
        Ok(())
    }

    async fn seal(&self, prefix: &str) -> Result<()> {
        let _writing = self.lock().await;
        self.directory
            .execute(DirectoryCommand::Finish {
                prefix: prefix.into(),
            })
            .await?;
        if let Some(groups) = &self.groups {
            groups.invalidate(prefix).await;
        }
        Ok(())
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let _writing = self.lock().await;
        let (prefix, _) = super::client::position(object)?;
        let store = match self.cached(&prefix).await {
            Some(store) => Some(store),
            None => {
                let group = self.lookup(&prefix).await?;
                if group.replicas.is_empty() {
                    None
                } else {
                    Some(self.live(&group).await?)
                }
            }
        };
        if let Some(store) = store {
            match store.put(object, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%error, %prefix, "replica write requires durable fallback")
                }
            }
        }
        if let Some(groups) = &self.groups {
            groups.invalidate(&prefix).await;
        }
        let group = self
            .directory
            .execute(DirectoryCommand::Write {
                object: object.into(),
                data: STANDARD.encode(&bytes),
            })
            .await?
            .groups
            .into_iter()
            .next()
            .context("fallback group missing")?;
        if !group.replicas.is_empty() {
            self.live(&group).await?;
        }
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        let (prefix, _) = super::client::position(object)?;
        let group = self.lookup(&prefix).await?;
        if group.archived || group.replicas.is_empty() {
            return self.archive_get(object).await;
        }
        self.live(&group).await?.get(object).await
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let group = self.lookup(prefix).await?;
        if !group.archived && !group.replicas.is_empty() {
            return self.live(&group).await?.latest(prefix).await;
        }
        match group.checkpoint {
            Some(checkpoint) => {
                let bytes = self
                    .archive_get(&checkpoint.object)
                    .await?
                    .context("final replica checkpoint is missing")?;
                ensure!(
                    super::record::checksum(&bytes) == checkpoint.digest,
                    "final replica checkpoint checksum mismatch"
                );
                Ok(Some((checkpoint.object, bytes)))
            }
            None => Ok(None),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: std::collections::BTreeSet<_> =
            self.archive_list(prefix).await?.into_iter().collect();
        for group in self
            .directory
            .execute(DirectoryCommand::Groups {
                prefix: prefix.into(),
            })
            .await?
            .groups
        {
            if !group.archived && !group.replicas.is_empty() {
                keys.extend(self.live(&group).await?.list(&group.prefix).await?);
            }
        }
        Ok(keys.into_iter().collect())
    }
}

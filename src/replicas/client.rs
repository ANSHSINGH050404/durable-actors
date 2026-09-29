use super::record::Record;
use crate::bucket::{ReplicaPlacement, SnapshotStore};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Command {
    Prepare { prefix: String },
    Seal { prefix: String },
    Latest { prefix: String },
    Get { object: String },
    List { prefix: String },
    Append { prefix: String, record: Record },
    Archived { key: String },
    Flush { prefix: String },
}
impl Command {
    pub fn scope(&self) -> Option<&str> {
        match self {
            Self::Prepare { prefix }
            | Self::Seal { prefix }
            | Self::Latest { prefix }
            | Self::List { prefix }
            | Self::Append { prefix, .. }
            | Self::Flush { prefix } => Some(prefix),
            Self::Get { object } => Some(object),
            Self::Archived { .. } => None,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(super) struct Request {
    pub replica: String,
    pub command: Command,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct Reply {
    pub object: Option<String>,
    pub data: Option<String>,
    pub keys: Vec<String>,
}

pub(crate) struct ReplicaClient {
    pub placement: ReplicaPlacement,
    token: String,
    http: reqwest::Client,
    latest: Mutex<BTreeMap<String, Bytes>>,
}
impl ReplicaClient {
    pub fn new(placement: ReplicaPlacement, token: String) -> Result<Self> {
        Ok(Self::with_http(placement, token, Self::http()?))
    }
    pub(crate) fn http() -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?)
    }
    pub(crate) fn with_http(
        placement: ReplicaPlacement,
        token: String,
        http: reqwest::Client,
    ) -> Self {
        Self {
            placement,
            token,
            http,
            latest: Mutex::new(BTreeMap::new()),
        }
    }
    pub(super) async fn send(&self, command: Command) -> Result<Reply> {
        let flushing = matches!(command, Command::Flush { .. });
        let request = self
            .http
            .post(format!(
                "{}/storage",
                self.placement.address.trim_end_matches('/')
            ))
            .bearer_auth(&self.token)
            .json(&Request {
                replica: self.placement.id.clone(),
                command,
            });
        let request = if flushing {
            request
        } else {
            request.timeout(Duration::from_secs(10))
        };
        let result: std::result::Result<Reply, String> =
            request.send().await?.error_for_status()?.json().await?;
        result.map_err(anyhow::Error::msg)
    }
    pub async fn archived(&self, key: &str) -> Result<()> {
        self.send(Command::Archived { key: key.into() }).await?;
        Ok(())
    }
}
#[async_trait]
impl SnapshotStore for ReplicaClient {
    async fn prepare(&self, prefix: &str) -> Result<()> {
        self.send(Command::Prepare {
            prefix: prefix.into(),
        })
        .await?;
        Ok(())
    }
    async fn seal(&self, prefix: &str) -> Result<()> {
        self.send(Command::Seal {
            prefix: prefix.into(),
        })
        .await?;
        Ok(())
    }
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let (prefix, version) = position(object)?;
        let mut cache = self.latest.lock().await;
        let record = Record::encode(version, &bytes, cache.get(&prefix).map(|b| b.as_ref()))?;
        self.send(Command::Append {
            prefix: prefix.clone(),
            record,
        })
        .await?;
        cache.insert(prefix, bytes);
        Ok(())
    }
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        self.send(Command::Get {
            object: object.into(),
        })
        .await?
        .data
        .map(|s| STANDARD.decode(s).map(Bytes::from).map_err(Into::into))
        .transpose()
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let reply = self
            .send(Command::Latest {
                prefix: prefix.into(),
            })
            .await?;
        reply
            .object
            .zip(reply.data)
            .map(|(object, data)| Ok((object, Bytes::from(STANDARD.decode(data)?))))
            .transpose()
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .send(Command::List {
                prefix: prefix.into(),
            })
            .await?
            .keys)
    }
}

pub(super) fn position(object: &str) -> Result<(String, u64)> {
    let (prefix, file) = object.rsplit_once('/').context("snapshot epoch missing")?;
    let version = file
        .strip_suffix(".json")
        .context("invalid snapshot object")?
        .parse()?;
    ensure!(version > 0, "invalid state version");
    Ok((format!("{prefix}/"), version))
}

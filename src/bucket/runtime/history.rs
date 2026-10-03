use super::*;
use crate::state_log::{StateAttribution, StateSnapshot};
use futures_util::{StreamExt, TryStreamExt, stream};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StateRecord {
    pub state_version: u64,
    pub owner_epoch: u64,
    pub request_id: String,
    pub attribution: Option<StateAttribution>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InspectedState {
    #[serde(flatten)]
    pub record: StateRecord,
    pub state: Box<serde_json::value::RawValue>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StateHistoryPage {
    pub records: Vec<StateRecord>,
    pub next_before: Option<u64>,
}

#[async_trait]
pub(crate) trait ActorStateReader: Send + Sync {
    async fn inspect_state(
        &self,
        actor: &ActorKey,
        version: Option<u64>,
    ) -> Result<Option<InspectedState>>;
    async fn state_history(
        &self,
        actor: &ActorKey,
        before: Option<u64>,
        limit: usize,
    ) -> Result<StateHistoryPage>;
}

#[async_trait]
impl ActorStateReader for RuntimeStorageReader {
    async fn inspect_state(
        &self,
        actor: &ActorKey,
        version: Option<u64>,
    ) -> Result<Option<InspectedState>> {
        let key =
            self.stored_versions(actor)
                .await?
                .into_iter()
                .find(|((_, stored_version), _)| {
                    version.is_none_or(|version| version == *stored_version)
                });
        let Some((_, key)) = key else {
            return Ok(None);
        };
        self.read_stored_state(&key).await
    }

    async fn state_history(
        &self,
        actor: &ActorKey,
        before: Option<u64>,
        limit: usize,
    ) -> Result<StateHistoryPage> {
        let mut versions = self.stored_versions(actor).await?;
        versions.retain(|((_, version), _)| before.is_none_or(|before| *version < before));
        let more = versions.len() > limit;
        versions.truncate(limit);
        let records = stream::iter(
            versions
                .into_iter()
                .map(|(_, key)| async move { self.history_record(&key).await }),
        )
        .buffered(8)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        Ok(StateHistoryPage {
            next_before: if more {
                records.last().map(|record| record.state_version)
            } else {
                None
            },
            records,
        })
    }
}

impl RuntimeStorageReader {
    async fn stored_versions(&self, actor: &ActorKey) -> Result<Vec<((u64, u64), String)>> {
        let prefix = crate::storage_paths::snapshots(actor)?;
        let mut versions: Vec<_> = self
            .snapshots
            .list(&prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                snapshot_position(&key)
                    .filter(|(_, version)| {
                        crate::storage::validate_snapshot_object_name(actor, *version, &key).is_ok()
                    })
                    .map(|position| (position, key))
            })
            .collect();
        versions.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        let mut seen = std::collections::HashSet::new();
        versions.retain(|((_, version), _)| seen.insert(*version));
        Ok(versions)
    }

    async fn read_stored_state(&self, key: &str) -> Result<Option<InspectedState>> {
        let Some(bytes) = self.read_persisted(key).await? else {
            return Ok(None);
        };
        decode_snapshot(key.into(), bytes.clone())?;
        let snapshot = StateSnapshot::decode(&bytes)?;
        let sqlite = self.read_sqlite(key, bytes).await?;
        let fields = crate::litestream::storage::restored_fields(
            self.restore.as_ref(),
            &sqlite.files,
            sqlite.txid,
        )
        .await?;
        Ok(Some(InspectedState {
            record: record(snapshot),
            state: serde_json::value::to_raw_value(&fields)?,
        }))
    }

    async fn read_commit(&self, key: &str) -> Result<Option<StateSnapshot>> {
        self.read_persisted(key)
            .await?
            .map(|bytes| {
                decode_snapshot(key.to_owned(), bytes)
                    .and_then(|loaded| StateSnapshot::decode(&loaded.bytes))
            })
            .transpose()
    }

    async fn history_record(&self, snapshot_key: &str) -> Result<Option<StateRecord>> {
        let key = format!("{snapshot_key}.metadata");
        if let Some(cached) = self.authority.get(&key).await? {
            return Ok(Some(serde_json::from_slice(&cached.bytes)?));
        }
        let Some(snapshot) = self.read_commit(snapshot_key).await? else {
            return Ok(None);
        };
        let state = record(snapshot);
        // This index is rebuildable; snapshot storage remains the source of truth.
        if let Err(error) = self.cache_history_record(&key, &state).await {
            tracing::warn!(%error, "state history metadata cache unavailable");
        }
        Ok(Some(state))
    }

    async fn cache_history_record(&self, key: &str, record: &StateRecord) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(2),
            replace(
                self.authority.as_ref(),
                key,
                None,
                crate::payload::encode(record)?,
            ),
        )
        .await??;
        Ok(())
    }
}

#[async_trait]
impl ActorStateReader for RuntimeStorage {
    async fn inspect_state(
        &self,
        actor: &ActorKey,
        version: Option<u64>,
    ) -> Result<Option<InspectedState>> {
        self.reader.inspect_state(actor, version).await
    }
    async fn state_history(
        &self,
        actor: &ActorKey,
        before: Option<u64>,
        limit: usize,
    ) -> Result<StateHistoryPage> {
        self.reader.state_history(actor, before, limit).await
    }
}

fn record(snapshot: StateSnapshot) -> StateRecord {
    StateRecord {
        state_version: snapshot.state_version,
        owner_epoch: snapshot.owner_epoch,
        request_id: snapshot.request_id,
        attribution: snapshot.attribution,
    }
}

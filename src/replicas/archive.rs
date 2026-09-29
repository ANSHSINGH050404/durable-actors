use super::record::{Batch, archive_prefix};
use crate::bucket::{Bucket, replace};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub(super) struct Archive(pub Arc<dyn Bucket>);

impl Archive {
    pub async fn write(&self, batch: &Batch) -> Result<String> {
        batch.decode()?;
        let key = batch.key()?;
        ensure!(
            replace(self.0.as_ref(), &key, None, serde_json::to_vec(batch)?).await?,
            "archive conflict"
        );
        Ok(key)
    }

    pub async fn read(&self, key: &str) -> Result<Batch> {
        let object = self.0.get(key).await?.context("archive batch missing")?;
        let batch: Batch = serde_json::from_slice(&object.bytes)?;
        ensure!(batch.key()? == key, "archive checksum mismatch");
        batch.decode()?;
        Ok(batch)
    }

    pub async fn get(&self, prefix: &str, version: u64) -> Result<Option<Vec<u8>>> {
        for key in self.0.list(&archive_prefix(prefix)?).await? {
            let Some((first, last)) = range(&key) else {
                continue;
            };
            if first <= version && version <= last {
                if let Some((_, bytes)) = self
                    .read(&key)
                    .await?
                    .decode()?
                    .into_iter()
                    .find(|(v, _)| *v == version)
                {
                    return Ok(Some(bytes));
                }
            }
        }
        Ok(None)
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut versions = BTreeMap::new();
        for key in self.0.list(&archive_prefix(prefix)?).await? {
            if range(&key).is_none() {
                continue;
            }
            let batch = self.read(&key).await?;
            for record in batch.records {
                let object = format!("{}{}.json", batch.prefix, record.version);
                if let Some(previous) = versions.insert(object, record.digest.clone()) {
                    ensure!(previous == record.digest, "conflicting archives");
                }
            }
        }
        Ok(versions.into_keys().collect())
    }
}

fn range(key: &str) -> Option<(u64, u64)> {
    let mut parts = key.rsplit('/').next()?.splitn(3, '-');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

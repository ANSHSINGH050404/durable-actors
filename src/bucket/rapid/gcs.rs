use super::*;
use crate::bucket::{Bucket, gcs::GcsClients};

pub(super) struct GcsSnapshots {
    bucket: String,
    read: GcsBucket,
    clients: GcsClients,
    class: StorageClass,
}

pub(super) enum StorageClass {
    Rapid,
    Standard,
}

impl GcsSnapshots {
    pub fn new(bucket: &str, clients: GcsClients, class: StorageClass) -> Result<Self> {
        Ok(Self {
            bucket: format!("projects/_/buckets/{bucket}"),
            read: GcsBucket::with_clients(bucket, clients.clone())?,
            clients,
            class,
        })
    }
}

#[async_trait]
impl SnapshotStore for GcsSnapshots {
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let object = super::object_name(object)?;
        if matches!(self.class, StorageClass::Standard) {
            ensure!(
                crate::bucket::replace(&self.read, &object, None, bytes.to_vec()).await?,
                "conflicting immutable archive"
            );
            return Ok(());
        }
        let staging = format!(
            "{}-{}",
            object.replacen("snapshots-", "uploads-", 1),
            uuid::Uuid::new_v4()
        );
        let mut writer = self
            .clients
            .storage
            .open_appendable_object(&self.bucket, &staging)
            .set_if_generation_match(0)
            .send()
            .await?;
        writer.append(bytes.clone()).await?;
        let uploaded = writer.finalize().await?;
        let published = self
            .clients
            .control
            .move_object()
            .set_bucket(&self.bucket)
            .set_source_object(&staging)
            .set_destination_object(&object)
            .set_if_source_generation_match(uploaded.generation)
            .set_if_generation_match(0)
            .send()
            .await;
        match published {
            Ok(_) => Ok(()),
            Err(error) => {
                if self
                    .read_snapshot(&object)
                    .await?
                    .is_some_and(|copy| copy == bytes)
                {
                    return Ok(());
                }
                Err(error.into())
            }
        }
    }

    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        self.read_snapshot(&super::object_name(object)?).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let physical = super::object_name(prefix)?;
        self.read
            .list(&physical)
            .await?
            .into_iter()
            .map(|key| {
                let suffix = key
                    .strip_prefix(&physical)
                    .context("snapshot list escaped its prefix")?;
                Ok(format!("{prefix}{}", suffix.replace('~', "/")))
            })
            .collect()
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let key = self
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|key| super::super::snapshots::version(&key).map(|v| (v, key)))
            .max_by_key(|(v, _)| *v);
        match key {
            Some((_, key)) => Ok(self.get(&key).await?.map(|bytes| (key, bytes))),
            None => Ok(None),
        }
    }
}

impl GcsSnapshots {
    async fn read_snapshot(&self, object: &str) -> Result<Option<Bytes>> {
        if matches!(self.class, StorageClass::Standard) {
            return Ok(self.read.get(object).await?.map(|copy| copy.bytes.into()));
        }
        let (_descriptor, mut reader) = match self
            .clients
            .storage
            .open_object(&self.bucket, object)
            .send_and_read(google_cloud_storage::model_ext::ReadRange::all())
            .await
        {
            Ok(opened) => opened,
            Err(error)
                if error
                    .status()
                    .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Some(bytes.into()))
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid_gcs.rs"]
mod tests;

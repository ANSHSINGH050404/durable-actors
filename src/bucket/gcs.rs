use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use google_cloud_storage::client::{Storage, StorageControl};

use super::{Bucket, BucketObject};

pub struct GcsBucket {
    bucket: String,
    clients: GcsClients,
}

pub(crate) struct WarmGcs {
    clients: GcsClients,
    credentials: PendingCredentials,
}

#[derive(Clone)]
pub(crate) struct GcsClients {
    pub storage: Storage,
    pub control: StorageControl,
}

impl GcsBucket {
    pub(crate) fn with_clients(bucket: &str, clients: GcsClients) -> Result<Self> {
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients,
        })
    }

    pub(crate) fn clients(&self) -> GcsClients {
        self.clients.clone()
    }

    pub(crate) async fn require_standard(&self) -> Result<()> {
        let bucket = self
            .clients
            .control
            .get_bucket()
            .set_name(&self.bucket)
            .send()
            .await?;
        anyhow::ensure!(
            bucket.storage_class == "STANDARD",
            "archive bucket must use Standard GCS"
        );
        Ok(())
    }

    pub async fn new(bucket: &str) -> Result<Self> {
        Self::with_credentials(
            bucket,
            google_cloud_auth::credentials::Builder::default().build()?,
        )
        .await
    }

    pub async fn with_credentials(bucket: &str, credentials: Credentials) -> Result<Self> {
        Ok(Self {
            bucket: bucket_name(bucket)?,
            clients: GcsClients::new(credentials).await?,
        })
    }
}

impl WarmGcs {
    pub async fn new() -> Result<Self> {
        let credentials = PendingCredentials::default();
        Ok(Self {
            clients: GcsClients::new(credentials.clone().into()).await?,
            credentials,
        })
    }

    pub async fn preconnect(&self) {
        // An anonymous read warms the HTTP pool without granting an idle spare access.
        let probe = self
            .clients
            .storage
            .read_object("projects/_/buckets/durable-actors-warmup", "connection")
            .send();
        let _ = tokio::time::timeout(Duration::from_millis(250), probe).await;
    }

    pub async fn keep_warm(&self) {
        let period = Duration::from_secs(20);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.preconnect().await;
        }
    }

    pub fn bind(self, bucket: &str, credentials: Credentials) -> Result<GcsBucket> {
        let bucket = bucket_name(bucket)?;
        anyhow::ensure!(
            self.credentials.0.set(credentials).is_ok(),
            "storage already assigned"
        );
        Ok(GcsBucket {
            bucket,
            clients: self.clients,
        })
    }
}

impl GcsClients {
    async fn new(credentials: Credentials) -> Result<Self> {
        Ok(Self {
            storage: Storage::builder()
                .with_credentials(credentials.clone())
                .build()
                .await?,
            control: StorageControl::builder()
                .with_credentials(credentials)
                .build()
                .await?,
        })
    }
}

#[async_trait]
impl Bucket for GcsBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        let mut response = match self
            .clients
            .storage
            .read_object(&self.bucket, key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error.http_status_code() == Some(404)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let generation = response.object().generation;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Some(BucketObject { generation, bytes }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        match self
            .clients
            .storage
            .write_object(&self.bucket, key, bytes::Bytes::from(bytes))
            .set_if_generation_match(generation.unwrap_or(0))
            .send_unbuffered()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.http_status_code() == Some(412)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "FAILED_PRECONDITION") =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = String::new();
        loop {
            let response = self
                .clients
                .control
                .list_objects()
                .set_parent(&self.bucket)
                .set_prefix(prefix)
                .set_page_token(&token)
                .send()
                .await?;
            keys.extend(response.objects.into_iter().map(|object| object.name));
            token = response.next_page_token;
            if token.is_empty() {
                return Ok(keys);
            }
        }
    }
}

fn bucket_name(bucket: &str) -> Result<String> {
    anyhow::ensure!(
        !bucket.is_empty() && !bucket.contains('/'),
        "invalid storage bucket"
    );
    Ok(format!("projects/_/buckets/{bucket}"))
}

#[derive(Clone, Default)]
struct PendingCredentials(Arc<OnceLock<Credentials>>);

impl std::fmt::Debug for PendingCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PendingCredentials")
    }
}

impl CredentialsProvider for PendingCredentials {
    async fn headers(
        &self,
        extensions: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        match self.0.get() {
            Some(credentials) => credentials.headers(extensions).await,
            None => Ok(CacheableResource::New {
                entity_tag: EntityTag::new(),
                data: axum::http::HeaderMap::new(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/gcs.rs"]
mod tests;

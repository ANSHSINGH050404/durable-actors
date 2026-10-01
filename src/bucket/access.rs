use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use gcp_auth::TokenProvider;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::clock::{Clock, SystemClock};
use crate::postgres::PostgresDatabase;
mod cache;
use cache::SharedTokens;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub persistence: super::PersistenceConfig,
    pub region: String,
    pub token: Option<StorageToken>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BucketLocation {
    Gcs {
        bucket: String,
        artifact_bucket: String,
    },
    File {
        directory: PathBuf,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageToken {
    pub access_token: String,
    pub expires_at_ms: u64,
}

impl std::fmt::Debug for StorageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageToken")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

pub(crate) struct RuntimeAccess {
    location: BucketLocation,
    tokens: Option<StorageTokens>,
    persistence: super::PersistenceConfig,
}

impl RuntimeAccess {
    pub fn new(location: BucketLocation, persistence: super::PersistenceConfig) -> Result<Self> {
        Ok(Self {
            tokens: match &location {
                BucketLocation::Gcs { .. } => Some(StorageTokens {
                    source: Arc::new(GcsTokenSource {
                        credentials: StorageCredentials::new()?,
                        http: reqwest::Client::builder()
                            .timeout(Duration::from_secs(20))
                            .build()?,
                    }),
                    shared: None,
                }),
                BucketLocation::File { .. } => None,
            },
            location,
            persistence,
        })
    }

    pub fn with_token_cache(
        mut self,
        database: PostgresDatabase,
        issuer: String,
        stop: tokio_util::sync::CancellationToken,
    ) -> Self {
        if let Some(tokens) = &mut self.tokens {
            let shared = SharedTokens::new(database, issuer, tokens.source.clone());
            shared.start(stop);
            tokens.shared = Some(shared);
        }
        self
    }

    pub(crate) fn validate_code(&self, code: &crate::artifacts::ArtifactManifest) -> Result<()> {
        let BucketLocation::Gcs {
            artifact_bucket, ..
        } = &self.location
        else {
            anyhow::bail!("compiled GCS bundles require GCS storage");
        };
        ensure!(
            code.bucket == *artifact_bucket,
            "code artifact belongs to another bucket"
        );
        artifact_prefix(code)?;
        Ok(())
    }

    pub async fn bootstrap(
        &self,
        region: &str,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            persistence: self.persistence.clone(),
            region: region.into(),
            token: self.issue(actor, code_snapshot).await?,
        })?)
    }

    pub async fn issue(
        &self,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<Option<StorageToken>> {
        let BucketLocation::Gcs {
            bucket,
            artifact_bucket,
        } = &self.location
        else {
            return Ok(None);
        };
        let code = code_snapshot
            .map(crate::artifacts::ArtifactManifest::decode)
            .transpose()?;
        let boundary = boundary(
            bucket,
            &self.persistence,
            Some(artifact_bucket),
            actor,
            code.as_ref(),
        )?;
        let tokens = self
            .tokens
            .as_ref()
            .context("storage token issuer missing")?;
        Ok(Some(match &tokens.shared {
            Some(shared) => shared.issue(&boundary).await?,
            None => tokens.source.exchange(&boundary).await?,
        }))
    }
}

struct StorageTokens {
    source: Arc<dyn StorageTokenSource>,
    shared: Option<SharedTokens>,
}

#[async_trait]
trait StorageTokenSource: Send + Sync {
    async fn exchange(&self, boundary: &Value) -> Result<StorageToken>;
}

struct GcsTokenSource {
    credentials: StorageCredentials,
    http: reqwest::Client,
}

#[async_trait]
impl StorageTokenSource for GcsTokenSource {
    async fn exchange(&self, boundary: &Value) -> Result<StorageToken> {
        let source = self.credentials.token().await?;
        let mut form = reqwest::Url::parse("https://sts.googleapis.com/")?;
        form.query_pairs_mut().extend_pairs([
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", &source),
            ("options", &boundary.to_string()),
        ]);
        let started = SystemClock.now_ms()?;
        let response = self
            .http
            .post("https://sts.googleapis.com/v1/token")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form.query().unwrap().to_owned())
            .send()
            .await?
            .error_for_status()?;
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
            expires_in: u64,
        }
        let token: Token = response
            .json()
            .await
            .context("exchange scoped GCS credentials (service-account credentials required)")?;
        ensure!(
            token.expires_in > 60,
            "GCS credential lifetime is too short"
        );
        Ok(StorageToken {
            access_token: token.access_token,
            expires_at_ms: started + (token.expires_in - 30) * 1000,
        })
    }
}

enum StorageCredentials {
    ServiceAccount(gcp_auth::CustomServiceAccount),
    ApplicationDefault(AccessTokenCredentials),
}

impl StorageCredentials {
    fn new() -> Result<Self> {
        if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
            let document = std::fs::read_to_string(path)?;
            let value: Value = serde_json::from_str(&document)?;
            if value["type"] == "service_account" {
                return Ok(Self::ServiceAccount(
                    gcp_auth::CustomServiceAccount::from_json(&document)?,
                ));
            }
        }
        Ok(Self::ApplicationDefault(
            Builder::default().build_access_token_credentials()?,
        ))
    }

    async fn token(&self) -> Result<String> {
        match self {
            // STS requires an OAuth access token, not the default library's self-signed JWT.
            Self::ServiceAccount(credentials) => Ok(credentials
                .token(&["https://www.googleapis.com/auth/cloud-platform"])
                .await?
                .as_str()
                .to_owned()),
            Self::ApplicationDefault(credentials) => Ok(credentials.access_token().await?.token),
        }
    }
}

fn boundary(
    bucket: &str,
    persistence: &super::PersistenceConfig,
    artifact_bucket: Option<&str>,
    actor: &crate::actor::ActorKey,
    code: Option<&crate::artifacts::ArtifactManifest>,
) -> Result<Value> {
    let owner = format!(
        "projects/_/buckets/{bucket}/objects/{}",
        crate::storage_paths::owner(&actor.storage_key())?
    );
    let mut rules = vec![rule(
        bucket,
        &["storage.objectUser"],
        format!("resource.name == {}", serde_json::to_string(&owner)?),
    )];
    let prefix = crate::storage_paths::snapshots(actor)?;
    match persistence {
        super::PersistenceConfig::Local => rules.push(snapshot_rule(bucket, &[&prefix], false)?),
        super::PersistenceConfig::Rapid {
            archive_bucket,
            buckets,
            ..
        } => {
            let prefix = super::rapid::object_name(&prefix)?;
            rules.push(snapshot_rule(archive_bucket, &[&prefix], false)?);
            let logs = prefix.replacen("snapshots-", "logs-", 1);
            for placement in buckets {
                rules.push(snapshot_rule(&placement.bucket, &[&logs], true)?);
            }
        }
    }

    if let Some(code) = code {
        ensure!(
            Some(code.bucket.as_str()) == artifact_bucket,
            "code artifact belongs to another bucket"
        );
        let prefix = format!(
            "projects/_/buckets/{}/objects/{}",
            code.bucket,
            artifact_prefix(code)?,
        );
        rules.push(rule(
            &code.bucket,
            &["storage.objectViewer"],
            format!(
                "resource.name.startsWith({})",
                serde_json::to_string(&prefix)?
            ),
        ));
    }
    Ok(json!({"accessBoundary": {"accessBoundaryRules": rules}}))
}

fn snapshot_rule(bucket: &str, prefixes: &[&str], mutable: bool) -> Result<Value> {
    let expressions = prefixes.iter().map(|prefix| {
        let resource = format!("projects/_/buckets/{bucket}/objects/{prefix}");
        Ok(format!(
            "resource.name.startsWith({}) || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith({})",
            serde_json::to_string(&resource)?, serde_json::to_string(prefix)?
        ))
    }).collect::<Result<Vec<_>>>()?;
    let roles: &[&str] = if mutable {
        &["storage.objectUser"]
    } else {
        &["storage.objectViewer", "storage.objectCreator"]
    };
    Ok(rule(bucket, roles, expressions.join(" || ")))
}

fn artifact_prefix(code: &crate::artifacts::ArtifactManifest) -> Result<String> {
    let root = format!("{}artifacts/", crate::storage_paths::ROOT);
    let first = code.files.first().context("code artifact is empty")?;
    let relative = first
        .object
        .strip_prefix(&root)
        .context("invalid artifact location")?;
    let (deployment, _) = relative
        .split_once('/')
        .context("artifact deployment missing")?;
    uuid::Uuid::parse_str(deployment).context("invalid artifact deployment")?;
    let prefix = format!("{root}{deployment}/");
    ensure!(
        code.files
            .iter()
            .all(|file| file.object == format!("{prefix}{}", file.path)),
        "code files must belong to one artifact deployment"
    );
    Ok(prefix)
}

fn rule(bucket: &str, roles: &[&str], expression: String) -> Value {
    json!({"availableResource": format!("//storage.googleapis.com/projects/_/buckets/{bucket}"),
        "availablePermissions": roles.iter().map(|role| format!("inRole:roles/{role}")).collect::<Vec<_>>(),
        "availabilityCondition": {"expression": expression}})
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/access.rs"]
mod tests;

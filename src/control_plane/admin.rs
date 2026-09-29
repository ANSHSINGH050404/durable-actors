use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::postgres::PostgresDatabase;

use super::ActorJwtIssuer;
use super::contracts::{PublicActorContract, PublishedContract, SandboxOptions};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostLaunchSpec {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sandboxes: BTreeMap<String, SandboxOptions>,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<DeploymentSource>,
    pub image_ref: String,
    #[serde(default)]
    pub code_snapshot: Option<String>,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeploymentSource {
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
}

impl From<&HostLaunchSpec> for DeploymentSource {
    fn from(spec: &HostLaunchSpec) -> Self {
        Self {
            image_ref: spec.image_ref.clone(),
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec.actor_entrypoint.clone(),
        }
    }
}

impl HostLaunchSpec {
    pub(crate) fn host_config_key(&self) -> String {
        let mut identity = serde_json::to_vec(&(
            &self.project_id,
            &self.secret_refs,
            &self.image_ref,
            &self.code_snapshot,
            &self.working_directory,
            &self.actor_entrypoint,
        ))
        .expect("host identity is serializable");
        if !self.sandboxes.is_empty() {
            identity.extend(
                serde_json::to_vec(&self.sandboxes).expect("sandbox options are serializable"),
            );
        }
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &identity);
        format!("cfg.{}", URL_SAFE_NO_PAD.encode(digest.as_ref()))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (actor, options) in &self.sandboxes {
            validate_component("actor name", actor, 255)?;
            options.validate()?;
        }
        validate_component("project ID", &self.project_id, 64)?;
        if let Some(snapshot) = &self.code_snapshot {
            let manifest = crate::artifacts::ArtifactManifest::decode(snapshot)?;
            ensure!(
                self.working_directory == "/customer",
                "snapshot deployments must use /customer"
            );
            let entrypoint = self.actor_entrypoint.as_deref().unwrap_or("actors.mjs");
            ensure!(
                entrypoint == manifest.entrypoint()?,
                "compiled entrypoint must be actors.mjs or actors.pyz"
            );
        }
        ensure!(
            !self.image_ref.is_empty() && self.image_ref.len() <= 255,
            "sandbox image reference must contain between 1 and 255 bytes"
        );
        ensure!(
            self.working_directory.starts_with('/') && self.working_directory.len() <= 1024,
            "host working directory must be an absolute path of at most 1024 bytes"
        );
        if let Some(entrypoint) = &self.actor_entrypoint {
            ensure!(
                !entrypoint.is_empty() && entrypoint.len() <= 1024,
                "actor entrypoint must contain between 1 and 1024 bytes"
            );
        }
        ensure!(
            self.secret_refs.len() <= 16,
            "at most 16 secret references are allowed"
        );
        for reference in &self.secret_refs {
            validate_component("secret reference", reference, 255)?;
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait AdminRegistry: Send + Sync {
    #[cfg(test)]
    async fn register_test_deployment(&self, spec: &HostLaunchSpec) -> Result<bool> {
        self.register_deployment(spec, None).await
    }
    #[cfg(test)]
    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        self.lock_deployment(&spec.project_id)
            .await?
            .register(spec, contract)
            .await
    }
    async fn lock_deployment(&self, project_id: &str) -> Result<Box<dyn DeploymentUpdate + '_>>;
    async fn deployment_contract(&self, project_id: &str) -> Result<Option<PublishedContract>>;
    async fn launch_spec(&self, project_id: &str) -> Result<Option<HostLaunchSpec>>;
    async fn launch_specs(&self) -> Result<Vec<HostLaunchSpec>>;
    #[cfg(test)]
    async fn remove_deployment(&self, project_id: &str) -> Result<()> {
        self.lock_deployment(project_id).await?.remove().await
    }
}

#[async_trait]
pub(crate) trait DeploymentUpdate: Send {
    async fn register(
        &mut self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool>;
    async fn remove(&mut self) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct AdminService {
    api_key: Option<String>,
    registry: std::sync::Arc<dyn AdminRegistry>,
    issuer: ActorJwtIssuer,
    sessions: super::session::SessionVerifier,
}

impl AdminService {
    pub(crate) fn new(
        api_key: Option<String>,
        registry: std::sync::Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
    ) -> Result<Self> {
        if let Some(api_key) = &api_key {
            ensure!(
                !api_key.is_empty() && api_key.trim() == api_key,
                "API key is invalid"
            );
        }
        let sessions = issuer.session_verifier()?;
        Ok(Self {
            api_key,
            registry,
            issuer,
            sessions,
        })
    }

    pub(super) fn authorize_session_issuance(&self, authorization: &str) -> Result<()> {
        ensure!(
            self.api_key.is_some(),
            "session issuance requires administrative authentication"
        );
        self.authenticate(authorization)
    }

    pub(super) fn issue_session(
        &self,
        project_id: String,
        subject: String,
        expires_at_ms: i64,
    ) -> Result<super::issuer::IssuedActorToken> {
        self.issuer
            .issue_session(project_id, subject, expires_at_ms)
    }

    pub(super) fn authorize_discovery(
        &self,
        authorization: &str,
        project_id: &str,
    ) -> Result<Option<super::session::ActorSession>> {
        if self.authenticate(authorization).is_ok() {
            return Ok(None);
        }
        Ok(Some(self.sessions.authenticate(authorization, project_id)?))
    }

    pub(super) fn issue_direct_socket(
        &self,
        grant: super::socket_ticket::SocketGrant,
        credentials: crate::sandbox::SocketCredentials,
    ) -> Result<serde_json::Value> {
        let mut url = reqwest::Url::parse(&credentials.url)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none(),
            "invalid socket endpoint"
        );
        url.set_path("/v1/socket");
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("invalid socket endpoint"))?;
        let home_region = grant.region.clone();
        let (key, connect_by_ms, authorized_until_ms) = self.issuer.issue_socket(grant)?;
        url.query_pairs_mut().append_pair("key", &key);
        Ok(
            serde_json::json!({ "homeRegion": home_region, "websocketUrl": url.as_str(), "connectByMs": connect_by_ms, "authorizedUntilMs": authorized_until_ms }),
        )
    }

    pub(crate) fn authenticate(&self, authorization: &str) -> Result<()> {
        let Some(api_key) = &self.api_key else {
            return Ok(());
        };
        let token = authorization
            .strip_prefix("Bearer ")
            .context("admin credential must use Bearer authentication")?;
        ensure!(
            !token.is_empty()
                && token.trim() == token
                && bool::from(token.as_bytes().ct_eq(api_key.as_bytes())),
            "admin credential is invalid"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn register_test_deployment(&self, spec: &HostLaunchSpec) -> Result<bool> {
        self.registry.register_test_deployment(spec).await
    }

    pub(crate) async fn current_deployment(
        &self,
        project_id: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec(project_id).await
    }

    #[cfg(test)]
    pub(crate) async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        self.registry.register_deployment(spec, contract).await
    }

    pub(crate) async fn deployment_contract(
        &self,
        project_id: &str,
    ) -> Result<Option<PublishedContract>> {
        self.registry.deployment_contract(project_id).await
    }

    pub(crate) async fn lock_deployment(
        &self,
        project_id: &str,
    ) -> Result<Box<dyn DeploymentUpdate + '_>> {
        self.registry.lock_deployment(project_id).await
    }
}

#[derive(Default)]
pub(crate) struct LocalAdminRegistry {
    state: Mutex<HashMap<String, LocalAdminState>>,
    updates: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct LocalAdminState {
    deployment: Option<HostLaunchSpec>,
    contract: Option<PublishedContract>,
}

#[async_trait]
impl AdminRegistry for LocalAdminRegistry {
    async fn lock_deployment(&self, project_id: &str) -> Result<Box<dyn DeploymentUpdate + '_>> {
        Ok(Box::new(LocalDeploymentUpdate {
            registry: self,
            project_id: project_id.into(),
            _guard: self.updates.lock().await,
        }))
    }

    async fn deployment_contract(&self, project_id: &str) -> Result<Option<PublishedContract>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        Ok(state
            .get(project_id)
            .and_then(|record| record.contract.clone()))
    }

    async fn launch_specs(&self) -> Result<Vec<HostLaunchSpec>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .values()
            .filter_map(|state| state.deployment.clone())
            .collect())
    }

    async fn launch_spec(&self, project_id: &str) -> Result<Option<HostLaunchSpec>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .get(project_id)
            .and_then(|state| state.deployment.clone()))
    }
}

impl LocalAdminRegistry {
    fn remove_unlocked(&self, project_id: &str) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .remove(project_id);
        Ok(())
    }

    fn register_unlocked(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        spec.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        let state = state.entry(spec.project_id.clone()).or_default();
        let contract = contract.map(PublishedContract::new);
        let changed = state.deployment.as_ref() != Some(spec) || state.contract != contract;
        state.deployment = Some(spec.clone());
        state.contract = contract;
        Ok(changed)
    }
}

struct LocalDeploymentUpdate<'a> {
    registry: &'a LocalAdminRegistry,
    project_id: String,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}
#[async_trait]
impl DeploymentUpdate for LocalDeploymentUpdate<'_> {
    async fn register(
        &mut self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        ensure!(
            spec.project_id == self.project_id,
            "deployment project differs from update"
        );
        self.registry.register_unlocked(spec, contract)
    }
    async fn remove(&mut self) -> Result<()> {
        self.registry.remove_unlocked(&self.project_id)
    }
}

pub(crate) struct PostgresAdminRegistry {
    database: PostgresDatabase,
}

impl PostgresAdminRegistry {
    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AdminRegistry for PostgresAdminRegistry {
    async fn lock_deployment(&self, project_id: &str) -> Result<Box<dyn DeploymentUpdate + '_>> {
        let client = deadpool_postgres::Object::take(self.database.connection().await?);
        client
            .query_one(
                "SELECT pg_advisory_lock(hashtext('durable-actors-deployment'), hashtext($1))",
                &[&project_id],
            )
            .await?;
        Ok(Box::new(PostgresDeploymentUpdate {
            client,
            project_id: project_id.into(),
        }))
    }

    async fn deployment_contract(&self, project_id: &str) -> Result<Option<PublishedContract>> {
        let row = self.database.query_opt(
            "SELECT contract_hash, contract_json FROM durable_actors_contracts WHERE project_id = $1",
            &[&project_id]
        ).await?;
        row.map(|row| {
            Ok(PublishedContract {
                contract_hash: row.get(0),
                contract: serde_json::from_str(row.get::<_, &str>(1))?,
            })
        })
        .transpose()
    }

    async fn launch_specs(&self) -> Result<Vec<HostLaunchSpec>> {
        self.database.connection().await?.query(
            "SELECT image_ref, working_directory, actor_entrypoint, secret_refs, code_snapshot, source_json, project_id, sandbox_json FROM durable_actors_deployment",
            &[],
        ).await.context("load PostgreSQL host launch specs")?
            .iter().map(launch_spec_from_row).collect()
    }

    async fn launch_spec(&self, project_id: &str) -> Result<Option<HostLaunchSpec>> {
        self
            .database
            .query_opt(
                "SELECT image_ref, working_directory, actor_entrypoint, secret_refs, code_snapshot, source_json, project_id, sandbox_json \
                 FROM durable_actors_deployment WHERE project_id = $1",
                &[&project_id],
            )
            .await
            .context("load PostgreSQL host launch spec")?
            .as_ref().map(launch_spec_from_row)
            .transpose()
    }
}

// The owning session performs publication; losing it fences the update and releases the lock.
struct PostgresDeploymentUpdate {
    client: deadpool_postgres::ClientWrapper,
    project_id: String,
}
#[async_trait]
impl DeploymentUpdate for PostgresDeploymentUpdate {
    async fn register(
        &mut self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        ensure!(
            spec.project_id == self.project_id,
            "deployment project differs from update"
        );
        spec.validate()?;
        let transaction = self.client.transaction().await?;
        let changed = transaction.execute(
            "INSERT INTO durable_actors_deployment
                (project_id, image_ref, working_directory, actor_entrypoint, secret_refs, code_snapshot, source_json, sandbox_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (project_id) DO UPDATE SET
                image_ref = EXCLUDED.image_ref, working_directory = EXCLUDED.working_directory,
                actor_entrypoint = EXCLUDED.actor_entrypoint, secret_refs = EXCLUDED.secret_refs,
                code_snapshot = EXCLUDED.code_snapshot, source_json = EXCLUDED.source_json, sandbox_json = EXCLUDED.sandbox_json,
                updated_at = clock_timestamp()
             WHERE (durable_actors_deployment.image_ref, durable_actors_deployment.working_directory,
                    durable_actors_deployment.actor_entrypoint, durable_actors_deployment.secret_refs,
                    durable_actors_deployment.code_snapshot, durable_actors_deployment.source_json, durable_actors_deployment.sandbox_json)
                IS DISTINCT FROM (EXCLUDED.image_ref, EXCLUDED.working_directory, EXCLUDED.actor_entrypoint,
                                  EXCLUDED.secret_refs, EXCLUDED.code_snapshot, EXCLUDED.source_json, EXCLUDED.sandbox_json)",
            &[&spec.project_id, &spec.image_ref, &spec.working_directory, &spec.actor_entrypoint,
              &spec.secret_refs, &spec.code_snapshot, &spec.source.as_ref().map(serde_json::to_string).transpose()?, &serde_json::to_string(&spec.sandboxes)?]
        ).await.context("register PostgreSQL deployment")? > 0;
        let published = publish_contract(&transaction, &spec.project_id, contract).await?;
        transaction.commit().await?;
        Ok(changed || published)
    }

    async fn remove(&mut self) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM durable_actors_deployment WHERE project_id=$1",
                &[&self.project_id],
            )
            .await?;
        Ok(())
    }
}

async fn publish_contract(
    transaction: &tokio_postgres::Transaction<'_>,
    project_id: &str,
    contract: Option<&PublicActorContract>,
) -> Result<bool> {
    let updated = match contract {
        Some(contract) => transaction.execute(
            "INSERT INTO durable_actors_contracts (project_id, contract_hash, contract_json) VALUES ($1, $2, $3)
             ON CONFLICT (project_id) DO UPDATE SET contract_hash = EXCLUDED.contract_hash, contract_json = EXCLUDED.contract_json
             WHERE durable_actors_contracts.contract_hash IS DISTINCT FROM EXCLUDED.contract_hash",
            &[&project_id, &contract.hash(), &serde_json::to_string(contract.document())?]
        ).await?,
        None => transaction.execute("DELETE FROM durable_actors_contracts WHERE project_id = $1", &[&project_id]).await?,
    };
    Ok(updated > 0)
}

fn launch_spec_from_row(row: &tokio_postgres::Row) -> Result<HostLaunchSpec> {
    Ok(HostLaunchSpec {
        sandboxes: serde_json::from_str(row.get::<_, &str>(7))?,
        project_id: row.get(6),
        source: row
            .get::<_, Option<&str>>(5)
            .map(serde_json::from_str)
            .transpose()?,
        code_snapshot: row.get(4),
        image_ref: row.get(0),
        working_directory: row.get(1),
        actor_entrypoint: row.get(2),
        secret_refs: row.get(3),
    })
}

pub(crate) fn validate_component(name: &str, value: &str, maximum: usize) -> Result<()> {
    ensure!(!value.is_empty(), "{name} must not be empty");
    ensure!(
        value.len() <= maximum,
        "{name} must be at most {maximum} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
        "{name} contains unsupported characters"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/admin.rs"]
mod tests;

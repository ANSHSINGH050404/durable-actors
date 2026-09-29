use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
    Api, Client, ResourceExt,
    api::{AttachParams, DeleteParams, ListParams, PostParams, Preconditions},
    runtime::wait::await_condition,
};
use serde_json::json;
use tokio::io::AsyncWrite;

use super::*;

#[derive(Clone, Debug)]
pub(crate) struct GkeConfig {
    pub namespace: String,
    pub zones: BTreeMap<String, String>,
    pub public_origin: String,
    pub artifact_bucket: String,
}

pub(super) struct Kubernetes {
    pods: Api<Pod>,
    secrets: Api<Secret>,
    config: GkeConfig,
    _cleanup: tokio_util::task::AbortOnDropHandle<()>,
}
impl Kubernetes {
    pub fn new(client: Client, config: GkeConfig) -> Self {
        let pods = Api::namespaced(client.clone(), &config.namespace);
        let cleanup_pods = pods.clone();
        let cleanup = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if let Err(error) = reap_completed(&cleanup_pods).await {
                    tracing::warn!(%error, "completed pod cleanup failed");
                }
            }
        }));
        Self {
            pods,
            secrets: Api::namespaced(client, &config.namespace),
            config,
            _cleanup: cleanup,
        }
    }

    async fn start(&self, pod: Pod) -> Result<Pod> {
        let created = self.pods.create(&PostParams::default(), &pod).await?;
        let mut cleanup = PodCleanup::new(self.pods.clone(), &created)?;
        let ready = tokio::time::timeout(
            Duration::from_secs(pod_start_timeout(&created)),
            await_condition(
                self.pods.clone(),
                &created.name_any(),
                |pod: Option<&Pod>| pod.is_none_or(terminal_or_ready),
            ),
        )
        .await
        .context("sandbox pod readiness timed out")??
        .context("sandbox pod disappeared")?;
        ensure!(
            ready.uid() == created.uid(),
            "sandbox pod was replaced during startup"
        );
        ensure!(is_ready(&ready), "sandbox pod failed: {:?}", ready.status);
        cleanup.disarm();
        Ok(ready)
    }

    async fn copy_file(
        &self,
        pod: &str,
        path: &str,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        let mut process = self
            .pods
            .exec(
                pod,
                ["/bin/cat", path],
                &AttachParams::default().container("runtime").stderr(false),
            )
            .await?;
        let status = process.take_status().context("exec status missing")?;
        tokio::io::copy(
            &mut process.stdout().context("exec stdout missing")?,
            destination,
        )
        .await?;
        let status = status.await.context("exec status was not received")?;
        ensure!(
            status.status.as_deref() == Some("Success"),
            "artifact read failed: {:?}",
            status.message
        );
        process.join().await?;
        Ok(())
    }

    fn zone(&self, region: &str) -> Result<&str> {
        self.config
            .zones
            .get(region)
            .map(String::as_str)
            .with_context(|| format!("no GKE zone configured for {region}"))
    }
}

#[async_trait]
impl SandboxCluster for Kubernetes {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let pod = spare_pod(request, self.zone(&request.canonical_region)?, &token)?;
        let ready = self.start(pod).await?;
        let ip: std::net::IpAddr = ready
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
            .context("ready pod has no IP")?
            .parse()?;
        let authority = if ip.is_ipv6() {
            format!("[{ip}]")
        } else {
            ip.to_string()
        };
        Ok(SpareHandle {
            name: ready.name_any(),
            resource_id: format!(
                "{}/{}/{}",
                self.config.namespace,
                ready.name_any(),
                ready.uid().context("pod UID missing")?
            ),
            control_route: format!("http://{authority}:7102"),
            control_token: token,
            route: format!("http://{authority}:7101"),
            canonical_region: request.canonical_region.clone(),
        })
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        if spare.resource_id.is_empty() {
            let Some(pod) = self.pods.get_opt(&spare.name).await? else {
                return Ok(());
            };
            ensure!(
                pod.labels()
                    .get("app.kubernetes.io/managed-by")
                    .is_some_and(|owner| owner == "terse"),
                "refusing to retire an unmanaged pod"
            );
            return delete_pod(
                &self.pods,
                &spare.name,
                &pod.uid().context("pod UID missing")?,
            )
            .await;
        }
        let (namespace, name, uid) = resource_identity(&spare.resource_id)?;
        ensure!(
            namespace == self.config.namespace && name == spare.name,
            "sandbox resource identity mismatch"
        );
        delete_pod(&self.pods, name, uid).await
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        let pods: HashMap<_, _> = self
            .pods
            .list(&ListParams::default())
            .await?
            .items
            .into_iter()
            .map(|pod| (pod.name_any(), pod))
            .collect();
        let mut stopped = Vec::new();
        for spare in spares {
            let (namespace, name, uid) = resource_identity(&spare.resource_id)?;
            ensure!(
                namespace == self.config.namespace && name == spare.name,
                "sandbox resource identity mismatch"
            );
            let live = pods.get(name).is_some_and(|pod| {
                pod.uid().as_deref() == Some(uid)
                    && !pod
                        .status
                        .as_ref()
                        .and_then(|status| status.phase.as_deref())
                        .is_some_and(|phase| matches!(phase, "Failed" | "Succeeded"))
            });
            if !live {
                stopped.push(spare.resource_id.clone());
            }
        }
        Ok(stopped)
    }

    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>> {
        let loaded =
            futures_util::future::try_join_all(names.iter().map(|name| self.secrets.get(name)))
                .await?;
        let mut environment = HashMap::new();
        for secret in loaded {
            ensure!(
                secret
                    .labels()
                    .get("terse.ai/customer-secret")
                    .is_some_and(|value| value == "true"),
                "secret is not marked for customer code"
            );
            for (name, value) in secret.data.unwrap_or_default() {
                ensure!(
                    !environment.contains_key(&name),
                    "duplicate customer environment variable {name}"
                );
                environment.insert(
                    name,
                    String::from_utf8(value.0).context("customer secret must be UTF-8")?,
                );
            }
        }
        Ok(environment)
    }

    async fn build_code(&self, request: &BuildCodeRequest) -> Result<CompiledCode> {
        let pod = build_pod(request, self.zone(&request.canonical_region)?)?;
        let ready = self.start(pod).await?;
        let cleanup = PodCleanup::new(self.pods.clone(), &ready)?;
        let directory = tempfile::tempdir()?;
        let mut contract = Vec::new();
        let mut files = Vec::new();
        let name = ready.name_any();
        tokio::try_join!(
            self.copy_file(&name, "/tmp/build/contract.json", &mut contract),
            self.copy_file(&name, "/tmp/build/files.json", &mut files)
        )?;
        let paths: Vec<String> = serde_json::from_slice(&files)?;
        let root = directory.path();
        let name = &name;
        futures_util::future::try_join_all(paths.iter().map(|path| async move {
            crate::artifacts::validate_path(path)?;
            let destination = root.join(path);
            tokio::fs::create_dir_all(destination.parent().context("artifact parent missing")?)
                .await?;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .await?;
            self.copy_file(&name, &format!("/tmp/build/{path}"), &mut file)
                .await
        }))
        .await?;
        let contract =
            serde_json::from_slice(&contract).context("decode compiled actor contract")?;
        cleanup.delete().await?;
        Ok(CompiledCode {
            directory,
            contract,
        })
    }
}

async fn reap_completed(pods: &Api<Pod>) -> Result<()> {
    for phase in ["Failed", "Succeeded"] {
        let params = ListParams::default()
            .labels("app.kubernetes.io/managed-by=terse,terse.ai/purpose!=replica")
            .fields(&format!("status.phase={phase}"));
        for pod in pods.list(&params).await? {
            delete_pod(
                pods,
                &pod.name_any(),
                &pod.uid().context("pod UID missing")?,
            )
            .await?;
        }
    }
    Ok(())
}

fn spare_pod(request: &CreateSpareRequest, zone: &str, token: &str) -> Result<Pod> {
    let mut pod = base_pod(&request.name, &request.image_ref, zone, &request.resources);
    pod["metadata"]["annotations"] =
        json!({"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"});
    pod["spec"]["containers"][0]["env"] = json!([
        {"name":"DURABLE_ACTORS_PROCESS_ROLE", "value":"spare"},
        {"name":"DURABLE_ACTORS_SPARE_TOKEN", "value":token},
        {"name":"DURABLE_ACTORS_CONTROL_PLANE_URL", "value":request.control_plane_url.as_ref().context("spare control plane URL required")?}
    ]);
    pod["spec"]["containers"][0]["readinessProbe"] = json!({"exec":{"command":["/usr/bin/test", "-f", "/tmp/durable-actors-spare-ready"]}, "periodSeconds":1, "failureThreshold":3});
    Ok(serde_json::from_value(pod)?)
}

fn pod_start_timeout(pod: &Pod) -> u64 {
    if pod
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("terse.ai/purpose"))
        .is_some_and(|purpose| purpose == "build")
    {
        720
    } else {
        120
    }
}

fn build_pod(request: &BuildCodeRequest, zone: &str) -> Result<Pod> {
    let mut pod = base_pod(
        &format!("do-build-{}", uuid::Uuid::new_v4()),
        &request.image_ref,
        zone,
        &ResourceLimits {
            cpu_millis: 1000,
            memory_mib: 1024,
        },
    );
    pod["metadata"]["labels"]["terse.ai/purpose"] = json!("build");
    pod["spec"]["activeDeadlineSeconds"] = json!(720);
    pod["spec"]["containers"][0]["command"] = json!([
        "/bin/sh",
        "-ec",
        "mkdir -p /tmp/build; bun /opt/durable-actors/sdk/dist/compiler/deployment-build.js \"$1\" \"$2\" /tmp/build > /tmp/build/contract.json; python3 -c \"import json; from pathlib import Path; root = Path('/tmp/build'); print(json.dumps([str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p not in (root / 'contract.json', root / 'files.json')]))\" > /tmp/build/files.json; touch /tmp/build/ready; exec sleep infinity",
        "build",
        request.working_directory,
        request.actor_entrypoint
    ]);
    pod["spec"]["containers"][0]["readinessProbe"] =
        json!({"exec":{"command":["/usr/bin/test", "-f", "/tmp/build/ready"]}, "periodSeconds":1});
    // Build images contain the source at /customer; only runtime pods replace it with emptyDir.
    pod["spec"]["containers"][0]["volumeMounts"]
        .as_array_mut()
        .unwrap()
        .retain(|mount| mount["name"] != "customer");
    Ok(serde_json::from_value(pod)?)
}

fn base_pod(name: &str, image: &str, zone: &str, resources: &ResourceLimits) -> serde_json::Value {
    json!({
        "apiVersion":"v1", "kind":"Pod", "metadata":{"name":name, "labels":{"app.kubernetes.io/managed-by":"terse", "terse.ai/purpose":"actor"}},
        "spec":{
            "runtimeClassName":"gvisor", "automountServiceAccountToken":false, "serviceAccountName":"sandbox",
            "restartPolicy":"Never", "terminationGracePeriodSeconds":15,
            "nodeSelector":{"topology.kubernetes.io/zone":zone, "sandbox.gke.io/runtime":"gvisor"},
            "securityContext":{"runAsNonRoot":true, "runAsUser":10000, "runAsGroup":10000, "fsGroup":10000},
            "containers":[{"name":"runtime", "image":image, "imagePullPolicy":"IfNotPresent", "command":["/usr/local/bin/durable-actors"],
                "securityContext":{"allowPrivilegeEscalation":false, "readOnlyRootFilesystem":true, "capabilities":{"drop":["ALL"]}},
                "resources":{"requests":{"cpu":format!("{}m", resources.cpu_millis),"memory":format!("{}Mi", resources.memory_mib)},"limits":{"cpu":format!("{}m", resources.cpu_millis),"memory":format!("{}Mi", resources.memory_mib)}},
                "volumeMounts":[{"name":"tmp","mountPath":"/tmp"},{"name":"customer","mountPath":"/customer"}]
            }], "volumes":[{"name":"tmp","emptyDir":{}},{"name":"customer","emptyDir":{}}]
        }
    })
}

fn terminal_or_ready(pod: &Pod) -> bool {
    is_ready(pod)
        || pod
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref())
            .is_some_and(|phase| matches!(phase, "Failed" | "Succeeded"))
}
fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}
fn resource_identity(resource: &str) -> Result<(&str, &str, &str)> {
    let mut parts = resource.split('/');
    let namespace = parts.next().context("namespace missing")?;
    let name = parts.next().context("pod name missing")?;
    let uid = parts.next().context("pod UID missing")?;
    ensure!(
        !namespace.is_empty() && !name.is_empty() && !uid.is_empty() && parts.next().is_none(),
        "invalid pod identity"
    );
    Ok((namespace, name, uid))
}
async fn delete_pod(pods: &Api<Pod>, name: &str, uid: &str) -> Result<()> {
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid.into()),
            resource_version: None,
        }),
        ..DeleteParams::default()
    };
    match pods.delete(name, &params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(error)) if error.code == 404 => Ok(()),
        Err(error) => Err(error.into()),
    }
}
struct PodCleanup {
    pods: Api<Pod>,
    identity: Option<(String, String)>,
}
impl PodCleanup {
    fn new(pods: Api<Pod>, pod: &Pod) -> Result<Self> {
        Ok(Self {
            pods,
            identity: Some((pod.name_any(), pod.uid().context("pod UID missing")?)),
        })
    }
    fn disarm(&mut self) {
        self.identity = None;
    }
    async fn delete(mut self) -> Result<()> {
        let (name, uid) = self.identity.as_ref().unwrap();
        delete_pod(&self.pods, name, uid).await?;
        self.disarm();
        Ok(())
    }
}
impl Drop for PodCleanup {
    fn drop(&mut self) {
        if let Some((name, uid)) = self.identity.take() {
            let pods = self.pods.clone();
            tokio::spawn(async move {
                if let Err(error) = delete_pod(&pods, &name, &uid).await {
                    tracing::warn!(%error, %name, "sandbox cleanup failed");
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/kubernetes.rs"]
mod tests;

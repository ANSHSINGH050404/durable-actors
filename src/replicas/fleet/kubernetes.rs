use super::{ObservedPod, PodHealth, PodInventory, PodRecord, ReplicaPods};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions},
    runtime::wait::await_condition,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

#[derive(Clone, Debug)]
pub(crate) struct ReplicaPodConfig {
    pub namespace: String,
    pub image: String,
    pub archive_bucket: String,
    pub credentials_secret: String,
    pub resources: serde_json::Value,
}

pub(crate) struct KubernetesReplicas {
    pods: Api<Pod>,
    nodes: Api<Node>,
    http: reqwest::Client,
    secret: String,
    config: ReplicaPodConfig,
}
impl KubernetesReplicas {
    pub fn new(client: Client, config: ReplicaPodConfig, secret: String) -> Result<Self> {
        crate::sandbox::gke::validate_image(&config.image)?;
        Ok(Self {
            pods: Api::namespaced(client.clone(), &config.namespace),
            nodes: Api::all(client),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()?,
            secret,
            config,
        })
    }

    async fn register(&self, expected: &PodRecord, ready: Pod) -> Result<PodRecord> {
        let uid = ready.uid().context("replica pod UID missing")?;
        ensure!(
            expected.uid.as_ref().is_none_or(|old| *old == uid),
            "replica pod identity changed"
        );
        let ip: std::net::IpAddr = ready
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.as_deref())
            .context("replica pod IP missing")?
            .parse()?;
        let address = format!("http://{}", std::net::SocketAddr::new(ip, 7200));
        let id: String = self
            .http
            .get(format!("{address}/identity"))
            .bearer_auth(&self.secret)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            expected
                .placement
                .as_ref()
                .is_none_or(|old| old.id == id && old.address == address),
            "replica disk identity changed"
        );
        Ok(PodRecord {
            name: expected.name.clone(),
            zone: expected.zone.clone(),
            uid: Some(uid),
            node: ready.spec.and_then(|s| s.node_name),
            placement: Some(crate::bucket::ReplicaPlacement {
                id,
                address,
                zone: expected.zone.clone(),
            }),
        })
    }
}

#[async_trait]
impl ReplicaPods for KubernetesReplicas {
    async fn ensure(
        &self,
        expected: &PodRecord,
        group: Option<&str>,
        excluded_nodes: &[String],
    ) -> Result<PodRecord> {
        let pod = match self.pods.get_opt(&expected.name).await? {
            Some(pod) => pod,
            None => {
                ensure!(expected.uid.is_none(), "registered replica pod disappeared");
                let pod = replica_pod(expected, group, excluded_nodes, &self.config)?;
                match self.pods.create(&PostParams::default(), &pod).await {
                    Ok(pod) => pod,
                    Err(kube::Error::Api(error)) if error.code == 409 => {
                        self.pods.get(&expected.name).await?
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        };
        ensure!(
            pod.labels()
                .get("terse.ai/purpose")
                .is_some_and(|purpose| purpose == "replica"),
            "replica pod belongs to another workload"
        );
        let original_uid = pod.uid();
        let ready = if is_ready(&pod) {
            pod
        } else {
            tokio::time::timeout(
                Duration::from_secs(120),
                await_condition(self.pods.clone(), &expected.name, |pod: Option<&Pod>| {
                    pod.is_none_or(|pod| is_ready(pod) || terminal(pod))
                }),
            )
            .await
            .context("replica startup timed out")??
            .context("replica pod disappeared during startup")?
        };
        ensure!(
            ready.uid() == original_uid && is_ready(&ready),
            "replica pod failed or changed during startup"
        );
        self.register(expected, ready).await
    }

    async fn health(&self, expected: &PodRecord, inventory: &PodInventory) -> Result<PodHealth> {
        if let Some(observed) = inventory.get(&expected.name) {
            let mut health = observed.health;
            health.live &= expected.uid.is_some() && expected.uid == observed.pod.uid;
            return Ok(health);
        }
        // A group may have been registered after this reconciliation's inventory read.
        let Some(pod) = self.pods.get_opt(&expected.name).await? else {
            return Ok(PodHealth {
                live: false,
                draining: false,
                current_image: true,
            });
        };
        let node = match pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) {
            Some(name) => self.nodes.get_opt(name).await?,
            None => None,
        };
        let mut health = pod_health(&pod, node.as_ref(), &self.config.image);
        health.live &= expected.uid.is_some() && expected.uid == pod.uid();
        Ok(health)
    }

    async fn observed(&self) -> Result<PodInventory> {
        let pod_params = ListParams::default().labels("terse.ai/purpose=replica");
        let node_params = ListParams::default();
        let (pods, nodes) =
            tokio::try_join!(self.pods.list(&pod_params), self.nodes.list(&node_params))?;
        let nodes: HashMap<_, _> = nodes
            .items
            .into_iter()
            .map(|node| (node.name_any(), node))
            .collect();
        Ok(pods
            .items
            .into_iter()
            .map(|pod| {
                let node = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .and_then(|name| nodes.get(name));
                let observed = ObservedPod {
                    health: pod_health(&pod, node, &self.config.image),
                    prefix: pod.annotations().get("terse.ai/replica-prefix").cloned(),
                    pod: PodRecord {
                        name: pod.name_any(),
                        zone: String::new(),
                        uid: pod.uid(),
                        node: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                        placement: None,
                    },
                };
                (pod.name_any(), observed)
            })
            .collect())
    }

    async fn protect(&self, pod: &PodRecord, group: &str) -> Result<()> {
        let uid = pod.uid.as_ref().context("replica UID missing")?;
        let patch = json!({"metadata":{"uid":uid,"labels":{"terse.ai/assigned":"true","terse.ai/replica-group":group_label(group)},"annotations":{"terse.ai/replica-prefix":group,"cluster-autoscaler.kubernetes.io/safe-to-evict":"false"}}});
        self.pods
            .patch(&pod.name, &PatchParams::default(), &Patch::Merge(patch))
            .await?;
        Ok(())
    }

    async fn retire(&self, expected: &PodRecord) -> Result<()> {
        let Some(pod) = self.pods.get_opt(&expected.name).await? else {
            return Ok(());
        };
        ensure!(
            pod.labels()
                .get("terse.ai/purpose")
                .is_some_and(|purpose| purpose == "replica"),
            "refusing to delete an unrelated pod"
        );
        let uid = pod.uid().context("replica UID missing")?;
        if expected
            .uid
            .as_ref()
            .is_some_and(|expected| *expected != uid)
        {
            return Ok(());
        }
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some(uid),
                resource_version: None,
            }),
            ..DeleteParams::default()
        };
        match self.pods.delete(&expected.name, &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(error)) if error.code == 404 => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn replica_pod(
    pod: &PodRecord,
    group: Option<&str>,
    excluded_nodes: &[String],
    config: &ReplicaPodConfig,
) -> Result<Pod> {
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/managed-by", "terse".to_owned()),
        ("terse.ai/purpose", "replica".to_owned()),
    ]);
    let mut annotations = BTreeMap::from([(
        "cluster-autoscaler.kubernetes.io/safe-to-evict",
        if group.is_some() { "false" } else { "true" }.to_owned(),
    )]);
    if let Some(group) = group {
        labels.insert("terse.ai/replica-group", group_label(group));
        labels.insert("terse.ai/assigned", "true".into());
        annotations.insert("terse.ai/replica-prefix", group.into());
    }
    let mut value = json!({
        "apiVersion":"v1","kind":"Pod","metadata":{"name":pod.name,"labels":labels,"annotations":annotations},
        "spec":{
            "serviceAccountName":"replica","automountServiceAccountToken":false,"runtimeClassName":"gvisor","restartPolicy":"Never","terminationGracePeriodSeconds":30,
            "nodeSelector":{"topology.kubernetes.io/zone":pod.zone,"sandbox.gke.io/runtime":"gvisor"},
            "securityContext":{"runAsNonRoot":true,"runAsUser":10000,"runAsGroup":10000,"fsGroup":10000},
            "containers":[{"name":"replica","image":config.image,"imagePullPolicy":"IfNotPresent","command":["/usr/local/bin/durable-actors"],
                "env":[{"name":"DURABLE_ACTORS_PROCESS_ROLE","value":"replica"},{"name":"DURABLE_ACTORS_ARCHIVE_BUCKET","value":config.archive_bucket},{"name":"DURABLE_ACTORS_REPLICA_SECRET","valueFrom":{"secretKeyRef":{"name":config.credentials_secret,"key":"replica-key"}}}],
                "resources":config.resources,
                "securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}},
                "volumeMounts":[{"name":"data","mountPath":"/tmp"}],"ports":[{"name":"storage","containerPort":7200}],
                "readinessProbe":{"httpGet":{"path":"/health","port":"storage"},"periodSeconds":1,"failureThreshold":3}
            }],"volumes":[{"name":"data","emptyDir":{}}],
            "topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"kubernetes.io/hostname","whenUnsatisfiable":"ScheduleAnyway","labelSelector":{"matchLabels":{"terse.ai/purpose":"replica"}}}]
        }
    });
    if !excluded_nodes.is_empty() {
        value["spec"]["affinity"]["nodeAffinity"] = json!({"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"NotIn","values":excluded_nodes}]}]}});
    }
    if let Some(group) = group {
        value["spec"]["affinity"]["podAntiAffinity"] = json!({"requiredDuringSchedulingIgnoredDuringExecution":[{"topologyKey":"kubernetes.io/hostname","labelSelector":{"matchLabels":{"terse.ai/replica-group":group_label(group)}}}]});
    }
    Ok(serde_json::from_value(value)?)
}
fn group_label(prefix: &str) -> String {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, prefix.as_bytes())
        .as_ref()
        .iter()
        .take(30)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn terminal(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|p| matches!(p, "Failed" | "Succeeded"))
}
fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

fn pod_health(pod: &Pod, node: Option<&Node>, image: &str) -> PodHealth {
    PodHealth {
        live: !terminal(pod),
        draining: pod.metadata.deletion_timestamp.is_some()
            || node
                .and_then(|n| n.spec.as_ref())
                .is_some_and(|s| s.unschedulable == Some(true)),
        current_image: pod
            .spec
            .as_ref()
            .and_then(|s| s.containers.first())
            .and_then(|c| c.image.as_deref())
            == Some(image),
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/replicas/kubernetes.rs"]
mod tests;

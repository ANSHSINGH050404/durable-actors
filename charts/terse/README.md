# Terse on GKE Sandbox

The chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. Each active actor uses one gVisor pod. State writes go directly to precreated GCS Rapid buckets; no storage pods are started, assigned, or contacted during activation.

Each write appends the same state record to persistent streams in two Rapid zones and waits for both durable flushes. Standard GCS stores manifests, checkpoints, and archived segments. The host opens connections alongside code and state loading; read-only activation does not wait for log setup.

## Prerequisites

- Regional GKE Standard with Workload Identity, enforced NetworkPolicy, Gateway API, and ordinary and COS Sandbox node pools across the configured compute zones. Run the control plane on ordinary nodes.
- Standard authority, artifact, and archive buckets with uniform access and public access prevention. Keep the archive indefinitely; its location determines the permanent failure domain. Do not expire referenced actor history or code.
- Two Rapid buckets in supported distinct zones, without automatic deletion of log objects. Buckets are shared infrastructure; actor data is isolated by credential prefixes.
- PostgreSQL, preferably private Cloud SQL with regional HA. The database user needs migration privileges.
- A Google service account with `roles/storage.objectUser` on the application buckets and `storage.buckets.get` on the Rapid and archive buckets. `roles/storage.legacyBucketReader` supplies the latter at bucket scope. Bind `<control-namespace>/<release>-terse` with `roles/iam.workloadIdentityUser`; customer pods use downscoped credentials and cannot reach the metadata server.
- A control-namespace Secret containing `postgres-url`, `api-key`, and `jwt-signing-key` (base64 Ed25519 PKCS#8).
- A matching TLS Secret or Google-managed Compute Engine certificate. Set exactly one of `gateway.tlsSecret` and `gateway.preSharedCert`.

## Provision storage

Use Google Cloud CLI 553 or newer. Substitute your own globally unique bucket names. Create each Rapid bucket once:

```sh
gcloud storage buckets create gs://ACTOR_RAPID_A --project=PROJECT \
  --location=us-west4 --placement=us-west4-a --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets create gs://ACTOR_RAPID_B --project=PROJECT \
  --location=us-west4 --placement=us-west4-b --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
```

The application archives segments before deleting their Rapid copies. Do not configure lifecycle deletion for `durable-actors-v3-logs-`, or for permanent Standard state. Startup rejects Rapid deletion rules that could match the log prefix. A crashed host's retained logs must remain available until safely archived.

## Install

Copy `values.yaml` and supply the image digest, Google service account, bucket names, namespaces, and zones:

```yaml
replicaCount: 2
zones:
  north-america-west: [us-west4-a, us-west4-b, us-west4-c]
cloudSql:
  instanceConnectionName: project:us-west4:actors-db
storage:
  authorityBucket: actor-ownership
  artifactBucket: actor-code
  archiveBucket: actor-archive
  rapid:
    buckets:
      - {bucket: actor-rapid-a, zone: us-west4-a}
      - {bucket: actor-rapid-b, zone: us-west4-b}
```

Exactly two Rapid buckets in different zones are required. Startup verifies their actual GCS storage class, placement, and retention rules. Keep storage configuration identical across controllers and immutable for existing ownership records.

```sh
helm lint charts/terse -f production-values.yaml
helm template actors charts/terse --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/terse --namespace terse-control --create-namespace -f production-values.yaml
```

With Cloud SQL, `postgres-url` points to `127.0.0.1:5432`; the native proxy sidecar starts before the control plane. Leave `cloudSql.instanceConnectionName` empty for a direct database connection. Configure DNS and certificates separately. `networkPolicy.dnsCidrs` allows host-networked or link-local DNS listeners.

## Writes and recovery

The first write waits for a durable manifest binding both object generations to the actor's ownership epoch. Subsequent writes reuse those streams; they perform no finalize, rename, ownership update, or Standard upload. A failed or cancelled write fences that activation. If either Rapid stream cannot be opened, the activation uses immutable Standard snapshots.

Segments rotate at 8 MiB, with a checkpoint worker checking every 60 seconds. Clean shutdown archives all records and records the final snapshot in ownership. Clean resume reads that snapshot directly. Crash recovery waits for lease expiry, fences available old streams, verifies records, and makes recovered state durable in Standard before claiming a new ownership epoch. One surviving Rapid zone can recover acknowledged records because every acknowledgment required both copies. An interrupted request can have an uncertain outcome.

Actor-scoped credentials cover only the ownership object, the actor's log and archive prefixes, and its immutable deployment code.

## Capacity and deployment

WebSockets can use independently sized replicas of the same control-plane image:

```yaml
sockets:
  dedicatedGateway: true
  replicaCount: 4
  maxConnectionsPerActor: 32768
  resources:
    requests: {cpu: "1", memory: 1Gi}
    limits: {memory: 2Gi}
```

This adds a `-sockets` Deployment, Service, disruption budget, and GKE backend/health policies. The HTTPS route sends exact `/v1/socket` requests to those replicas; other routes and internal host RPCs use the original control-plane Service. `sockets.nodeSelector` controls placement separately. Isolation is disabled by default. With an external ingress (`gateway.enabled: false`), configure the equivalent WebSocket route yourself.

The connection limit applies per actor at its socket-owning gateway, even with isolation disabled. PostgreSQL coordinates room ownership across replicas; ordinary control-plane pods are ineligible when dedicated gateways are enabled. See the [runtime limits](../../docs/reference/configuration.md#websockets). Scale socket replicas for aggregate connections and traffic; a busy individual actor still executes on one host. Changing the route or replacing gateway pods disconnects their existing sockets. Idle sandbox shutdown preserves connections, metadata, tags, and automatic responses at the gateway. The next application message activates a replacement sandbox.

The chart defaults to two control-plane replicas. The default pool keeps 64 ready pods per image and region, shared across those replicas, with 0.5 CPU and 256 MiB per pod. `pool.fleetMaximum` bounds idle spares, not active actors. An exhausted pool creates new pods. State records above 4 MiB use Standard; large state increases memory and transfer costs.

To keep node capacity available for new hosts and spare replenishment, the chart enables an active [GKE CapacityBuffer](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/configure-capacity-buffer) with these defaults:

```yaml
capacityBuffer:
  enabled: true
  replicas: 32
```

The default buffer requires GKE's Preview CapacityBuffer API (`autoscaling.x-k8s.io/v1beta1`, active buffers on GKE 1.35.2-gke.1842000 or later) and autoscaling on the gVisor node pools. Node auto-provisioning is optional when eligible pools already exist. Set `capacityBuffer.enabled: false` on clusters without this API.

Each buffer slot uses `pool.cpuMillis` and `pool.memoryMiB` and targets gVisor nodes in `zones[region]`. With the defaults, 32 slots reserve 16 CPUs and 8 GiB in addition to active hosts and the ready spare pool. This is one buffer per chart release for the selected region, independent of the number of runtime images. Hosts with larger per-actor resource overrides might need more capacity than a slot provides.

GKE maintains running-node headroom and replenishes it as pods consume capacity. Extra nodes incur normal compute charges and remain subject to autoscaling limits and quotas. The buffer does not initialize actor processes or preload runtime images; `pool.idle` still controls ready sandboxes. Changes can take up to five minutes to affect provisioning, so establish the reserve before a benchmark or expected burst. Inspect the buffer, scheduling events, and node readiness with:

```sh
kubectl get capacitybuffers.autoscaling.x-k8s.io -n terse-sandboxes
kubectl describe capacitybuffer actors-terse-capacity-buffer -n terse-sandboxes
kubectl get nodes -l sandbox.gke.io/runtime=gvisor
```

These names assume release `actors` and the default sandbox namespace. Set `capacityBuffer.enabled: false` and upgrade the release to remove the buffer; GKE can then scale down surplus nodes.

This is a breaking development storage format. Provision fresh ownership records or import state explicitly, then replace controllers and actor images together. Existing SQL migrations retain their checksums.

Source builds use the pinned Google Rust SDK's opt-in append API. Repository and Docker builds set `--cfg google_cloud_unstable_storage_bidi` in `.cargo/config.toml`. Builds outside this repository, including `cargo install`, must supply `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`.

# Self-host Durable Actors on GCP

Deploy Durable Actors on GKE Sandbox with PostgreSQL and one Standard GCS bucket. Standard GCS is the default. GCS Rapid is an optional acceleration for workloads that need lower state-write latency.

## Prerequisites

- GKE Standard with Workload Identity Federation and enforced NetworkPolicy. Use an ordinary node pool for the controller and a COS Sandbox node pool with the `gvisor` RuntimeClass for actors. The actor pool must have nodes in your chosen `placement.zone`.
- PostgreSQL reachable from the controller, with a database user allowed to run migrations. Use a TLS connection for a remote database. A private Cloud SQL instance is suitable when your cluster has a supported connection to it.
- An HTTPS hostname and certificate. The chart can create an Ingress for an existing controller, or you can route an existing HTTPS gateway to its Service. The endpoint must support WebSockets.
- Helm, `kubectl`, Google Cloud CLI, and permissions to create the following resources.

Google Cloud is the supported hosting platform. [GKE Sandbox setup](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/sandbox-pods) and [Workload Identity setup](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) describe the cluster configuration.

## Create the bucket and identity

Choose a globally unique bucket name and a region near your actor nodes. These examples use `us-west4` and release/namespace `actors`.

```sh
export PROJECT=my-google-project
export BUCKET=my-unique-actor-state
export GSA="actors@${PROJECT}.iam.gserviceaccount.com"

gcloud storage buckets create "gs://${BUCKET}" --project="$PROJECT" \
  --location=us-west4 --default-storage-class=STANDARD \
  --uniform-bucket-level-access --public-access-prevention
gcloud iam service-accounts create actors --project="$PROJECT"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${GSA}" --role=roles/storage.objectUser
gcloud iam service-accounts add-iam-policy-binding "$GSA" --project="$PROJECT" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${PROJECT}.svc.id.goog[actors/actors-durable-actors]"
```

The bucket stores ownership, immutable code, and state snapshots under separate prefixes. Actor pods receive credentials scoped to their own state and deployed code. Do not configure lifecycle deletion for referenced state or code. Take PostgreSQL backups and rehearse recovery alongside the retained bucket data.

## Create credentials

Create the controller namespace and a stable signing key. Set `POSTGRES_URL` to your reachable PostgreSQL connection string. The Secret remains stable across chart upgrades.

```sh
kubectl create namespace actors
umask 077
openssl genpkey -algorithm ED25519 -outform DER | openssl base64 -A > jwt-signing-key
openssl rand -hex 32 | tr -d '\n' > api-key
kubectl -n actors create secret generic actors-credentials \
  --from-literal=postgres-url="$POSTGRES_URL" \
  --from-file=jwt-signing-key --from-file=api-key
```

Store the generated credentials in your secret manager. Configure your backend with the same API key. Provision your TLS Secret separately in namespace `actors`; its certificate must cover your public hostname.

## Install

Create `actors-values.yaml`, substituting your actual bucket, identity, hostname, zone, ingress class, and TLS Secret:

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west4-a
storage:
  bucket: my-unique-actor-state
serviceAccount:
  googleServiceAccount: actors@my-google-project.iam.gserviceaccount.com
ingress:
  enabled: true
  className: my-ingress
  tlsSecret: actors-tls
```

Set `VERSION` to the desired [Durable Actors release](https://github.com/TerseAI/durable-actors/releases). Published charts already pin the corresponding runtime image digest.

```sh
helm upgrade --install actors \
  "https://github.com/TerseAI/durable-actors/releases/download/v${VERSION}/durable-actors-${VERSION}.tgz" \
  --namespace actors --values actors-values.yaml --wait --timeout 10m
kubectl -n actors rollout status deployment/actors-durable-actors
```

The same chart is published at `oci://ghcr.io/terseai/charts/durable-actors`; use that reference with `--version "$VERSION"` if you prefer OCI distribution.

Point DNS at your ingress controller. With `ingress.enabled: false`, route your HTTPS gateway to `actors-durable-actors.actors.svc.cluster.local:7100`. Configure the controller's WebSocket timeout through `ingress.annotations` or your external gateway configuration. The public origin is required even when the chart does not create ingress.

## Connect and verify

Configure the backend:

```sh
export DURABLE_ACTORS_CONTROL_PLANE_URL=https://actors.example.com
export DURABLE_ACTORS_PROJECT_ID=my-project
export DURABLE_ACTORS_SECRET="$(cat api-key)"
curl --fail https://actors.example.com/healthz
```

Use the [deployment registration API](reference/openapi.md) to register compiled actor code and its contract for your project, then generate your application client using the [TypeScript](reference/typescript-guide.md) or [Python](reference/python-guide.md) guide. A healthy controller does not register or deploy an application automatically. Upload immutable compiled artifacts into the configured bucket and register their manifest; artifact format and registration endpoints are described in the [OpenAPI specification](reference/openapi.yaml).

Verify an actor write and read, restart its sandbox, and read the value again. Also verify a WebSocket connection through your chosen ingress. One controller handles HTTP and sockets by default; actors start on demand. Increase `actors.warm` for lower cold-start latency and `replicaCount` for controller redundancy. These settings do not create additional node pools or guarantee capacity.

## Speed up writes with GCS Rapid

Rapid stores the hot write log in two zones and keeps permanent history in the Standard bucket. Each acknowledged write has durably flushed to both Rapid copies. This can reduce state-write latency; benchmark your workload and account for the extra storage and request costs.

Choose a region with Rapid availability and use Google Cloud CLI 553 or newer. Create two distinct buckets once, then grant object access and bucket-metadata access to the controller identity. The following placements are examples; verify current [Rapid availability and requirements](https://docs.cloud.google.com/storage/docs/rapid-bucket).

```sh
export RAPID_A=my-unique-actor-rapid-a
export RAPID_B=my-unique-actor-rapid-b

gcloud storage buckets create "gs://${RAPID_A}" --project="$PROJECT" \
  --location=us-west4 --placement=us-west4-a --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets create "gs://${RAPID_B}" --project="$PROJECT" \
  --location=us-west4 --placement=us-west4-b --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
for bucket in "$RAPID_A" "$RAPID_B"; do
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --member="serviceAccount:${GSA}" --role=roles/storage.objectUser
done
for bucket in "$BUCKET" "$RAPID_A" "$RAPID_B"; do
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --member="serviceAccount:${GSA}" --role=roles/storage.legacyBucketReader
done
```

Select Rapid in the installation values:

```yaml
storage:
  bucket: my-unique-actor-state
  mode: rapid
  rapid:
    buckets:
      - {bucket: my-unique-actor-rapid-a, zone: us-west4-a}
      - {bucket: my-unique-actor-rapid-b, zone: us-west4-b}
```

The Standard bucket remains required. Startup checks Rapid storage class, actual zonal placement, and retention rules. Do not configure automatic deletion of Rapid log objects: the runtime archives them before removing their Rapid copies. If Rapid streams cannot be opened at activation, the runtime can persist immutable Standard snapshots.

Storage mode and bucket identities are bound to existing actor ownership records. Select the mode before creating actors in an installation. Changing an existing installation's mode needs a planned data migration; a Helm values change alone does not migrate actor state.

## Upgrades and troubleshooting

Pin a release version, review its compatibility notes, and repeat the install command with your saved values. Keep bucket identities and the credentials Secret stable. Clients should reconnect after controller replacement. Kubernetes rollback does not undo database migrations or changes to persisted data.

```sh
kubectl -n actors get pods
kubectl -n actors logs deployment/actors-durable-actors -c control-plane --since=10m
kubectl -n actors-actors get pods
kubectl -n actors-actors get events --sort-by=.lastTimestamp
```

- Pending actor pods: check gVisor node capacity, the selected zone, and resource requests.
- Storage permission errors: check bucket roles, the Google service account annotation, and the Workload Identity binding.
- DNS failures: for host-networked or link-local DNS, add the resolver address to `networkPolicy.dnsCidrs`, such as a `/32` for your cluster's NodeLocal DNS listener.
- Controller startup failures: check PostgreSQL reachability, migration permissions, and the signing-key format.
- WebSocket disconnects: check ingress upgrade support and connection timeouts.

See the [chart values](../charts/durable-actors/values.yaml) for resource limits and optional settings.

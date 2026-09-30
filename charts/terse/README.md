# Terse on GKE Sandbox

The chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. Each active actor uses one gVisor pod. State writes go directly to precreated GCS Rapid buckets; no storage pods are started, assigned, or contacted during activation.

Each write starts Standard and Rapid uploads together. The default acknowledgment requires **Standard to succeed or two distinct Rapid zones to succeed**, whichever happens first. Standard GCS is the permanent archive; when Rapid wins, the Standard upload continues asynchronously. Google Storage Transfer Service retries missing archives hourly, including writes whose actor died before uploading. GCS lifecycle rules expire Rapid snapshots and abandoned uploads after seven days, independently of archival completion.

Seven days is the recovery budget, not a guarantee that archival will finish. Before Standard succeeds, an acknowledged write is protected only by the configured Rapid zones. If all its Rapid copies expire before archival succeeds, the write is lost. Monitor transfer failures and lag well before the retention deadline. Lifecycle deletion is asynchronous; seven days is eligibility, not an exact deletion deadline.

## Prerequisites

- Regional GKE Standard with Workload Identity, enforced NetworkPolicy, Gateway API, and ordinary and COS Sandbox node pools across the configured compute zones. Run the control plane on ordinary nodes.
- Standard authority, artifact, and archive buckets with uniform access and public access prevention. Keep the archive indefinitely; its location determines the permanent failure domain. Do not expire referenced actor history or code.
- Two Rapid buckets in supported distinct zones, with the lifecycle and transfer configuration below. Buckets are shared infrastructure; actor data is isolated by credential prefixes.
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

Edit `rapid-lifecycle.json` to select the retention in days, then apply it to both Rapid buckets. Do not apply this policy to Standard. These commands replace the bucket's lifecycle configuration.

```sh
gcloud storage buckets update gs://ACTOR_RAPID_A --lifecycle-file=charts/terse/rapid-lifecycle.json
gcloud storage buckets update gs://ACTOR_RAPID_B --lifecycle-file=charts/terse/rapid-lifecycle.json
```

Enable `storagetransfer.googleapis.com`. Get the project's Storage Transfer service agent through [googleServiceAccounts.get](https://docs.cloud.google.com/storage-transfer/docs/reference/rest/v1/googleServiceAccounts/get). Grant it `roles/storage.objectViewer` and `roles/storage.legacyBucketReader` on both Rapid sources, and `roles/storage.objectUser` and `roles/storage.legacyBucketReader` on the Standard destination, at bucket scope. Configure one scheduled transfer per source:

```sh
gcloud transfer jobs create gs://ACTOR_RAPID_A gs://ACTOR_ARCHIVE --project=PROJECT \
  --name=actors-rapid-a-to-standard --schedule-repeats-every=1h \
  --include-prefixes=durable-actors-v3-snapshots- --overwrite-when=never
gcloud transfer jobs create gs://ACTOR_RAPID_B gs://ACTOR_ARCHIVE --project=PROJECT \
  --name=actors-rapid-b-to-standard --schedule-repeats-every=1h \
  --include-prefixes=durable-actors-v3-snapshots- --overwrite-when=never
```

Keep these jobs enabled before accepting writes. They copy published immutable snapshots and leave Rapid copies until lifecycle expiration. Do not include `durable-actors-v3-uploads-`: [Storage Transfer can copy unfinalized Rapid objects](https://docs.cloud.google.com/storage/docs/rapid/create-zonal-buckets#transfer_data_with_storage_transfer_service). The runtime uses flat, hashed actor keys to avoid creating hierarchical folders. It finalizes an upload under a separate prefix and atomically moves it into the published snapshot prefix before acknowledging that zone.

Alert on failed transfer operations and repeated `rapid_archive_deferred` logs. Regularly verify a transfer finishes inside the retention window. Hourly scheduling is a retry interval, not a completion SLA; choose retention to cover outages and the time needed to drain your backlog. There is no application cleanup worker. See [GCS lifecycle semantics](https://docs.cloud.google.com/storage/docs/lifecycle).

For incident preservation, copy selected data to a separate Standard bucket or a prefix outside both managed prefixes before it becomes eligible for deletion. Rapid does not support native object holds. Do not rename a referenced snapshot out of the live namespace.

## Install

Copy `values.yaml` and supply the image digest, Google service account, bucket names, namespaces, and zones:

```yaml
replicaCount: 3
zones:
  north-america-west: [us-west4-a, us-west4-b, us-west4-c]
cloudSql:
  instanceConnectionName: project:us-west4:actors-db
storage:
  authorityBucket: actor-ownership
  artifactBucket: actor-code
  archiveBucket: actor-archive
  rapid:
    ackZones: 2
    buckets:
      - {bucket: actor-rapid-a, zone: us-west4-a}
      - {bucket: actor-rapid-b, zone: us-west4-b}
```

`ackZones` is configurable from one to the number of buckets. Every configured bucket must occupy a different zone; startup verifies the actual GCS class and placement. At most seven Rapid buckets fit in the ten-rule credential access boundary alongside authority, archive, and code. Keep the storage configuration identical across controllers and immutable for existing ownership records.

```sh
helm lint charts/terse -f production-values.yaml
helm template actors charts/terse --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/terse --namespace terse-control --create-namespace -f production-values.yaml
```

With Cloud SQL, `postgres-url` points to `127.0.0.1:5432`; the native proxy sidecar starts before the control plane. Leave `cloudSql.instanceConnectionName` empty for a direct database connection. Configure DNS and certificates separately. `networkPolicy.dnsCidrs` allows host-networked or link-local DNS listeners.

## Writes and recovery

An actor writes the same immutable snapshot to Standard and all configured Rapid zones in parallel and acknowledges once Standard succeeds or `ackZones` Rapid publications succeed. The remaining operations may be canceled; recovery requires successful listings from more than `bucket_count - ackZones` Rapid zones so it intersects every possible acknowledged set. Recovery also reads Standard and chooses the highest consistent version. It fails when it cannot establish this read quorum or cannot read the permanent archive. A known immutable checkpoint can be read from any surviving copy.

Ownership and leases still use conditional writes to the authority bucket. Epoch-specific keys prevent late writes from changing a successor's state. The host checks its conservative lease deadline before and after persistence; a failed or canceled write permanently fences that host. Recovery waits for the previous lease to expire or be released. After an unclean stop, it republishes the selected tail to the configured quorum before claiming ownership, because an ambiguous write may have reached only one zone. Lease fencing assumes clock skew stays below its five-second safety margin. Clean shutdown records the final snapshot reference in ownership. No remote replica preparation, sealing, or baseline reseeding is needed.

An unsuccessful request may have persisted; callers must allow an unknown outcome. Standard upload concurrency is bounded to eight per host; saturation and errors defer to managed transfer. Customer tokens cover only that actor's ownership object, snapshot/upload prefixes, and immutable deployment code.

## Capacity and cutover

The default actor pool keeps 64 ready pods per image and region, with 0.5 CPU and 256 MiB per pod. `pool.fleetMaximum` bounds idle spares, not active actors. Exhausted actor pools use pod creation. Large snapshots increase memory, transfer, and GCS operation costs; each version is an independent object in every successful Rapid zone and eventually Standard.

This is a breaking storage change. Dedicated replica records and archives are not read by this runtime. Provision fresh storage and import any state you need before cutover; do not point the new runtime at existing replica-backed ownership and expect transparent recovery. Drain the old deployment while its controllers can still archive, then replace controllers and actor images together. Delete the old dedicated replica pods after stopping the old controllers; the new runtime has no replica pool or replica reconciler. Historical SQL migrations remain unchanged so their checksums remain valid; their unused replica tables can be removed after old-data handling is complete.

Source builds use the pinned Google Rust SDK's opt-in append API. Repository and Docker builds set `--cfg google_cloud_unstable_storage_bidi` in `.cargo/config.toml`. Builds outside this repository, including `cargo install`, must supply `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`.

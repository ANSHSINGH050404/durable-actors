# Terse on GKE Sandbox

The chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. Actor hosts and their dedicated Rust storage replicas run under Google-managed gVisor. The control plane prewarms pods, downloads immutable code artifacts from Standard GCS into actor pods, and tracks pod assignments in PostgreSQL. GCS conditional writes retain ownership and leases.

Every actor activation receives its own replica group. The default is **one actor pod plus three replica pods across three zones**, with three control-plane instances spread across zones. Replica processes never execute customer code or serve multiple actor activations. Every member of the current group must commit each write to its disk-backed SQLite WAL with `synchronous=FULL` before the actor acknowledges it. Healthy writes go directly from the actor to its replicas. When replica capacity is unavailable, writes persist synchronously to GCS through the control plane until a replacement group is ready.

## Prerequisites

- Regional GKE Standard with Workload Identity Federation, enforced NetworkPolicy, and both ordinary and COS Sandbox node pools across the configured zones. Enable Gateway API for the supplied public gateway. Keep the application control plane on ordinary nodes. A zonal GKE control plane remains a zone-level dependency even with nodes in other zones.
- Standard GCS authority, artifact, and archive buckets with uniform access and public access prevention. Archive placement must cover the promised failure domain. Do not apply blanket expiry rules to actor state or referenced code.
- PostgreSQL reachable by the control plane, preferably private Cloud SQL with regional HA. Bucket fallback and replica replacement require this database. The database user needs schema migration privileges. The chart does not run PostgreSQL in Kubernetes.
- A Google service account with `roles/storage.objectUser` on the three buckets and `storage.buckets.get` on the archive bucket. Grant `roles/cloudsql.client` when using the Cloud SQL proxy. Bind both Kubernetes identities, `<control-namespace>/<release>-terse` and `<sandbox-namespace>/replica`, with `roles/iam.workloadIdentityUser`.
- An existing Secret in the control namespace containing `postgres-url`, `api-key`, `replica-key` (at least 32 random bytes), and `jwt-signing-key` (base64 Ed25519 PKCS#8). Create `storage.replicas.credentialsSecret` in the sandbox namespace containing the same `replica-key`; do not label it as a customer Secret. Customer pods cannot read Kubernetes Secrets or access the metadata server.
- For HTTPS, a matching TLS Secret or a Google-managed Compute Engine SSL certificate. Configure exactly one of `gateway.tlsSecret` and `gateway.preSharedCert`.

Control-plane replicas coordinate deployment updates through PostgreSQL session locks. PostgreSQL also stores trace status and short-lived actor credentials; LISTEN/NOTIFY wakes observers across instances, with periodic polling after missed notifications. Kubernetes inventory is fetched once per reconciliation pass. GKE Workload Identity supplies control-plane and replica credentials. Actor pods receive downscoped tokens limited to their ownership object and unique deployment artifact prefix; the shared cache keys each token by issuer and its complete permission boundary, refreshes before expiry, and fences abandoned refresh claims. Hot actor persistence does not use this cache or PostgreSQL.

All control-plane instances must share the same database, buckets, credentials and pool configuration. The chart grants pod lifecycle/exec and Secret-read access in the sandbox namespace, plus read-only node metadata for placement and drain detection. Customer Secrets must have `terse.ai/customer-secret: "true"` and be named in deployment `secretRefs`.

Network policies allow customer pods to reach the control plane, authenticated replicas, DNS, and public internet endpoints. Only replica pods receive metadata-server access. Set `networkPolicy.dnsCidrs` for DNS listeners using host networking or link-local addresses, such as GKE NodeLocal DNS.

## Install

Copy `values.yaml`, then set the runtime image digest, bucket names, Google service account, namespaces, and zones. The chart creates application resources; cluster/node pools, Cloud SQL, Google IAM, buckets, and certificates are provisioned separately.

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
  durability: regional
  replicas:
    placements: [us-west4-a, us-west4-b, us-west4-c]
    idle: 192
    maxStarting: 32
    credentialsSecret: terse-replica-credentials
    resources:
      requests: {cpu: 50m, memory: 64Mi}
      limits: {memory: 512Mi}
```

For Cloud SQL, `postgres-url` uses `127.0.0.1:5432`; the proxy establishes the private authenticated connection. The native sidecar starts before the control plane. Leave `cloudSql.instanceConnectionName` empty when connecting directly to another PostgreSQL service.

```sh
helm lint charts/terse -f production-values.yaml
helm template actors charts/terse --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/terse --namespace terse-control --create-namespace -f production-values.yaml
kubectl -n terse-control get gateway actors-terse
```

For a global Google-managed certificate, set `gateway.preSharedCert`, clear `gateway.tlsSecret`, and set `gateway.addressName` to a reserved global address. Point the public hostname at that address. Installation does not change DNS. An external HTTPS/WebSocket load balancer can route to the control-plane Service with `gateway.enabled: false`.

## Assignment and writes

The controller records ready spares by pod UID and disk identity. It atomically reserves available members on distinct nodes, persists membership, and binds each disk permanently to the actor's ownership epoch. When configured for three regional copies, it can run with two copies in distinct surviving zones. If fewer than two failure domains are available, it retains bucket durability. Identical retries are idempotent; rebinding a disk or reviving a closed group is rejected. A resumed actor durably seeds its recovered baseline before activation returns.

The actor caches direct replica endpoints and sends ordered compressed state records in parallel. Each replica checks its disk identity, assignment, epoch, version and content before committing. After a failed or ambiguous write, the controller seals the old group and verifies its final archive before accepting the write through GCS. It seeds a fresh group from that verified checkpoint before publishing new endpoints. All members of each published group remain required; a failed member is never simply ignored. An unsuccessful request may still have persisted, so retrying callers must allow an unknown outcome.

Bucket mode writes archive data synchronously and records its checkpoint in PostgreSQL before returning. Group transitions are serialized by PostgreSQL session locks and recheck GCS ownership. Lost controller sessions cannot publish membership or checkpoints. When all requested zones regain warm capacity, the controller seals a reduced group so subsequent writes can restore the full replica count. Storage failure alone does not expire a live actor's lease. Actors lost with their compute zone recover after ownership fencing; connection retries and a recovery delay remain possible.

The placement list controls replica count. Zonal placement uses one zone; regional placement requires multiple zones in one region. Every requested zone needs schedulable Sandbox nodes in the Kubernetes cluster. GKE clusters do not span regions: this chart's dedicated-pod provisioner currently targets one cluster, so a cross-region deployment requires a multi-cluster provider and an archive strategy that preserves that failure guarantee. Do not configure remote regions in a single GKE cluster and expect them to be scheduled.

## Archival, recovery and retirement

Each replica stores compressed state records and a materialized head on a disk-backed `emptyDir`. The volume survives a container process restart but not pod deletion or node loss. Independent copies and GCS archives provide durability beyond that disk.

The designated uploader archives at 16 MiB or an oldest pending record age of 10 seconds, checked every 100 ms. These are batching thresholds, not payload limits or a guaranteed maximum lag. Batches begin with an independently decodable state checkpoint. Other replicas verify the GCS batch before pruning covered local records. If the uploader fails, followers try after 30 seconds. Upload errors retain local records and retry; disk exhaustion fails writes.

Recovery verifies that the old owner has released or expired, seals original surviving replicas, and selects consistent state. A new empty disk is never a witness. Sealing one original member prevents the old group from obtaining another all-copy acknowledgment. If no original copy survives and no verified final checkpoint exists, recovery blocks instead of silently rolling back to an older periodic archive.

The default actor idle timeout is 10 seconds, subject to active invocations and socket lifecycle. Replicas do not expire independently when writes become quiet. To retire a group, the controller persists a closing marker, seals it, flushes remaining records immediately, verifies the final GCS checkpoint, and persists the closed record before deleting pods by UID. Archive failures can keep replicas alive after the actor stops. Controller restarts retry this sequence from PostgreSQL.

Assigned replicas have a zero-disruption PDB and are marked unsafe for blind autoscaler eviction. A node marked unschedulable triggers a controlled transition through bucket persistence. Forced node/pod failures use the same sealing and archival checks. Never remove these protections to force a scale-down. Used replicas are deleted rather than rebound; fresh spares replenish the pool. Replica startup slots are divided across zones so an unavailable zone cannot consume the entire default startup budget.

GCS history remains after pod deletion. History is retained indefinitely; retention/compaction must preserve checkpoints referenced by dormant actors. The replica WAL is internal persistence machinery, separate from customer SQLite APIs.

## Code and capacity

Build a source image extending the published runtime image, containing application source and dependencies. Deploy its digest with `imageRef`, `workingDirectory`, and `actorEntrypoint`. A temporary Sandbox builder runs the compiler and publishes a generation-pinned, checksummed artifact. Rust downloads it in parallel into the assigned actor; there is no GCS filesystem mount.

```dockerfile
FROM RUNTIME_IMAGE_AT_SHA256_DIGEST
COPY --chown=10000:10000 . /customer
WORKDIR /customer
```

Defaults maintain 64 ready actor spares per runtime/region and 192 unassigned replicas. Actor pods request and are limited to 0.25 CPU and 256 MiB. Replica requests are 50m CPU and 64 MiB; CPU may burst, with a configurable 512 MiB memory limit. Large state increases memory and disk needs. These settings must be measured against the application's workload.

`pool.fleetMaximum` limits unassigned actor spares, not active actors. Each active actor consumes its own replica group in addition to the maintained reserve. Configure node autoscaling separately and account for all four pods, gVisor and system overhead. Resource overrides or exhausted actor pools use the cold pod-creation path; exhausted replica pools use bucket persistence while spares start. Python switches the prewarmed Bun process to its Python executor on assignment.


Size node capacity and autoscaling so the surviving zones can handle the workload after a zone loss.

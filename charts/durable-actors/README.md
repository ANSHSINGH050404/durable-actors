# Durable Actors on GCP

Run Durable Actors on GKE Sandbox with one Standard GCS bucket and PostgreSQL. Standard GCS is the default persistence mode. Enable GCS Rapid when you need faster state writes.

Follow the [self-hosting guide](../../docs/self-hosting.md) for cluster prerequisites, IAM, credentials, installation, and Rapid configuration.

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west4-a
storage:
  bucket: my-actor-state
serviceAccount:
  googleServiceAccount: actors@my-project.iam.gserviceaccount.com
ingress:
  enabled: true
  className: my-ingress
  tlsSecret: actors-tls
```

Use your installed ingress controller's class. The controller must support WebSockets; configure its connection timeouts through `ingress.annotations`. With ingress disabled, route your HTTPS endpoint to the chart's ClusterIP Service on port 7100.

Published chart packages include the matching immutable runtime digest. For source builds, supply `image.digest` for an image built from the same source revision.

## Defaults

- One controller handles API requests and WebSockets.
- Actor pods start on demand: `actors.warm: 0`.
- Each actor requests and is limited to 500 millicores and 256 MiB unless its deployment overrides resources.
- The controller requests 250 millicores and 512 MiB, with a 1 GiB memory limit. Measure and adjust `resources` for your workload.
- The sandbox namespace defaults to `<release-namespace>-<release-name>`; use one unique sandbox namespace per installation.
- gVisor, separate sandbox credentials, RBAC, and sandbox NetworkPolicy are included.

A single controller can interrupt service during replacement; clients must reconnect WebSockets. Increase `replicaCount` for controller redundancy and `actors.warm` to reduce cold starts. The warm count is per image/region and supports 0–256. It does not cap active actors. Node autoscaling and quotas must accommodate your active workload.

## Rapid writes

Add two precreated Rapid buckets in different supported zones. The Standard bucket continues to store ownership, code, and permanent archives.

```yaml
storage:
  bucket: my-actor-state
  mode: rapid
  rapid:
    buckets:
      - {bucket: my-actor-rapid-a, zone: us-west4-a}
      - {bucket: my-actor-rapid-b, zone: us-west4-b}
```

Both copies must durably flush before a Rapid write is acknowledged. Mode and bucket identities are fixed for existing actor ownership records; choose Rapid before creating actors in that installation. An existing installation needs a planned data migration, not a values-only mode change.

## Advanced settings

`credentialsSecret` defaults to `actors-credentials`; it must contain `postgres-url`, `api-key`, and `jwt-signing-key`. `nodeSelector` places controllers on ordinary nodes. `networkPolicy.dnsCidrs` permits specific host-networked or link-local DNS endpoints, for example a NodeLocal DNS `/32`. Add only the address your cluster uses.

See [values.yaml](values.yaml) for the full values reference and [values.schema.json](values.schema.json) for validation. No database, bucket, ingress controller, DNS record, or certificate is provisioned by this chart.

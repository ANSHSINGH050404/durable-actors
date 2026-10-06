# Environment variables

Configuration uses environment variables. Precedence is to use exported variables followed by reading a .env in the folder durable-actors executes in. There is also support for specifying an env file using the `--env-file` switch.

## Connecting to the control plane

| Variable                           | Default                                 | Description                                                                                                                 |
| ---------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                 | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                   | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |

## Diagnostics and runtime overrides

| Variable                   | Default                   | Description                                                                                      |
| -------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| `RUST_LOG`                 | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter.   |
| `DURABLE_ACTORS_TELEMETRY` | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`    | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.            |
| `DURABLE_ACTORS_CACHE_DIR` | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                             |

## Local development

| Variable                    | Default                            | Description                                                                                                           |
| --------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                                | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | `src/actors.ts` or `src/actors.py` | Actor source file, relative to the project. TypeScript wins when both exist.                                          |
| `DURABLE_ACTORS_PORT`       | `7100`                             | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors`        | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                            | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |

## WebSockets

| Variable                                | Default | Description                                                                                                                          |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS` | `32768` | Per-actor connection limit; 1–32,768, including pending connect handlers. Actual capacity depends on workload and gateway resources. |

## Capacity and placement

| Variable                              | Default | Description                                                                                                                                                                                                                                                                                 |
| ------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS` | `10000` | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket handlers reset the timer; running handlers defer eviction. Open WebSockets remain at the gateway and do not keep the sandbox alive. Automatic gateway replies do not reset actor idle time. |
| `DURABLE_ACTORS_HOST_STARTUP_MS`      | `10000` | Positive actor-host startup timeout in milliseconds.                                                                                                                                                                                                                                        |
| `DURABLE_ACTORS_SPARE_IDLE`           | `64`    | Ready actor sandboxes per image and configured compute region; must not exceed the fleet budget. Zero creates hosts on demand. Control-plane replicas must share pool settings. Customer secrets are installed at assignment.                                                               |
| `DURABLE_ACTORS_SPARE_FLEET_MAX`      | `256`   | Maximum unassigned spares across pools. Active actors do not count against this budget.                                                                                                                                                                                                     |
| `DURABLE_ACTORS_SPARE_MAX_STARTING`   | `32`    | Maximum simultaneous spare starts across control-plane replicas.                                                                                                                                                                                                                            |
| `DURABLE_ACTORS_SPARE_TTL_SECONDS`    | `600`   | Unassigned host lifetime; 30–3600 seconds.                                                                                                                                                                                                                                                  |
| `DURABLE_ACTORS_HOST_CPU_MILLIS`      | `500`   | Actor CPU request and cap; 100–64000 millicores.                                                                                                                                                                                                                                            |
| `DURABLE_ACTORS_HOST_MEMORY_MIB`      | `256`   | Actor memory request and cap; 128–262144 MiB.                                                                                                                                                                                                                                               |
| `DURABLE_ACTORS_REGION`               | Unset   | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                                                                                              |
| `DURABLE_ACTORS_HOME_REGION`          | Unset   | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                                                                                            |

## Authentication and callbacks

| Variable                                | Default                        | Description                                                                                                                                    |
| --------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                      | Signing key identifier.                                                                                                                        |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane` | Token issuer.                                                                                                                                  |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`     | Server authentication audience.                                                                                                                |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`        | Actor-call audience.                                                                                                                           |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                        | Positive maximum credential lifetime in seconds.                                                                                               |
| `DURABLE_ACTORS_SOCKET_EVENT_URL`       | Disabled                       | Incoming-message callback URL. Callbacks send the shared secret when configured and omit authorization otherwise; see [OpenAPI](openapi.yaml). |

## Kubernetes hosting

Use the [self-hosting guide](../self-hosting.md) and [Helm chart](../../charts/durable-actors/README.md) to run on GKE Sandbox with PostgreSQL and one Standard GCS bucket. One controller handles HTTP and WebSockets, and actor pods start on demand. The chart sets `DURABLE_ACTORS_SPARE_IDLE=0`; the runtime's standalone default above remains 64.

| Variable | Description |
| --- | --- |
| `DURABLE_ACTORS_PROCESS_ROLE` | `control_plane` for the server; the provider assigns actor/spare roles. |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND` | Listen address, `0.0.0.0:7100` in the chart. |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | Private Kubernetes Service origin reachable from sandboxes. |
| `DURABLE_ACTORS_GATEWAY_ROUTE` | Private HTTP origin of the gateway pod; the chart uses its pod IP. |
| `DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS` | Whether this process can own socket rooms; defaults to `true`. |
| `DURABLE_ACTORS_PUBLIC_URL` | Public HTTPS origin for client invocation and socket routing. |
| `DURABLE_ACTORS_POSTGRES_URL` | Registry, trace, and spare bookkeeping database; the user needs migration privileges. |
| `DURABLE_ACTORS_PERSISTENCE` | `standard` by default. Select `rapid` for two-zone Rapid append logs with Standard archives. |
| `DURABLE_ACTORS_BUCKET` | Standard GCS bucket for ownership and, in Standard mode, immutable state snapshots. |
| `DURABLE_ACTORS_ARTIFACT_BUCKET` | Immutable compiled actor code. Defaults to `DURABLE_ACTORS_BUCKET` in Standard mode; the chart uses that bucket in both modes. |
| `DURABLE_ACTORS_ARCHIVE_BUCKET` | Required only in Rapid mode: permanent Standard GCS manifests, checkpoints, and archived log segments. The chart uses `storage.bucket`. |
| `DURABLE_ACTORS_RAPID_BUCKETS` | Required only in Rapid mode: JSON array of exactly two `{ "bucket": "name", "zone": "us-west4-a" }` placements in distinct zones. Both durable flushes are required for acknowledgment. |
| `DURABLE_ACTORS_ARCHIVE_BATCH_BYTES` | Rapid archive batch target, default `16777216` bytes. |
| `DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS` | Rapid archive batch interval, default `10000` milliseconds. |
| `DURABLE_ACTORS_GKE_NAMESPACE` | Dedicated sandbox namespace; the chart defaults to `<release-namespace>-<release-name>`. |
| `DURABLE_ACTORS_GKE_ZONE` | Actor placement zone, for example `us-west4-a`; the runtime infers the canonical compute region. The chart sets this from `placement.zone`. |
| `DURABLE_ACTORS_GKE_ZONES` | Advanced alternative to `GKE_ZONE`: JSON map from canonical compute region to eligible Google zones, such as `{"north-america-west":["us-west4-a","us-west4-b"]}`. A single zone string per region is also accepted. Configure only one of the two placement variables. |
| `DURABLE_ACTORS_RUNTIME_IMAGE` | Shared runtime OCI image pinned by SHA-256 digest. |
| `DURABLE_ACTORS_GOOGLE_SERVICE_ACCOUNT` | Workload Identity service account used as the storage-token cache issuer identity. |
| `DURABLE_ACTORS_JWT_SIGNING_KEY` | Shared base64 Ed25519 PKCS#8 key; stable across restarts. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Optional ADC file; use Workload Identity on GKE. |

Standard mode rejects Rapid bucket and archive settings. Storage mode and bucket identities are fixed for existing actor ownership records; changing them requires a planned data migration. See [Rapid configuration](../self-hosting.md#speed-up-writes-with-gcs-rapid) for bucket creation, IAM, and chart values.

# Environment variables

The CLI reads `.env` in the current directory. Exported variables take precedence. Application backends must load their own environment; containers can use `--env-file`.

## Application connection

Used by backend clients, `generate --remote`, and `observe`. Local startup also uses the project ID and API key.

| Variable                           | Default                                             | Meaning                                                                                                                     |
| ---------------------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely             | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                             | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                               | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |

Local CLI commands and backend clients need no connection settings with the defaults. Connections to localhost (`localhost`, `127.0.0.1`, and `[::1]`) default to project `local`. Remote connections require an explicit project ID. If you override the project or port, use matching settings in your backend.

Authentication is disabled when `DURABLE_ACTORS_SECRET` is unset on the server, locally or remotely. Set it on both the server and your backend to test or enable authentication. Clients omit the authorization header when no secret is configured. Leave the variable unset to disable authentication; empty values and surrounding whitespace are invalid on the server.

## Local development

Used by `dev`.

| Variable                    | Default                     | Meaning                                                                                                               |
| --------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                         | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | `src/actors.ts`             | Actor source file, relative to the project.                                                                           |
| `DURABLE_ACTORS_PORT`       | `7100`                      | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors` | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                     | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |

## Kubernetes hosting

Use the [Helm chart](../../charts/terse/README.md) for production on GKE Sandbox. It colocates the control plane and HTTPS/WebSocket gateway with a prewarmed actor pool. The chart sets these runtime variables:

| Variable | Meaning |
| --- | --- |
| `DURABLE_ACTORS_PROCESS_ROLE` | `control_plane` for the server; the provider assigns actor/spare roles. |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND` | Listen address, `0.0.0.0:7100` in the chart. |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | Private Kubernetes Service origin reachable from sandboxes. |
| `DURABLE_ACTORS_PUBLIC_URL` | Public HTTPS origin for client invocation and socket routing. |
| `DURABLE_ACTORS_POSTGRES_URL` | Registry, trace and spare bookkeeping database; migrations required. |
| `DURABLE_ACTORS_BUCKET` | Standard GCS authority bucket for CAS ownership and leases. |
| `DURABLE_ACTORS_ARTIFACT_BUCKET` | Immutable compiled customer code. |
| `DURABLE_ACTORS_ARCHIVE_BUCKET` | Permanent Standard GCS bucket for immutable snapshots. Writes race the Rapid quorum; archival continues asynchronously when Rapid wins. |
| `DURABLE_ACTORS_RAPID_BUCKETS` | JSON array of `{ "bucket": "name", "zone": "us-west4-a" }` placements in distinct Rapid zones. Required in production. |
| `DURABLE_ACTORS_RAPID_ACK_ZONES` | Number of distinct Rapid zones required to acknowledge before Standard succeeds; default `2`. |
| `DURABLE_ACTORS_GKE_NAMESPACE` | Dedicated sandbox namespace, default `terse-sandboxes`. |
| `DURABLE_ACTORS_GKE_ZONES` | JSON map from canonical compute region to a nonempty list of Google zones, for example `{"north-america-west":["us-west4-a","us-west4-b","us-west4-c"]}`. A single zone string is also accepted. Actor placement spreads across the eligible zones. |
| `DURABLE_ACTORS_RUNTIME_IMAGE` | Shared runtime OCI image pinned by SHA-256 digest. |
| `DURABLE_ACTORS_JWT_SIGNING_KEY` | Shared base64 Ed25519 PKCS#8 key; stable across restarts. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Optional ADC file; use Workload Identity on GKE. |

## Advanced settings

### Capacity and placement

| Variable                                    | Default              | Meaning                                                                                                                                                                                                         |
| ------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`      | `10000`              | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket messages reset the timer; running handlers defer eviction. Open WebSockets retain the host and connections, but not the actor instance. Without open sockets, an idle host shuts down. |
| `DURABLE_ACTORS_HOST_STARTUP_MS`            | `10000`              | Positive actor-host startup timeout in milliseconds.                                                                                                                                                            |
| `DURABLE_ACTORS_SPARE_IDLE`                 | `64`                 | Ready actor sandboxes per image and configured compute region; must not exceed the fleet budget. Zero creates hosts on demand. Control-plane replicas must share pool settings. Customer secrets are installed at assignment. |
| `DURABLE_ACTORS_SPARE_FLEET_MAX` | `256` | Maximum unassigned spares across pools. Active actors do not count against this budget. |
| `DURABLE_ACTORS_SPARE_MAX_STARTING` | `32` | Maximum simultaneous spare starts across control-plane replicas. |
| `DURABLE_ACTORS_SPARE_TTL_SECONDS`          | `600`                | Unassigned host lifetime; 30–3600 seconds.                                                                                                                                                                      |
| `DURABLE_ACTORS_HOST_CPU_MILLIS`            | `500`                | Actor CPU request and cap; 100–64000 millicores.                                                                                                                                                                |
| `DURABLE_ACTORS_HOST_MEMORY_MIB`            | `256`                | Actor memory request and cap; 128–262144 MiB.                                                                                                                                                                   |
| `DURABLE_ACTORS_REGION`                     | Unset                | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                 |
| `DURABLE_ACTORS_HOME_REGION`                | Unset                | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                |

### Per-actor sandbox overrides

```ts
import { Actor, Sandbox } from "durable-actors"

@Sandbox({
    cpu: 2,
    memoryMiB: 2048,
    regions: ["canada"],
    idleTimeoutMs: 60_000,
})
export class CustomerAgent extends Actor {}
```

| Option | Meaning | Default when omitted |
| --- | --- | --- |
| `cpu` | CPU request and cap in cores; 0.1–64 in increments of 0.001. | `DURABLE_ACTORS_HOST_CPU_MILLIS` divided by 1000; normally 0.5. |
| `memoryMiB` | Memory request and cap; integer from 128–262144 MiB. | `DURABLE_ACTORS_HOST_MEMORY_MIB`; normally 256. |
| `regions` | Nonempty list of unique allowed compute regions. Order is not a preference. | Existing placement and server defaults. |
| `idleTimeoutMs` | Inactivity before eviction; integer from 1–86400000 ms. | `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`; normally 10000 (10 seconds). |

Supported regions: `canada`, `north-america-east`, `north-america-central`, `north-america-south`, `north-america-west`, `europe-west`, `asia-southeast`.

### Authentication and callbacks

| Variable                                | Default                              | Meaning                                                                                |
| --------------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                            | Signing key identifier.                                                                |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane`       | Token issuer.                                                                          |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`           | Server authentication audience.                                                        |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`              | Actor-call audience.                                                                   |
| `DURABLE_ACTORS_SOCKET_JWT_AUDIENCE`    | `durable-actors-authority:websocket` | Socket audience for manual hosts; must match the authority audience plus `:websocket`. |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                              | Positive maximum credential lifetime in seconds.                                       |
| `DURABLE_ACTORS_SOCKET_EVENT_URL`       | Disabled                             | Incoming-message callback URL. Callbacks send the shared secret when configured and omit authorization otherwise; see [OpenAPI](openapi.yaml).                            |

### Diagnostics and runtime overrides

| Variable                         | Default                   | Meaning                                                                                        |
| -------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------- |
| `RUST_LOG`                       | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter. |
| `DURABLE_ACTORS_TELEMETRY`       | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`          | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.          |
| `DURABLE_ACTORS_CACHE_DIR`       | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                           |

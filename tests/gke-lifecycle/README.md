# Live GKE lifecycle benchmark

This opt-in runner measures the SDK from the machine running Node to a deployed public endpoint. It requires a built SDK, Bun actor support in the deployed release image, `kubectl`, `gcloud`, Python 3, and an isolated deployment of `charts/terse` with two Rapid buckets and a Standard archive. The cloud credentials and kubeconfig must address that deployment.

Deploy this actor through the normal deployment API before running:

```ts
import { Actor, Persisted } from "durable-actors"
export class Counter extends Actor {
    @Persisted value = 0
    async increment() { return ++this.value }
    async read() { return this.value }
}
```

Set `TERSE_LIFECYCLE_DIRECTORY` to an absolute output directory containing `resources.json`:

```json
{
  "project": "google-cloud-project",
  "release": "bench",
  "namespace": "benchmark-control",
  "sandbox_namespace": "benchmark-sandboxes",
  "buckets": {"owner": "benchmark-authority-bucket", "archive": "benchmark-archive-bucket"}
}
```

Set `TERSE_LIFECYCLE_SETTINGS` to an absolute path to a private JSON file, readable only by its owner:

```json
{
  "url": "https://benchmark.example.com",
  "project_id": "benchmark-project",
  "api_key": "benchmark-api-key",
  "kubeconfig": "/absolute/path/to/benchmark-kubeconfig"
}
```

The test expects a pod named `postgres` in the control namespace, with local `psql -U postgres` access to the registry database. It requires permission to inspect pod status/logs, execute read-only database queries, and read the authority and archive buckets. No credentials are included in the results.

```sh
export TERSE_LIFECYCLE_DIRECTORY=/absolute/path/to/benchmark-output
export TERSE_LIFECYCLE_SETTINGS=/absolute/path/to/private-settings.json
# For a benchmark certificate, trust its CA explicitly; keep verification enabled.
export NODE_EXTRA_CA_CERTS=/absolute/path/to/benchmark-ca.pem
node tests/gke-lifecycle/run.mjs 20 2
```

Arguments are measured activation samples per case and warmups per case. Defaults are 20 and 2. The runner emits five hot write/read pairs after each initial activation. Results go into a timestamped subdirectory. Warmup rows are marked and must be excluded from summaries.

Cold reads/writes use separate new actors; the generic gVisor process must already be ready. Resume uses the same actor and client after idle shutdown and verified GCS archival, requiring a different host and higher ownership epoch. Incorrect state, missing prewarmed-pod evidence, failed archival, or request errors fail the run. Pod/spare and archive checks happen outside the request timer.

Use the default 10-second host idle timeout. The runner waits 13 seconds after the last initial invocation before checking all old hosts. It does not provision or delete infrastructure. Collect infrastructure placement evidence and remove the isolated resources after the run.

## Regional client and fault checks

`client.mjs` runs inside a client pod using the SDK packaged in the production image. Set `BENCH_SDK_CLIENT` to `/opt/durable-actors/sdk/dist/client/remoteClient.js`, `BENCH_INTERNAL_URL` to the control-plane Service origin, `BENCH_PROJECT_ID`, and `BENCH_API_KEY` through a Secret. `BENCH_MODE` labels the output. The client preserves the returned target's path while routing through that internal origin. It checks 100 hot writes and reads after five warmups, then a clean resume. This excludes laptop/tunnel latency and the separate application `/actors/access` authorization service.

Using the same private settings as the lifecycle runner:

```sh
node tests/gke-lifecycle/crash.mjs
node tests/gke-lifecycle/checkpoint.mjs
```

The crash test locates only its newly created actor's pod, sends SIGKILL to PID 1, verifies ownership remained unsealed, waits for lease expiration, and checks that a new host recovers all five acknowledged writes before accepting write six. Run it only against an isolated benchmark deployment. It writes `crash-result.json` and the old host's log.

The checkpoint test keeps one actor active for over two minutes. It verifies that archival happened while the actor remained active, that writes opened a new segment, and that clean resume preserves all 131 writes. It writes `checkpoint-result.json`. Neither runner removes test objects or infrastructure; clean up their exact actor prefixes after collecting results.

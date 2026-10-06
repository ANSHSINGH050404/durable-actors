import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const chart = new URL("../", import.meta.url).pathname
const helm = process.env.HELM ?? "helm"
function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "actors-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "actors", chart, "--namespace", "actors", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally {
        rmSync(directory, { recursive: true })
    }
}

function manifests(overrides) {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    return result.stdout
}

test("starts one controller and creates actor pods on demand using one Standard bucket", () => {
    const output = manifests()
    assert.match(output, /replicas: 1\b/)
    assert.match(output, /DURABLE_ACTORS_PERSISTENCE, value: "standard"/)
    for (const name of ["BUCKET", "ARTIFACT_BUCKET"]) assert.match(output, new RegExp(`DURABLE_ACTORS_${name}, value: "test-state"`))
    assert.match(output, /DURABLE_ACTORS_SPARE_IDLE, value: "0"/)
    assert.match(output, /kind: Service\b/)
    assert.equal(output.match(/kind: Deployment\n/g).length, 1)
})

test("isolates sandboxes in a release-specific namespace with constrained credentials and network access", () => {
    const output = manifests()
    assert.match(output, /namespace: actors-actors/)
    assert.match(output, /automountServiceAccountToken: false/)
    assert.match(output, /kind: RoleBinding/)
    assert.match(output, /kind: NetworkPolicy/)
    assert.match(output, /169\.254\.0\.0\/16/)
    assert.match(output, /runAsNonRoot: true/)
})

test("enables Rapid with two distinct zones and the same Standard bucket for archives", () => {
    const output = manifests({ storage: { mode: "rapid", rapid: { buckets: [
        { bucket: "rapid-a", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-b" }
    ] } } })
    assert.match(output, /DURABLE_ACTORS_PERSISTENCE, value: "rapid"/)
    assert.match(output, /DURABLE_ACTORS_ARCHIVE_BUCKET, value: "test-state"/)
    for (const value of ["rapid-a", "rapid-b", "us-west4-a", "us-west4-b"]) assert.ok(output.includes(value))
})

test("routes HTTPS and WebSockets to the same service through the selected ingress controller", () => {
    const output = manifests({ ingress: { enabled: true, className: "example", tlsSecret: "actors-tls", annotations: { "example.com/websocket-timeout": "3600" } } })
    assert.match(output, /apiVersion: networking.k8s.io\/v1\nkind: Ingress/)
    assert.match(output, /ingressClassName: "example"/)
    assert.match(output, /secretName: "actors-tls"/)
    assert.match(output, /host: "actors.example.com"/)
    assert.match(output, /example.com\/websocket-timeout: "3600"/)
})

test("allows extra controllers and a small warm pool", () => {
    const output = manifests({ replicaCount: 2, actors: { warm: 2 } })
    assert.match(output, /replicas: 2\b/)
    assert.match(output, /DURABLE_ACTORS_SPARE_IDLE, value: "2"/)
    assert.match(output, /kind: PodDisruptionBudget/)
})

for (const [name, overrides] of [
    ["unknown storage mode", { storage: { mode: "standrad" } }],
    ["missing Rapid placements", { storage: { mode: "rapid" } }],
    ["duplicate Rapid zones", { storage: { mode: "rapid", rapid: { buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-a" }] } } }],
    ["shared Standard and Rapid bucket", { storage: { mode: "rapid", rapid: { buckets: [{ bucket: "test-state", zone: "us-west4-a" }, { bucket: "rapid-b", zone: "us-west4-b" }] } } }],
    ["Rapid settings in Standard mode", { storage: { rapid: { buckets: [{ bucket: "rapid-a", zone: "us-west4-a" }] } } }],
    ["mutable runtime image", { image: { digest: "latest" } }],
    ["insecure public URL", { publicUrl: "http://actors.example.com" }],
    ["shared trust namespace", { sandboxNamespace: "actors" }],
    ["missing ingress certificate", { ingress: { enabled: true } }],
    ["warm pool above fleet limit", { actors: { warm: 300 } }]
]) test(`rejects ${name}`, () => assert.notEqual(render(overrides).status, 0))

import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const chart = new URL("../", import.meta.url).pathname
const helm = process.env.HELM ?? "helm"
function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "terse-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "test", chart, "--namespace", "terse-control", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally {
        rmSync(directory, { recursive: true })
    }
}

for (const [name, overrides, cpuMillis] of [
    ["default", {}, 500],
    ["configured", { pool: { cpuMillis: 750 } }, 750]
]) {
    test(`renders ${name} sandbox CPU allocation`, () => {
        const result = render(overrides)
        assert.equal(result.status, 0, result.stderr)
        const cpu = result.stdout.split("\n").find(line => line.includes("DURABLE_ACTORS_HOST_CPU_MILLIS"))
        assert.match(cpu, new RegExp(`value: "${cpuMillis}"`))
    })
}

for (const [durability, placements] of [
    ["zonal", ["us-west4-a", "us-west4-a", "us-west4-a"]],
    ["regional", ["us-west4-a", "us-west4-b", "us-west4-c"]],
    ["multi_region", ["us-west4-a", "us-east4-a"]]
]) {
    test(`renders ${durability} dedicated replica policy and HTTPS ingress`, () => {
        const result = render({ storage: { durability, replicas: { placements } } })
        assert.equal(result.status, 0, result.stderr)
        assert.match(result.stdout, /kind: Gateway/)
        assert.match(result.stdout, /DURABLE_ACTORS_REPLICA_PLACEMENTS/)
        assert.match(result.stdout, /DURABLE_ACTORS_REPLICA_IDLE/)
        assert.match(result.stdout, /terse.ai\/assigned: "true"/)
        assert.match(result.stdout, /automountServiceAccountToken: false/)
        assert.match(result.stdout, /port: 7200/)
    })
}

test("production defaults spread compute and storage across three zones", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, /DURABLE_ACTORS_DURABILITY, value: "regional"/)
    const placements = result.stdout.split("\n").find(line => line.includes("DURABLE_ACTORS_REPLICA_PLACEMENTS"))
    const zones = result.stdout.split("\n").find(line => line.includes("DURABLE_ACTORS_GKE_ZONES"))
    for (const zone of ["us-west4-a", "us-west4-b", "us-west4-c"]) {
        assert.ok(placements.includes(zone))
        assert.ok(zones.includes(zone))
    }
    assert.match(result.stdout, /replicas: 3/)
    assert.match(result.stdout, /whenUnsatisfiable: DoNotSchedule/)
    assert.match(result.stdout, /minDomains: 2/)
})
for (const [name, override] of [
    ["single regional control plane", { replicaCount: 1 }],
    ["single regional compute zone", { zones: { "north-america-west": "us-west4-a" } }],
    ["regional copies in one zone", { storage: { durability: "regional", replicas: { placements: ["us-west4-a", "us-west4-a"] } } }],
    ["multi-region copies in one region", { storage: { durability: "multi_region" } }],
    ["empty replica set", { storage: { replicas: { placements: [] } } }],
    ["unknown policy", { storage: { durability: "best_effort" } }],
    ["mutable image", { image: { digest: "latest" } }],
    ["shared trust namespace", { sandboxNamespace: "terse-control" }]
])
    test(`rejects ${name}`, () => assert.notEqual(render(override).status, 0))

test("dedicated replicas have storage identity access while customer pods stay isolated", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, /name: replica\n  namespace: terse-sandboxes/)
    assert.match(result.stdout, /169\.254\.169\.254\/32/)
    assert.match(result.stdout, /maxUnavailable: 0/)
    assert.match(result.stdout, /resources: \[nodes\]/)
})

test("sandboxes can resolve DNS through kube-dns and GKE NodeLocal DNS pods", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    const policy = result.stdout.split("---").find(document => document.includes("kind: NetworkPolicy") && document.includes("namespace: terse-sandboxes"))
    assert.ok(policy)
    const dnsRule = policy.slice(policy.indexOf("kubernetes.io/metadata.name: kube-system"))
    const destinations = dnsRule.slice(0, dnsRule.indexOf("ports:"))
    assert.match(destinations, /kube-dns/)
    assert.match(destinations, /node-local-dns/)
    assert.match(dnsRule, /protocol: UDP, port: 53/)
    assert.match(dnsRule, /protocol: TCP, port: 53/)
})

test("uses a Google-managed certificate on the HTTPS gateway", () => {
    const result = render({ gateway: { tlsSecret: "", preSharedCert: "actors-production" } })
    assert.equal(result.status, 0, result.stderr)
    const gateway = result.stdout.split("---").find(document => document.includes("kind: Gateway"))
    assert.match(gateway, /networking\.gke\.io\/pre-shared-certs: "actors-production"/)
    assert.doesNotMatch(gateway, /certificateRefs/)
})

for (const [name, gateway] of [
    ["missing TLS certificate", { tlsSecret: "", preSharedCert: "" }],
    ["ambiguous TLS certificates", { tlsSecret: "tls", preSharedCert: "managed" }]
])
    test(`rejects ${name}`, () => assert.notEqual(render({ gateway }).status, 0))

test("Cloud SQL connects through a private local proxy that starts before the control plane", () => {
    const result = render({ cloudSql: { instanceConnectionName: "project:us-west4:actors-db" } })
    assert.equal(result.status, 0, result.stderr)
    const deployment = result.stdout.split("---").find(document => document.includes("kind: Deployment"))
    assert.match(deployment, /initContainers:[\s\S]*name: cloud-sql-proxy[\s\S]*restartPolicy: Always/)
    assert.match(deployment, /--address=127\.0\.0\.1/)
    assert.match(deployment, /--private-ip/)
    assert.match(deployment, /project:us-west4:actors-db/)
    assert.match(deployment, /path: \/startup/)
    assert.match(deployment, /key: postgres-url/)
})

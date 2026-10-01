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

test("renders two Rapid zones and the Standard archive", () => {
    const result = render()
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, /DURABLE_ACTORS_ARCHIVE_BUCKET, value: "test-archive"/)
    const buckets = result.stdout.split("\n").find(line => line.includes("DURABLE_ACTORS_RAPID_BUCKETS"))
    for (const value of ["test-rapid-a", "test-rapid-b", "us-west4-a", "us-west4-b"]) assert.ok(buckets.includes(value))
    const deployment = result.stdout.split("---").find(document => document.includes("kind: Deployment\n"))
    assert.match(deployment, /replicas: 2\b/)
    assert.match(result.stdout, /kind: Gateway/)
    assert.match(result.stdout, /whenUnsatisfiable: DoNotSchedule/)
})

for (const [name, overrides, bytes, interval] of [
    ["default", {}, 16777216, 10000],
    ["configured", { storage: { rapid: { archiveBatchBytes: 2097152, archiveBatchIntervalMs: 250 } } }, 2097152, 250]
]) test(`renders ${name} archive batch triggers`, () => {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stdout, new RegExp(`DURABLE_ACTORS_ARCHIVE_BATCH_BYTES, value: "${bytes}"`))
    assert.match(result.stdout, new RegExp(`DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS, value: "${interval}"`))
})

for (const field of ["archiveBatchBytes", "archiveBatchIntervalMs"])
    test(`rejects nonpositive ${field}`, () => assert.notEqual(render({ storage: { rapid: { [field]: 0 } } }).status, 0))

for (const [name, override] of [
    ["single control plane", { replicaCount: 1 }],
    ["single compute zone", { zones: { "north-america-west": "us-west4-a" } }],
    ["duplicate Rapid zones", { storage: { rapid: { buckets: [{ bucket: "rapid-one", zone: "us-west4-a" }, { bucket: "rapid-two", zone: "us-west4-a" }] } } }],
    ["single Rapid zone", { storage: { rapid: { buckets: [{ bucket: "rapid-one", zone: "us-west4-a" }] } } }],
    ["empty Rapid set", { storage: { rapid: { buckets: [] } } }],
    ["mutable image", { image: { digest: "latest" } }],
    ["shared trust namespace", { sandboxNamespace: "terse-control" }]
]) test(`rejects ${name}`, () => assert.notEqual(render(override).status, 0))

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

test("node capacity buffering can be disabled", () => {
    const result = render({ capacityBuffer: { enabled: false } })
    assert.equal(result.status, 0, result.stderr)
    assert.doesNotMatch(result.stdout, /kind: (CapacityBuffer|PodTemplate)/)
})

for (const [name, overrides, replicas, cpu, memory, namespace, zones] of [
    ["default", {}, 32, "500m", "256Mi", "terse-sandboxes", ["us-west4-a", "us-west4-b", "us-west4-c"]],
    ["configured", {
        capacityBuffer: { replicas: 16 },
        pool: { cpuMillis: 750, memoryMiB: 512 },
        sandboxNamespace: "custom-sandboxes",
        region: "north-america-east",
        zones: { "north-america-east": ["us-east4-a", "us-east4-b"] }
    }, 16, "750m", "512Mi", "custom-sandboxes", ["us-east4-a", "us-east4-b"]]
]) test(`reserves ${name} capacity for sandbox-shaped pods in the configured region`, () => {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    const documents = result.stdout.split("---")
    const buffer = documents.find(document => document.includes("kind: CapacityBuffer\n"))
    const template = documents.find(document => document.includes("kind: PodTemplate\n"))
    assert.ok(buffer, "capacity buffer is rendered")
    assert.ok(template, "buffer pod template is rendered")
    const templateName = template.match(/metadata:\n\s+name: (\S+)/)[1]
    assert.match(buffer, /apiVersion: autoscaling\.x-k8s\.io\/v1beta1/)
    assert.match(buffer, new RegExp(`podTemplateRef:\\n\\s+name: ${templateName}`))
    assert.match(buffer, new RegExp(`replicas: ${replicas}\\b`))
    assert.match(buffer, /provisioningStrategy: buffer\.x-k8s\.io\/active-capacity/)
    for (const document of [buffer, template]) assert.match(document, new RegExp(`namespace: ${namespace}\\b`))
    assert.match(template, /runtimeClassName: gvisor/)
    assert.match(template, /nodeSelector:\n\s+sandbox\.gke\.io\/runtime: gvisor/)
    assert.match(template, /key: sandbox\.gke\.io\/runtime\n\s+operator: Equal\n\s+value: gvisor\n\s+effect: NoSchedule/)
    assert.match(template, /requiredDuringSchedulingIgnoredDuringExecution:/)
    assert.match(template, /key: topology\.kubernetes\.io\/zone\n\s+operator: In/)
    for (const zone of zones) assert.ok(template.includes(zone))
    if (name === "configured") assert.doesNotMatch(template, /us-west4/)
    assert.match(template, new RegExp(`requests:\\n\\s+cpu: "${cpu}"\\n\\s+memory: "${memory}"`))
    assert.match(template, /terminationGracePeriodSeconds: 0/)
    assert.match(template, /automountServiceAccountToken: false/)
    assert.doesNotMatch(template, /secretKeyRef|DURABLE_ACTORS_|terse\.ai\/purpose: actor/)
})

for (const [name, capacityBuffer] of [
    ["zero slots", { enabled: true, replicas: 0 }],
    ["negative slots", { enabled: true, replicas: -1 }],
    ["fractional slots", { enabled: true, replicas: 1.5 }],
    ["non-boolean enable flag", { enabled: "true" }]
]) test(`rejects capacity buffer with ${name}`, () => assert.notEqual(render({ capacityBuffer }).status, 0))

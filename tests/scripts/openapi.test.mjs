import SwaggerParser from "@apidevtools/swagger-parser"
import Ajv from "ajv/dist/2020.js"
import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

const specPath = fileURLToPath(new URL("../../docs/reference/openapi.yaml", import.meta.url))

test("OpenAPI validates and covers the public HTTP routes", async () => {
    const spec = await SwaggerParser.validate(specPath)
    const paths = new Set()
    for (const file of ["control_plane/public_api.rs", "control_plane/contract_api.rs", "control_plane/inspection.rs", "sockets/browser.rs", "host/http.rs"]) {
        const source = await readFile(new URL(`../../src/${file}`, import.meta.url), "utf8")
        for (const match of source.matchAll(/\.route\(\s*"([^"]+)"/gu)) paths.add(match[1])
    }
    assert.deepEqual(Object.keys(spec.paths).sort(), [...paths].sort())
    const operations = Object.values(spec.paths).flatMap(path => ["get", "post", "put", "delete"].filter(method => path[method]).map(method => path[method]))
    assert.equal(new Set(operations.map(operation => operation.operationId)).size, operations.length)
    assert.ok(operations.every(operation => operation.operationId))
})

test("actor discovery documents an explicit project for every operation", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const discoveryPaths = Object.entries(spec.paths).filter(([path]) => /\/actors\/\{actor_name\}\/\{actor_id\}\/find-(actor|websocket)$/u.test(path))
    assert.ok(discoveryPaths.length > 0)
    for (const [path, operation] of discoveryPaths) {
        const project = operation.parameters.find(parameter => parameter.name === "project_id")
        assert.ok(project, `${path} requires an explicit project`)
        assert.equal(project.in, "path")
        assert.equal(project.required, true)
        assert.ok(path.includes("/projects/{project_id}/"))
    }
})

test("OpenAPI examples satisfy their request and response schemas", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const ajv = new Ajv({ strict: false, validateFormats: false })
    for (const paths of [spec.paths, spec.webhooks]) {
        for (const path of Object.values(paths)) {
            for (const method of ["get", "post", "put", "delete"]) {
                const operation = path[method]
                if (!operation) continue
                for (const body of [operation.requestBody, ...Object.values(operation.responses)]) {
                    for (const media of Object.values(body?.content ?? {})) {
                        for (const example of Object.values(media.examples ?? {})) {
                            assert.ok(ajv.validate(media.schema, example.value), `${operation.operationId}: ${ajv.errorsText()}`)
                        }
                    }
                }
            }
        }
    }
})

test("actor discovery accepts an optional placement region", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const validate = new Ajv({ strict: false }).compile(spec.components.schemas.FindActorRequest)
    for (const request of [{}, { homeRegion: null }, { homeRegion: "north-america-west" }]) assert.ok(validate(request), JSON.stringify(request))
    for (const request of [{ metadata: {} }, { homeRegion: 42 }, { homeRegion: "" }, { homeRegion: "bad/region" }, { homeRegion: "Uppercase" }, { homeRegion: "a".repeat(65) }])
        assert.equal(validate(request), false, JSON.stringify(request))
})

test("project session issuance requires an identity and absolute deadline", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const validate = new Ajv({ strict: false, validateFormats: false }).compile(spec.components.schemas.IssueActorSession)
    const request = { subject: "credential-fingerprint", expiresAtMs: 1_790_000_060_000 }
    assert.ok(validate(request), JSON.stringify(validate.errors))
    for (const invalid of [{ subject: request.subject }, { expiresAtMs: request.expiresAtMs }, { ...request, subject: "" }, { ...request, unexpected: true }])
        assert.equal(validate(invalid), false, JSON.stringify(invalid))
})

test("deployment schemas describe both hosted and local registration and the complete read response", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const deployment = spec.paths["/v1/projects/{project_id}/deployment"]
    const ajv = new Ajv({ strict: false })
    const register = ajv.compile(deployment.put.requestBody.content["application/json"].schema)
    const read = ajv.compile(deployment.get.responses["200"].content["application/json"].schema)
    for (const imageRef of ["im-source", "local"]) {
        const value = { imageRef, workingDirectory: "/customer", actorEntrypoint: null, secretRefs: [] }
        assert.ok(register({ imageRef, workingDirectory: "/customer" }), ajv.errorsText(register.errors))
        assert.ok(read(value), ajv.errorsText(read.errors))
        for (const field of Object.keys(value)) {
            const incomplete = { ...value }
            delete incomplete[field]
            assert.equal(read(incomplete), false, `deployment response requires ${field}`)
        }
    }
})

test("websocket discovery requires metadata and enforces grant limits", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const validate = new Ajv({ strict: false }).compile(spec.components.schemas.FindWebSocketRequest)
    for (const request of [{ metadata: null }, { metadata: {}, homeRegion: null }, { metadata: {}, authorizationLifetimeMs: 1000 }, { metadata: {}, authorizationLifetimeMs: 86400000 }])
        assert.ok(validate(request), JSON.stringify(request))
    for (const request of [{}, { metadata: {}, unknown: true }, { metadata: {}, authorizationLifetimeMs: 999 }, { metadata: {}, authorizationLifetimeMs: 86400001 }])
        assert.equal(validate(request), false, JSON.stringify(request))
})

test("OpenAPI accepts the compiler's public contract fixture", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const contract = JSON.parse(await readFile(new URL("../../sdk/tests/fixtures/public-contract.json", import.meta.url), "utf8"))
    const ajv = new Ajv({ strict: false })
    assert.ok(ajv.validate(spec.components.schemas.PublicActorContract, contract), ajv.errorsText())
})

test("invocation rejections require a recognized pre-execution reason", async () => {
    const spec = await SwaggerParser.dereference(specPath)
    const validate = new Ajv({ strict: false }).compile(spec.components.schemas.ActorInvocationReply)
    for (const reason of ["stale_owner", "host_unavailable", "upstream_not_reached"]) assert.ok(validate({ type: "not_executed", reason }), JSON.stringify(validate.errors))
    for (const reason of [undefined, "", "unknown", 42]) assert.equal(validate({ type: "not_executed", reason }), false)
})

import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../src/errors.js"
import { SocketProxy } from "../src/proxy.js"

const actors = { Room: {} }

test("malformed socket grants report a protocol error", async () => {
    const grant = {
        websocketUrl: "wss://host.example/socket?key=ticket",
        homeRegion: "local",
        connectByMs: 1000
    }
    for (const invalid of [
        { websocketUrl: "not a URL" },
        { websocketUrl: "https://host.example/socket" },
        { connectByMs: "1000" },
        { connectByMs: 1.5 },
        { connectByMs: Number.MAX_SAFE_INTEGER + 1 },
        { homeRegion: "" }
    ]) {
        const proxy = new SocketProxy(
            actors,
            {},
            {
                fetch: async () => Response.json({ ...grant, ...invalid })
            }
        )
        await assert.rejects(proxy.handle({ actorName: "Room", actorId: "one", metadata: {} }), ActorProtocolError)
    }
})

test("socket grants use environment settings and prefer the shared secret over the API key", async t => {
    const environment = process.env
    t.after(() => {
        process.env = environment
    })
    process.env = {
        ...environment,
        DURABLE_ACTORS_PROJECT_ID: "local",
        DURABLE_ACTORS_SECRET: "new-key",
        DURABLE_ACTORS_CONTROL_PLANE_URL: "http://127.0.0.1:8123",
        DURABLE_ACTORS_API_KEY: "wrong"
    }
    const proxy = new SocketProxy(
        actors,
        {},
        {
            fetch: async (url, init) => {
                assert.equal(String(url), "http://127.0.0.1:8123/v1/projects/local/actors/Room/one/find-websocket")
                assert.equal(new Headers(init?.headers).get("authorization"), "Bearer new-key")
                return Response.json({
                    websocketUrl: "ws://127.0.0.1:8123/socket?key=ticket",
                    homeRegion: "local",
                    connectByMs: 1000
                })
            }
        }
    )
    await proxy.handle({ actorName: "Room", actorId: "one", metadata: {} })
})

test("socket grants target the configured project as well as the actor", async () => {
    for (const projectId of ["team-a", "team-b"]) {
        const options = { projectId, controlPlaneUrl: "https://actors.example.com", apiKey: "secret" }
        const proxy = new SocketProxy(actors, options, {
            fetch: async url => {
                assert.equal(
                    String(url),
                    `https://actors.example.com/v1/projects/${projectId}/actors/Room/shared/find-websocket`
                )
                return Response.json({
                    websocketUrl: "wss://actors.example.com/v1/socket?key=ticket",
                    homeRegion: "us-east",
                    connectByMs: 1000
                })
            }
        })
        await proxy.handle({ actorName: "Room", actorId: "shared", metadata: {} })
    }
})

test("socket setup accepts an explicit home region and validates its timeout", async () => {
    const proxy = new SocketProxy(
        actors,
        { projectId: "default", apiKey: "secret", setupTimeoutMs: 180000 },
        {
            fetch: async (_url, init) => {
                assert.equal(JSON.parse(init!.body as string).homeRegion, "north-america-west")
                return Response.json({
                    websocketUrl: "wss://modal.example/v1/socket?_modal_connect_token=modal&key=actor",
                    homeRegion: "north-america-east",
                    connectByMs: 1000
                })
            }
        }
    )
    const grant = await proxy.handle({
        actorName: "Room",
        actorId: "room",
        metadata: {},
        homeRegion: "north-america-west"
    })
    assert.equal(new URL(grant.websocketUrl).searchParams.get("_modal_connect_token"), "modal")
    assert.throws(
        () => new SocketProxy(actors, { projectId: "default", apiKey: "secret", setupTimeoutMs: 0 }),
        /timeout/i
    )
})

test("proxy issues socket authorization using only server-selected target and metadata", async () => {
    const requests: { url: string; headers: Headers; body: unknown }[] = []
    const proxy = new SocketProxy(
        actors,
        { projectId: "default", controlPlaneUrl: "https://actors.example.com", apiKey: "backend-secret" },
        {
            fetch: async (url, init) => {
                requests.push({
                    url: String(url),
                    headers: new Headers(init?.headers),
                    body: JSON.parse(init!.body as string)
                })
                return Response.json({
                    websocketUrl: "wss://actors.example.com/v1/socket?key=socket-ticket",
                    homeRegion: "north-america-east",
                    connectByMs: 1000
                })
            }
        }
    )
    const grant = await proxy.handle({
        actorName: "Room",
        actorId: "lobby",
        metadata: { userId: "trusted" }
    })
    assert.deepEqual(grant, {
        websocketUrl: "wss://actors.example.com/v1/socket?key=socket-ticket",
        homeRegion: "north-america-east",
        connectByMs: 1000
    })
    assert.equal(requests[0]!.headers.get("authorization"), "Bearer backend-secret")
    assert.equal(requests[0]!.url, "https://actors.example.com/v1/projects/default/actors/Room/lobby/find-websocket")
    assert.deepEqual(requests[0]!.body, {
        metadata: { userId: "trusted" }
    })
    assert.equal(requests.length, 1)
})

test("proxy requires JSON metadata and a backend API key", async () => {
    const proxy = new SocketProxy(
        actors,
        { projectId: "default", controlPlaneUrl: "https://actors.example.com", apiKey: "secret" },
        {
            fetch: async () => assert.fail("invalid request reached issuance")
        }
    )
    await assert.rejects(
        proxy.handle({ actorName: "Room", actorId: "lobby", metadata: { invalid: () => undefined } }),
        /JSON/
    )
    assert.throws(
        () =>
            new SocketProxy(actors, {
                projectId: "default",
                controlPlaneUrl: "https://actors.example.com",
                apiKey: ""
            }),
        /apiKey/
    )
})

test("local backend socket grants need no project or secret", async t => {
    const environment = process.env
    t.after(() => {
        process.env = environment
    })
    process.env = Object.fromEntries(Object.entries(environment).filter(([key]) => !key.startsWith("DURABLE_ACTORS_")))
    const proxy = new SocketProxy(
        actors,
        {},
        {
            fetch: async (url, init) => {
                assert.equal(String(url), "http://127.0.0.1:7100/v1/projects/local/actors/Room/one/find-websocket")
                assert.equal(new Headers(init?.headers).get("authorization"), null)
                return Response.json({
                    websocketUrl: "ws://127.0.0.1:7100/v1/socket?key=ticket",
                    homeRegion: "local",
                    connectByMs: 1000
                })
            }
        }
    )
    await proxy.handle({ actorName: "Room", actorId: "one", metadata: {} })
})

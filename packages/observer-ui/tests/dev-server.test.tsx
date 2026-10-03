import assert from "node:assert/strict"
import { createServer as createHttpServer } from "node:http"
import type { AddressInfo } from "node:net"
import { test } from "node:test"
import { createServer } from "vite"

import createConfig from "../vite.config.js"

test("the Vite proxy preserves the observer's loopback host validation", async t => {
    const config = createConfig({ command: "serve", mode: "development" })
    const upstream = createHttpServer((request, response) => {
        const expected = `127.0.0.1:${(upstream.address() as AddressInfo).port}`
        response.writeHead(request.headers.host === expected ? 200 : 403)
        response.end(JSON.stringify({ connectionsComplete: true, actors: [] }))
    })
    await new Promise<void>(resolve => upstream.listen(0, "127.0.0.1", resolve))
    t.after(() => new Promise<void>(resolve => upstream.close(() => resolve())))
    const target = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`
    const proxy = config.server!.proxy!["/api/observe"]!
    const server = await createServer({
        ...config,
        configFile: false,
        server: { ...config.server, port: 0, proxy: { "/api/observe": { ...(typeof proxy === "string" ? {} : proxy), target } } }
    })
    t.after(() => server.close())
    await server.listen()
    const response = await fetch(`http://127.0.0.1:${(server.httpServer!.address() as AddressInfo).port}/api/observe/actors`)
    assert.equal(response.status, 200)
    assert.deepEqual(await response.json(), { connectionsComplete: true, actors: [] })
})

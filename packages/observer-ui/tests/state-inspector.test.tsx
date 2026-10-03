import React from "react"

import { JSDOM } from "jsdom"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import "./dom.js"

const { cleanup, fireEvent, render } = await import("@testing-library/react")
const { StateInspector } = await import("../src/StateInspector.js")
afterEach(cleanup)

const snapshots = [1, 2].map(stateVersion => ({
    stateVersion,
    ownerEpoch: 1,
    requestId: `req-${stateVersion}`,
    state: { count: stateVersion, settings: { enabled: true } },
    attribution: { operation: "onMessage", connectionId: "socket-a", committedAtMs: 1000 + stateVersion, interleaved: false }
}))
const client = {
    listActors: async () => ({ connectionsComplete: true, actors: [] }),
    checkConnection: async () => {},
    getState: async (query: { version?: number }) => ({ snapshot: snapshots[(query.version ?? 2) - 1]!, schema: null }),
    listStateHistory: async () => ({ records: snapshots.toReversed().map(({ state, ...record }) => record), nextBefore: null })
}

test("inspects persisted state and compares a retained version with attribution", async () => {
    const view = render(<StateInspector client={client} actorName="Counter" actorId="one" />)
    assert.ok(await view.findByText("req-2"))
    assert.ok(view.getByText("socket-a"))
    assert.ok(view.getByText("count"))
    fireEvent.click(view.getByRole("button", { name: "Changes" }))
    fireEvent.click(await view.findByRole("button", { name: /Version 2/ }))
    assert.ok(await view.findByText(/count/))
    assert.ok(view.getByText(/Comparing version 1/))
})

test("explains an actor without committed state", async () => {
    const view = render(<StateInspector client={{ ...client, getState: async () => ({ snapshot: null, schema: null }) }} actorName="Counter" actorId="new" />)
    assert.ok(await view.findByText(/No persisted state yet/))
})

test("trace version signals refresh storage while a selected historical comparison stays pinned", async () => {
    let publish: (page: any) => void = () => {}
    let version = 2
    const live = {
        ...client,
        getState: async (query: { version?: number }) => ({
            snapshot: { ...snapshots[1]!, stateVersion: query.version ?? version, requestId: `req-${query.version ?? version}`, state: { count: query.version ?? version } },
            schema: null
        }),
        watchRequests: async (receive: typeof publish) => {
            publish = receive
            await new Promise<void>(() => {})
        }
    }
    const view = render(<StateInspector client={live} actorName="Counter" actorId="one" />)
    await view.findByText("req-2")
    fireEvent.click(view.getByRole("button", { name: "Changes" }))
    fireEvent.click(await view.findByRole("button", { name: /Version 2/ }))
    await view.findByText(/Comparing version 1/)
    version = 3
    const { act } = await import("react")
    await act(async () => publish({ records: [{ actorName: "Counter", actorId: "one", stateVersion: 3 }] }))
    assert.ok(await view.findByRole("button", { name: "View latest · version 3" }))
    assert.ok(view.getByText(/Comparing version 1 → 2/))
})

test("diff formatting escapes actor-controlled keys and values", async () => {
    const { stateDiff } = await import("../src/state-diff.js")
    const payloads = [
        "<script>alert(1)</script>",
        "<SCRIPT>alert(1)</SCRIPT>",
        "<ScRiPt src=x></ScRiPt >",
        '<IMG SRC=x ONERROR="alert(1)">',
        '<svg onload="alert(1)"></svg>',
        '\"><img src=x onerror=alert(1)>',
        "&lt;script&gt;alert(1)&lt;/script&gt;"
    ]
    for (const payload of payloads) {
        const fragment = JSDOM.fragment(stateDiff({}, { [payload]: payload }))
        assert.equal(fragment.querySelector("script, img, svg, iframe, object, embed"), null)
        for (const element of fragment.querySelectorAll("*")) {
            assert.ok([...element.attributes].every(attribute => !attribute.name.toLowerCase().startsWith("on")))
        }
        assert.ok(fragment.textContent?.includes(payload))
    }
})

test("refresh keeps pagination available when newer commits exceed the first history page", async () => {
    let version = 2
    const live = {
        ...client,
        getState: async () => ({ snapshot: { ...snapshots[1]!, stateVersion: version }, schema: null }),
        listStateHistory: async (query: { before?: number }) => ({
            records: (query.before ? [3, 2, 1] : version === 2 ? [2, 1] : [5, 4]).map(stateVersion => ({ ...snapshots[0]!, stateVersion })),
            nextBefore: version === 5 && !query.before ? 4 : null
        })
    }
    const view = render(<StateInspector client={live} actorName="Counter" actorId="one" />)
    await view.findByText("req-2")
    version = 5
    fireEvent.click(view.getByRole("button", { name: "Refresh state" }))
    fireEvent.click(view.getByRole("button", { name: "Changes" }))
    fireEvent.click(await view.findByRole("button", { name: "Load older versions" }))
    assert.ok(await view.findByRole("button", { name: /Version 3/ }))
})

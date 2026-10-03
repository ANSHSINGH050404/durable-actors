import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { test } from "node:test"

import * as api from "../src/index.js"

test("importing actor definitions does not load remote transports or TypeScript tooling", () => {
    const entrypoint = new URL("../src/index.js", import.meta.url).href
    execFileSync(process.execPath, [
        "--input-type=module",
        "--eval",
        `
        import { register } from "node:module";
        register("data:text/javascript," + encodeURIComponent(\`export function resolve(specifier, context, next) {
            if (/^(?:@grpc\\\\/|protobufjs$|ws$|tsx\\\\/)/.test(specifier)) throw new Error("eager dependency: " + specifier);
            return next(specifier, context);
        }\`));
        await import(${JSON.stringify(entrypoint)});
    `
    ])
})

test("the package root exposes the complete minimal actor API", () => {
    assert.deepEqual(Object.keys(api).sort(), [
        "Actor",
        "ActorInvocationError",
        "Emittable",
        "Ephemeral",
        "Interleave",
        "Persisted",
        "Sandbox"
    ])
})

test("actor calls read environment settings lazily without a setup function", () => {
    const entrypoint = new URL("../src/index.js", import.meta.url).href
    {
        execFileSync(process.execPath, [
            "--input-type=module",
            "--eval",
            `
            import assert from "node:assert/strict";
            for (const key of ["DURABLE_ACTORS_SECRET", "DURABLE_ACTORS_HOME_REGION", "DURABLE_ACTORS_CONTROL_PLANE_URL"]) delete process.env[key];
            const { Actor, ActorInvocationError } = await import(${JSON.stringify(entrypoint)});
            Object.assign(process.env, {
                DURABLE_ACTORS_PROJECT_ID: "default",
                DURABLE_ACTORS_SECRET: "backend-key",
                DURABLE_ACTORS_CONTROL_PLANE_URL: "https://control.example.com"
            });
            const requests = [];
            globalThis.fetch = async (url, options) => {
                assert.equal(options.headers.authorization, "Bearer backend-key");
                requests.push(url);
                return new Response("{}", { status: 401 });
            };
            class Counter extends Actor { async increment() { return 1; } }
            const counter = Counter.get("one");
            await assert.rejects(counter.increment(), error => error instanceof ActorInvocationError && error.code === "unauthenticated");
            await assert.rejects(counter.broadcast("hello"), error => error instanceof ActorInvocationError && error.code === "unauthenticated");
            assert.deepEqual(requests, [
                "https://control.example.com/v1/projects/default/actors/Counter/one/invoke",
                "https://control.example.com/v1/projects/default/actors/Counter/one/find-actor"
            ]);
        `
        ])
    }
})

import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import { stampReleaseVersion, verifyReleaseVersion } from "../../scripts/release.mjs"

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8")

test("release version changes keep Python packages and native binaries aligned", () => {
    const manifests = {
        cargoLock: read("Cargo.lock"),
        cargoToml: read("Cargo.toml"),
        npmPackage: read("sdk/package.json"),
        observerPackage: read("packages/observer-ui/package.json"),
        pythonPackage: read("sdk-python/pyproject.toml"),
        pythonLock: read("sdk-python/uv.lock"),
        helmChart: read("charts/terse/Chart.yaml")
    }
    const stamped = stampReleaseVersion(manifests, "9.8.7")
    assert.match(stamped.pythonPackage, /^version = "9.8.7"$/m)
    assert.match(stamped.pythonLock, /name = "durable-actors"\nversion = "9.8.7"/)
    verifyReleaseVersion(stamped, "9.8.7")
    assert.throws(() => verifyReleaseVersion({ ...stamped, pythonPackage: manifests.pythonPackage }, "9.8.7"), /pyproject/)
})

test("runtime image can load Python actor executors", () => {
    assert.match(read("Dockerfile"), /FROM python:3\.13-slim-bookworm AS python-sdk/)
    assert.match(read("Dockerfile"), /COPY --from=python-sdk \/usr\/local \/usr\/local/)
    assert.match(read(".dockerignore"), /^!sdk-python\/src\/\*\*$/m)
})

import assert from "node:assert/strict"
import test from "node:test"

import { pinChartImage } from "../../scripts/pin-chart-image.mjs"

test("published chart values include the runtime digest from the image job", () => {
    assert.equal(pinChartImage('image:\n  digest: ""\n', `sha256:${"a".repeat(64)}`), `image:\n  digest: "${"a".repeat(64)}"\n`)
})

test("chart packaging fails for an invalid digest or ambiguous values", () => {
    assert.throws(() => pinChartImage('image:\n  digest: ""\n', "latest"), /digest/)
    assert.throws(() => pinChartImage("image: {}\n", `sha256:${"a".repeat(64)}`), /one image digest/)
    assert.throws(() => pinChartImage('  digest: ""\n  digest: ""\n', `sha256:${"a".repeat(64)}`), /one image digest/)
})

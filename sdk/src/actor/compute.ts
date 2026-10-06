import { z } from "zod"

import { ActorDefinitionError } from "../errors.js"

const computeRegion = z.enum([
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast"
])

const computeOptionsSchema = z.strictObject({
    cpu: z.number().min(0.1).max(64).multipleOf(0.001).optional(),
    memoryMiB: z.number().int().min(128).max(262144).optional(),
    idleTimeoutMs: z.number().int().min(1).max(86400000).optional(),
    regions: z
        .array(computeRegion)
        .min(1)
        .max(computeRegion.options.length)
        .refine(regions => new Set(regions).size === regions.length, "regions must be unique")
        .optional()
})

type ComputeRegion = z.infer<typeof computeRegion>
interface ComputeOptions {
    /** CPU cores, from 0.1 to 64 in increments of 0.001. */
    readonly cpu?: number
    /** Memory request and cap, from 128 to 262144 MiB. */
    readonly memoryMiB?: number
    /** Inactivity before eviction, from 1 to 86400000 ms. Inherits the server default (normally 10000). */
    readonly idleTimeoutMs?: number
    /** Allowed compute regions, without priority order. Existing actors remain in their saved region. */
    readonly regions?: readonly ComputeRegion[]
}

/** Overrides the deployment's compute defaults for this actor class. */
function Compute(options: ComputeOptions) {
    computeOptionsSchema.parse(options)
    return (_value: Function, context: Pick<ClassDecoratorContext, "kind">): void => {
        if (context.kind !== "class") throw new ActorDefinitionError("@Compute requires an actor class")
    }
}

export { Compute, computeOptionsSchema }
export type { ComputeOptions, ComputeRegion }

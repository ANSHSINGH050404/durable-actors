import { z } from "zod"

import { ActorDefinitionError } from "../errors.js"

const sandboxRegion = z.enum([
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast"
])

const sandboxOptionsSchema = z.strictObject({
    cpu: z.number().min(0.1).max(64).multipleOf(0.001).optional(),
    memoryMiB: z.number().int().min(128).max(262144).optional(),
    idleTimeoutMs: z.number().int().min(1).max(86400000).optional(),
    regions: z
        .array(sandboxRegion)
        .min(1)
        .max(sandboxRegion.options.length)
        .refine(regions => new Set(regions).size === regions.length, "regions must be unique")
        .optional()
})

type SandboxRegion = z.infer<typeof sandboxRegion>
interface SandboxOptions {
    /** CPU cores, from 0.1 to 64 in increments of 0.001. */
    readonly cpu?: number
    /** Memory request and cap, from 128 to 262144 MiB. */
    readonly memoryMiB?: number
    /** Inactivity before eviction, from 1 to 86400000 ms. Inherits the server default (normally 10000). */
    readonly idleTimeoutMs?: number
    /** Allowed compute regions, without priority order. Existing actors remain in their saved region. */
    readonly regions?: readonly SandboxRegion[]
}

/** Overrides the deployment's sandbox defaults for this actor class. */
function Sandbox(options: SandboxOptions) {
    sandboxOptionsSchema.parse(options)
    return (_value: Function, context: Pick<ClassDecoratorContext, "kind">): void => {
        if (context.kind !== "class") throw new ActorDefinitionError("@Sandbox requires an actor class")
    }
}

export { Sandbox, sandboxOptionsSchema }
export type { SandboxOptions, SandboxRegion }

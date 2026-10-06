import { Ajv } from "ajv"
import type { JSONSchema7, JSONSchema7Definition } from "json-schema"
import ts from "typescript"
import { z } from "zod"

import { computeOptionsSchema } from "../actor/compute.js"
import type { SocketContract } from "../wire/contract.js"
import type { ActorApi, PublicActorContract, RpcContract } from "../wire/public-contract.js"

function parsePublicContract(input: unknown): PublicActorContract {
    const document = documentSchema.parse(input)
    const names = new Set<string>()
    for (const actor of document.actors) {
        unique(names, actor.actorName, "actor")
        validateActor(actor)
    }
    return document
}

function validateActor(actor: ActorApi): void {
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, actor.actorName)
    if (scanner.scan() !== ts.SyntaxKind.Identifier || scanner.scan() !== ts.SyntaxKind.EndOfFileToken)
        throw new Error(`actor name ${actor.actorName} cannot be emitted as a TypeScript identifier`)
    if (actor.actorName !== actor.socket.actorName) throw new Error("actor and socket contract names must match")
    validateSocket(actor.socket)
    validateRpc(actor.rpc)
}

function validateSocket(socket: SocketContract): void {
    for (const kind of ["Metadata", "Incoming", "Outgoing", "State"]) reference(socket.schema, `#/definitions/${kind}`)
    const fields = new Set<string>()
    const state = socket.schema.definitions!.State
    for (const field of socket.emittable) {
        unique(fields, field, "emittable field")
        if (typeof state !== "object" || !Object.hasOwn(state.properties ?? {}, field))
            throw new Error(`emittable field ${field} is not public state`)
    }
}

function validateRpc(rpc: RpcContract): void {
    const methods = new Set<string>()
    for (const method of rpc.methods) {
        unique(methods, method.name, "RPC method")
        if (
            ["constructor", "then", "connect", "broadcast", "db", "onConnect", "onMessage", "onDisconnect"].includes(
                method.name
            )
        )
            throw new Error(`reserved RPC method ${method.name}`)
        let optionalSeen = false
        method.parameters.forEach((parameter, index) => {
            const type = reference(rpc.schema, parameter.type.$ref)
            if (
                parameter.rest &&
                (parameter.optional ||
                    index !== method.parameters.length - 1 ||
                    typeof type !== "object" ||
                    type.type !== "array" ||
                    Array.isArray(type.items))
            )
                throw new Error("rest parameter must be a final, required array parameter")
            if (optionalSeen && !parameter.optional && !parameter.rest)
                throw new Error("required parameter follows optional parameter")
            optionalSeen ||= parameter.optional
        })
        if (method.result.kind === "value") reference(rpc.schema, method.result.type.$ref)
    }
}

function unique(seen: Set<string>, value: string, kind: string): void {
    if (seen.has(value)) throw new Error(`duplicate ${kind} ${value}`)
    seen.add(value)
}

function parseSchema(input: unknown, context: z.RefinementCtx): JSONSchema7 | typeof z.NEVER {
    if (!ajv.validate<JSONSchema7>("http://json-schema.org/draft-07/schema#", input)) {
        context.addIssue({ code: "custom", message: `invalid contract schema: ${ajv.errorsText()}` })
        return z.NEVER
    }
    visit(input, input)
    return input
}

function visit(node: JSONSchema7Definition, root: JSONSchema7): void {
    if (typeof node === "boolean") return
    if (["$id", "id", "tsType"].some(key => Object.hasOwn(node, key)))
        throw new Error("contract schemas cannot override type resolution")
    if (node.$ref) reference(root, node.$ref)
    for (const children of [node.definitions, node.properties, node.patternProperties])
        for (const child of Object.values(children ?? {})) visit(child, root)
    for (const child of [
        node.items,
        node.additionalItems,
        node.additionalProperties,
        node.contains,
        node.propertyNames,
        node.not,
        node.if,
        node.then,
        node.else,
        node.allOf,
        node.anyOf,
        node.oneOf
    ]) {
        if (Array.isArray(child)) child.forEach(item => visit(item, root))
        else if (child !== undefined) visit(child, root)
    }
    for (const child of Object.values(node.dependencies ?? {})) if (!Array.isArray(child)) visit(child, root)
}

function reference(schema: JSONSchema7, ref: string): JSONSchema7Definition {
    if (!ref.startsWith("#/definitions/")) throw new Error("contract type references must be local definitions")
    let target: unknown = schema
    for (const part of ref.slice(2).split("/")) {
        const key = part.replaceAll("~1", "/").replaceAll("~0", "~")
        target =
            target && typeof target === "object" && Object.hasOwn(target, key)
                ? (target as Record<string, unknown>)[key]
                : undefined
    }
    if (typeof target !== "boolean" && (!target || typeof target !== "object" || Array.isArray(target)))
        throw new Error(`contract type reference is missing: ${ref}`)
    return target as JSONSchema7Definition
}

const ajv = new Ajv({ strict: false, validateFormats: false })
const schema = z.looseObject({ definitions: z.record(z.string(), z.unknown()) }).transform(parseSchema)
const typeReference = z.strictObject({ $ref: z.string() })
const component = z
    .string()
    .min(1)
    .max(255)
    .regex(/^[A-Za-z0-9._-]+$/u)
const documentSchema = z.strictObject({
    version: z.literal(1),
    typescript: z.strictObject({
        declarations: z.string().min(1),
        dependencies: z.record(z.string().regex(/^(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+$/u), z.string().min(1))
    }),
    actors: z.array(
        z.strictObject({
            actorName: component,
            description: z.string().optional(),
            sandbox: computeOptionsSchema.optional(),
            socket: z.strictObject({
                version: z.literal(1),
                actorName: component,
                schema,
                emittable: z.array(z.string())
            }),
            rpc: z.strictObject({
                schema,
                methods: z.array(
                    z.strictObject({
                        name: component,
                        description: z.string().optional(),
                        parameters: z.array(
                            z.strictObject({
                                name: z.string().min(1),
                                optional: z.boolean(),
                                rest: z.boolean(),
                                type: typeReference
                            })
                        ),
                        result: z.discriminatedUnion("kind", [
                            z.strictObject({ kind: z.literal("void") }),
                            z.strictObject({ kind: z.literal("value"), type: typeReference })
                        ])
                    })
                )
            })
        })
    )
})

export { parsePublicContract }

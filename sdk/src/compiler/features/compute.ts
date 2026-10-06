import ts from "typescript"

import { computeOptionsSchema } from "../../actor/compute.js"
import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readCompute(use: DecoratorUse): DecoratorResult {
    const call = use.node.expression
    if (!ts.isClassDeclaration(use.target) || !ts.isCallExpression(call) || call.arguments.length !== 1)
        return invalid(use, "@Compute requires an actor class and one literal options object")
    const argument = call.arguments[0]!
    if (!ts.isObjectLiteralExpression(argument)) return invalid(use, "@Compute requires a literal options object")
    const values: Record<string, unknown> = {}
    for (const property of argument.properties) {
        if (
            !ts.isPropertyAssignment(property) ||
            (!ts.isIdentifier(property.name) && !ts.isStringLiteral(property.name))
        )
            return invalid(use, "@Compute options must be explicit literal properties")
        const name = property.name.text
        if (Object.hasOwn(values, name)) return invalid(use, `@Compute repeats option ${name}`)
        values[name] = literal(property.initializer)
        if (values[name] === undefined)
            return invalid(use, "@Compute options must use literal numbers, strings, or arrays")
    }
    const parsed = computeOptionsSchema.safeParse(values)
    if (!parsed.success) return invalid(use, `Invalid @Compute options: ${parsed.error.message}`)
    return { annotations: [{ kind: AnnotationKind.Compute, node: use.node, options: parsed.data }], diagnostics: [] }
}

function validateCompute(actor: ParsedActor) {
    const annotations = actor.annotations.filter(annotation => annotation.kind === AnnotationKind.Compute)
    return {
        options: annotations[0]?.options,
        diagnostics: annotations
            .slice(1)
            .map(annotation => definitionDiagnostic(annotation.node, "@Compute cannot be repeated"))
    }
}

function literal(value: ts.Expression): unknown {
    if (ts.isNumericLiteral(value)) return Number(value.text)
    if (ts.isStringLiteral(value)) return value.text
    if (ts.isArrayLiteralExpression(value)) return value.elements.map(literal)
}

function invalid(use: DecoratorUse, message: string): DecoratorResult {
    return { annotations: [], diagnostics: [definitionDiagnostic(use.node, message)] }
}

export { readCompute, validateCompute }

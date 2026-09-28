import ts from "typescript"

import { sandboxOptionsSchema } from "../../actor/sandbox.js"
import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readSandbox(use: DecoratorUse): DecoratorResult {
    const call = use.node.expression
    if (!ts.isClassDeclaration(use.target) || !ts.isCallExpression(call) || call.arguments.length !== 1)
        return invalid(use, "@Sandbox requires an actor class and one literal options object")
    const argument = call.arguments[0]!
    if (!ts.isObjectLiteralExpression(argument)) return invalid(use, "@Sandbox requires a literal options object")
    const values: Record<string, unknown> = {}
    for (const property of argument.properties) {
        if (
            !ts.isPropertyAssignment(property) ||
            (!ts.isIdentifier(property.name) && !ts.isStringLiteral(property.name))
        )
            return invalid(use, "@Sandbox options must be explicit literal properties")
        const name = property.name.text
        if (Object.hasOwn(values, name)) return invalid(use, `@Sandbox repeats option ${name}`)
        values[name] = literal(property.initializer)
        if (values[name] === undefined)
            return invalid(use, "@Sandbox options must use literal numbers, strings, or arrays")
    }
    const parsed = sandboxOptionsSchema.safeParse(values)
    if (!parsed.success) return invalid(use, `Invalid @Sandbox options: ${parsed.error.message}`)
    return { annotations: [{ kind: AnnotationKind.Sandbox, node: use.node, options: parsed.data }], diagnostics: [] }
}

function validateSandbox(actor: ParsedActor) {
    const annotations = actor.annotations.filter(annotation => annotation.kind === AnnotationKind.Sandbox)
    return {
        options: annotations[0]?.options,
        diagnostics: annotations
            .slice(1)
            .map(annotation => definitionDiagnostic(annotation.node, "@Sandbox cannot be repeated"))
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

export { readSandbox, validateSandbox }

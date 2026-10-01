import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind, Persistence } from "../types.js"
import type { ActorFieldSchema, DecoratorResult, DecoratorUse, ParsedActor, ParsedMember } from "../types.js"

function validatePersistence(actor: ParsedActor) {
    const fields: ActorFieldSchema[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const node = member.node
        if (ts.isConstructorDeclaration(node)) {
            for (const parameter of node.parameters) {
                if (ts.isParameterPropertyDeclaration(parameter, node))
                    diagnostics.push(
                        definitionDiagnostic(
                            parameter,
                            "actor parameter properties must become explicitly decorated class fields"
                        )
                    )
            }
        }
        if (!ts.isPropertyDeclaration(node) || hasModifier(node, ts.SyntaxKind.StaticKeyword)) continue
        const result = validateField(actor.name, member)
        diagnostics.push(...result.diagnostics)
        if (result.field !== undefined) fields.push(result.field)
    }
    return { fields, diagnostics }
}

function readPersistence(use: DecoratorUse, mode: Persistence): DecoratorResult {
    const name = mode === Persistence.Persisted ? "Persisted" : "Ephemeral"
    const node = use.target
    if (
        !ts.isPropertyDeclaration(node) ||
        hasModifier(node, ts.SyntaxKind.StaticKeyword) ||
        hasModifier(node, ts.SyntaxKind.AccessorKeyword)
    )
        return invalid(use, `@${name} requires an instance field`)
    if (use.called) return invalid(use, `use @${name} without parentheses or arguments`)
    if (hasModifier(node, ts.SyntaxKind.DeclareKeyword))
        return invalid(use, `@${name} requires a runtime field, not a declare field`)
    if (fieldName(node.name) === undefined) return invalid(use, `@${name} requires a field with a literal string name`)
    if (mode === Persistence.Persisted && ts.isPrivateIdentifier(node.name))
        return invalid(use, "@Persisted cannot decorate a JavaScript private field; use @Ephemeral")
    return { annotations: [{ kind: AnnotationKind.Persistence, mode, node: use.node }], diagnostics: [] }
}

function readEmission(use: DecoratorUse): DecoratorResult {
    const node = use.target
    if (
        !ts.isPropertyDeclaration(node) ||
        hasModifier(node, ts.SyntaxKind.StaticKeyword) ||
        hasModifier(node, ts.SyntaxKind.AccessorKeyword)
    )
        return invalid(use, "@Emittable requires an instance field")
    if (use.called) return invalid(use, "use @Emittable without parentheses or arguments")
    if (
        ts.isPrivateIdentifier(node.name) ||
        hasModifier(node, ts.SyntaxKind.PrivateKeyword) ||
        hasModifier(node, ts.SyntaxKind.ProtectedKeyword)
    )
        return invalid(use, "@Emittable requires a public persisted field")
    return { annotations: [{ kind: AnnotationKind.Emission, node: use.node }], diagnostics: [] }
}

function validateField(
    actorName: string,
    member: ParsedMember
): { field?: ActorFieldSchema; diagnostics: ts.Diagnostic[] } {
    const node = member.node as ts.PropertyDeclaration
    if (fieldName(node.name) === "db")
        return { diagnostics: [definitionDiagnostic(node.name, "actor field db is reserved for SQLite")] }
    if (member.diagnostics.length > 0) return { diagnostics: [] }
    const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Persistence)
    const emission = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Emission)
    if (annotations.length !== 1)
        return {
            diagnostics: [
                definitionDiagnostic(
                    node.name,
                    `actor field ${actorName}.${node.name.getText()} must declare exactly one of @Persisted or @Ephemeral`,
                    annotations.map(annotation => annotation.node)
                )
            ]
        }
    if (emission.length > 1 || (emission.length === 1 && annotations[0]!.mode !== Persistence.Persisted))
        return {
            diagnostics: [
                definitionDiagnostic(node, "@Emittable requires a public @Persisted field and cannot be repeated")
            ]
        }
    return {
        field: {
            name: fieldName(node.name)!,
            persistence: annotations[0]!.mode,
            ...(ts.isPrivateIdentifier(node.name) ? { private: true } : {}),
            ...(hasModifier(node, ts.SyntaxKind.PrivateKeyword)
                ? { visibility: "private" as const }
                : hasModifier(node, ts.SyntaxKind.ProtectedKeyword)
                  ? { visibility: "protected" as const }
                  : {}),
            ...(emission.length === 1 ? { emittable: true } : {})
        },
        diagnostics: []
    }
}

function fieldName(name: ts.PropertyName): string | undefined {
    if (ts.isIdentifier(name) || ts.isPrivateIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name))
        return name.text
    if (
        ts.isComputedPropertyName(name) &&
        (ts.isStringLiteral(name.expression) || ts.isNumericLiteral(name.expression))
    )
        return name.expression.text
}

function hasModifier(node: ts.Node, kind: ts.SyntaxKind): boolean {
    return ts.canHaveModifiers(node) && (ts.getModifiers(node)?.some(modifier => modifier.kind === kind) ?? false)
}

function invalid(use: DecoratorUse, message: string): DecoratorResult {
    return { annotations: [], diagnostics: [definitionDiagnostic(use.node, message)] }
}

export { readEmission, readPersistence, validatePersistence }

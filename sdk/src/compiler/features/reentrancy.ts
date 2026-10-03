import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readReentrancy(use: DecoratorUse): DecoratorResult {
    const node = use.target
    if (use.called) return invalidReentrancy(use)
    if (!ts.isMethodDeclaration(node) || !node.body) return invalidReentrancy(use)
    if (!hasModifier(node, ts.SyntaxKind.AsyncKeyword)) return invalidReentrancy(use)
    if (hasModifier(node, ts.SyntaxKind.StaticKeyword)) return invalidReentrancy(use)
    if (hasModifier(node, ts.SyntaxKind.PrivateKeyword) || hasModifier(node, ts.SyntaxKind.ProtectedKeyword))
        return invalidReentrancy(use)
    if (!ts.isIdentifier(node.name) && !ts.isStringLiteral(node.name)) return invalidReentrancy(use)

    return { annotations: [{ kind: AnnotationKind.Reentrancy, node: use.node }], diagnostics: [] }
}

function validateReentrancy(actor: ParsedActor) {
    const methods: string[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Reentrancy)
        if (annotations.length > 1)
            diagnostics.push(definitionDiagnostic(member.node, "@Interleave cannot be repeated"))
        if (annotations.length === 1) methods.push((member.node.name as ts.Identifier | ts.StringLiteral).text)
    }
    return { methods, diagnostics }
}

function hasModifier(node: ts.MethodDeclaration, kind: ts.SyntaxKind): boolean {
    return ts.getModifiers(node)?.some(modifier => modifier.kind === kind) ?? false
}

function invalidReentrancy(use: DecoratorUse): DecoratorResult {
    return {
        annotations: [],
        diagnostics: [
            definitionDiagnostic(
                use.target,
                "@Interleave requires a public instance async method; use it without parentheses"
            )
        ]
    }
}

export { readReentrancy, validateReentrancy }

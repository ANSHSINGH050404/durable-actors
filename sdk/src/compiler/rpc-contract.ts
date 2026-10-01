import ts from "typescript"

import { validateActorComponent } from "../actor/identity.js"
import { ActorDefinitionError } from "../errors.js"
import type { RpcContract, RpcMethod, RpcParameter, TypeReference } from "../wire/public-contract.js"

import { assertJsonType, jsonSchema, sourceTypeName } from "./json-schema.js"
import { extractPublicSchema } from "./public-schema.js"

function rpcContract(checker: ts.TypeChecker, actor: ts.ClassDeclaration): RpcContract {
    const types: Record<string, ts.Type> = Object.create(null)
    const methods = publicMethods(checker, actor).map(({ name, declaration }) =>
        readMethod(checker, declaration, `${actor.name!.text}.${name}`, name, types)
    )
    return { schema: rpcSchema(checker, types), methods }
}

function rpcSchema(checker: ts.TypeChecker, types: Record<string, ts.Type>) {
    const schema = jsonSchema(checker, types)
    for (const [name, definition] of Object.entries(schema.definitions ?? {})) {
        if (typeof definition === "boolean" || definition.$ref) continue
        const title = types[name] ? sourceTypeName(types[name]) : /^[A-Za-z_$][\w$]*$/u.test(name) ? name : undefined
        if (title) definition.title ??= title
    }
    return extractPublicSchema(schema, Object.keys(types))
}

function publicMethods(checker: ts.TypeChecker, actor: ts.ClassDeclaration) {
    const methods: { name: string; declaration: ts.MethodDeclaration }[] = []
    for (const member of actor.members) {
        if (!member.name || ts.isPrivateIdentifier(member.name) || !isPublicInstance(member)) continue
        const symbol = checker.getSymbolAtLocation(member.name)
        const name = symbol?.name ?? member.name.getText()
        const label = `${actor.name!.text}.${name}`
        if (ts.isGetAccessorDeclaration(member) || ts.isSetAccessorDeclaration(member))
            throw new ActorDefinitionError(`${label}: accessors are not supported`)
        if (!ts.isMethodDeclaration(member)) {
            if (checker.getTypeAtLocation(member).getCallSignatures().length)
                throw new ActorDefinitionError(`${label}: callable fields must be declared as async methods`)
            continue
        }
        if (["onConnect", "onMessage", "onDisconnect"].includes(name)) continue
        if (["then", "connect", "broadcast", "db"].includes(name))
            throw new ActorDefinitionError(`${label}: reserved RPC method name`)
        validateActorComponent("actor method", name)
        if (symbol?.declarations?.filter(ts.isMethodDeclaration).length !== 1)
            throw new ActorDefinitionError(`${label}: overloaded RPC methods are not supported`)
        if (!member.modifiers?.some(modifier => modifier.kind === ts.SyntaxKind.AsyncKeyword) || member.asteriskToken)
            throw new ActorDefinitionError(`${label}: RPC methods must be async functions`)
        if (member.typeParameters?.length)
            throw new ActorDefinitionError(`${label}: generic RPC methods are not supported`)
        if (member.questionToken) throw new ActorDefinitionError(`${label}: optional RPC methods are not supported`)
        methods.push({ name, declaration: member })
    }
    return methods.sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0))
}

function isPublicInstance(member: ts.ClassElement): boolean {
    const excluded = [ts.SyntaxKind.PrivateKeyword, ts.SyntaxKind.ProtectedKeyword, ts.SyntaxKind.StaticKeyword]
    return !ts.canHaveModifiers(member) || !ts.getModifiers(member)?.some(modifier => excluded.includes(modifier.kind))
}

function readMethod(
    checker: ts.TypeChecker,
    method: ts.MethodDeclaration,
    label: string,
    name: string,
    types: Record<string, ts.Type>
): RpcMethod {
    const signature = checker.getSignatureFromDeclaration(method)!
    const parameters = method.parameters.map((parameter, index) =>
        readParameter(checker, parameter, index, method, label, name, types)
    )
    const result = checker.getAwaitedType(signature.getReturnType())!
    if (result.flags & ts.TypeFlags.Void) return { name, parameters, result: { kind: "void" } }
    assertJsonType(checker, result, `${label} result`)
    return {
        name,
        parameters,
        result: { kind: "value", type: registerType(types, `Method_${name}_Result`, result) }
    }
}

function readParameter(
    checker: ts.TypeChecker,
    parameter: ts.ParameterDeclaration,
    index: number,
    method: ts.MethodDeclaration,
    label: string,
    methodName: string,
    types: Record<string, ts.Type>
): RpcParameter {
    const name = ts.isIdentifier(parameter.name) ? parameter.name.text : `arg${index}`
    if (name === "this") throw new ActorDefinitionError(`${label}: explicit this parameters are not supported`)
    const optional = !!(parameter.questionToken || parameter.initializer)
    const rest = !!parameter.dotDotDotToken
    const type = checker.getTypeAtLocation(parameter)
    if (rest && !checker.isArrayType(type))
        throw new ActorDefinitionError(`${label}: rest parameters must use an array type`)
    if (
        parameter.initializer &&
        method.parameters
            .slice(index + 1)
            .some(next => !next.questionToken && !next.initializer && !next.dotDotDotToken)
    )
        throw new ActorDefinitionError(`${label}: default parameters before required parameters are not supported`)
    assertJsonType(checker, type, `${label} parameter ${name}`, optional)
    return { name, optional, rest, type: registerType(types, `Method_${methodName}_Parameter_${index}`, type) }
}

function registerType(types: Record<string, ts.Type>, name: string, type: ts.Type): TypeReference {
    const existing = sourceTypeName(type) ? Object.keys(types).find(key => types[key] === type) : undefined
    const key = existing ?? name
    types[key] = type
    return { $ref: `#/definitions/${key}` }
}

export { rpcContract }

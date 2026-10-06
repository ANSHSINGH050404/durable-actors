import path from "node:path"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { validateActorComponent } from "../actor/identity.js"
import { ActorDefinitionError } from "../errors.js"
import type { PublicActorContract } from "../wire/public-contract.js"

import { DeclarationCompiler } from "./declarations.js"
import { readCompute, validateCompute } from "./features/compute.js"
import { readEmission, readPersistence, validatePersistence } from "./features/persistence.js"
import { readReentrancy, validateReentrancy } from "./features/reentrancy.js"
import { extractPublicSchema } from "./public-schema.js"
import { rpcContract } from "./rpc-contract.js"
import { socketContract } from "./socket-contract.js"
import { Persistence } from "./types.js"
import type {
    ActorAnalysis,
    ActorSchema,
    Annotation,
    CompilerOptions,
    DecoratorResult,
    DecoratorUse,
    ParsedActor,
    SdkSymbols
} from "./types.js"

class ActorCompiler {
    constructor(
        private readonly system: ts.System = ts.sys,
        private readonly declarations: Pick<DeclarationCompiler, "compile"> = new DeclarationCompiler(system)
    ) {}

    check(entrypoint: string, options: CompilerOptions = {}) {
        return this.analyze(entrypoint, options).schemas
    }

    compile(entrypoint: string, options: CompilerOptions = {}) {
        return this.describe(entrypoint, options).schemas
    }

    compileContract(entrypoint: string, options: CompilerOptions = {}): PublicActorContract {
        return this.compileDeployment(entrypoint, options).contract
    }

    compileDeployment(entrypoint: string, options: CompilerOptions = {}) {
        const { checker, actors, schemas, program } = this.describe(entrypoint, options)
        const api = {
            actors: [...schemas]
                .sort((left, right) =>
                    left.actorName < right.actorName ? -1 : left.actorName > right.actorName ? 1 : 0
                )
                .map(schema => ({
                    actorName: schema.actorName,
                    ...(schema.sandbox === undefined ? {} : { sandbox: schema.sandbox }),
                    socket: {
                        ...schema.contract,
                        schema: extractPublicSchema(schema.contract.schema, [
                            "Metadata",
                            "Incoming",
                            "Outgoing",
                            "State"
                        ])
                    },
                    rpc: rpcContract(
                        checker,
                        actors.find(actor => actor.name!.text === schema.actorName)!
                    )
                }))
        }
        const contract: PublicActorContract = {
            ...api,
            version: 1,
            typescript: this.declarations.compile(program, actors, api.actors)
        }
        const sources = new Map(program.getSourceFiles().map(file => [file.fileName, file.text]))
        return { schemas, contract, sources }
    }

    private describe(entrypoint: string, options: CompilerOptions) {
        const { program, source, sdk, schemas } = this.analyze(entrypoint, options)
        const checker = program.getTypeChecker()
        const actors = discoverActors(source, checker, sdk).actors
        return {
            program,
            checker,
            actors,
            schemas: schemas.map(schema => ({
                ...schema,
                contract: socketContract(
                    checker,
                    actors.find(actor => actor.name!.text === schema.actorName)!,
                    schema
                )
            }))
        }
    }

    private analyze(entrypoint: string, options: CompilerOptions) {
        const project = this.open(path.resolve(entrypoint), options)
        const program = ts.createProgram(project.files, project.options, project.host)
        const source = program.getSourceFile(project.entrypoint)!
        const sdkSource = program.getSourceFile(project.sdkEntrypoint)!
        const sdk = resolveSdkSymbols(program.getTypeChecker(), sdkSource)
        const analysis = analyzeActors(program, source, sdk)
        this.throwIfThereAreAnyErrors([
            ...program.getOptionsDiagnostics(),
            ...program.getGlobalDiagnostics(),
            ...program.getSyntacticDiagnostics(),
            ...program.getSemanticDiagnostics(),
            ...analysis.diagnostics
        ])
        return { schemas: analysis.schemas, program, source, sdk }
    }

    private open(entrypoint: string, settings: CompilerOptions) {
        if (!/\.(?:ts|tsx|mts|cts)$/u.test(entrypoint) || /\.d\.[cm]?ts$/u.test(entrypoint))
            throw new ActorDefinitionError("actor entrypoint must be a TypeScript source file")
        if (!this.system.fileExists(entrypoint))
            throw new ActorDefinitionError(`actor entrypoint ${entrypoint} is not a file`)
        const parsed = this.readTSConfiguration(entrypoint, settings.configFile)
        const options = this.tsCompilerOptions(parsed.options)
        const host = this.compilerHost(options)
        const sdkEntrypoint = this.resolveSdk(entrypoint, options, host)
        const declarations = parsed.fileNames.filter(file => /\.d\.[cm]?ts$/u.test(file))
        const files = [...new Set([entrypoint, sdkEntrypoint, ...declarations])]
        return { entrypoint, options, host, files, sdkEntrypoint }
    }

    private readTSConfiguration(entrypoint: string, configured: string | undefined) {
        const configFile = configured ?? ts.findConfigFile(path.dirname(entrypoint), this.system.fileExists)
        const root = configFile ? path.dirname(path.resolve(configFile)) : path.dirname(entrypoint)
        const config = configFile ? ts.readConfigFile(configFile, this.system.readFile) : { config: {} }
        this.throwIfThereAreAnyErrors(config.error ? [config.error] : [])
        const parsed = ts.parseJsonConfigFileContent(config.config, this.system, root)
        this.throwIfThereAreAnyErrors(parsed.errors.filter(error => error.code !== 18003))
        return parsed
    }

    private tsCompilerOptions(configured: ts.CompilerOptions): ts.CompilerOptions {
        if (configured.experimentalDecorators || configured.emitDecoratorMetadata)
            throw new ActorDefinitionError(
                "actor decorators require standard TypeScript decorators; disable experimentalDecorators and emitDecoratorMetadata"
            )
        return {
            target: ts.ScriptTarget.ES2022,
            module: ts.ModuleKind.NodeNext,
            strict: true,
            skipLibCheck: true,
            ...configured,
            allowJs: true,
            checkJs: false,
            noEmit: true
        }
    }

    private resolveSdk(entrypoint: string, options: ts.CompilerOptions, host: ts.CompilerHost) {
        const resolved = ts.resolveModuleName("durable-actors", entrypoint, options, host).resolvedModule
        const sdkEntrypoint =
            resolved?.resolvedFileName ??
            ts.resolveModuleName(fileURLToPath(new URL("../index.js", import.meta.url)), entrypoint, options, host)
                .resolvedModule?.resolvedFileName
        if (!sdkEntrypoint) throw new ActorDefinitionError("cannot resolve the durable-actors SDK")
        return sdkEntrypoint
    }

    private compilerHost(options: ts.CompilerOptions): ts.CompilerHost {
        const host = ts.createCompilerHost(options)
        host.readFile = this.system.readFile
        host.fileExists = this.system.fileExists
        host.directoryExists = this.system.directoryExists
        host.getDirectories = this.system.getDirectories
        host.realpath = this.system.realpath
        host.getCurrentDirectory = this.system.getCurrentDirectory
        host.getSourceFile = (file, version) => {
            const text = host.readFile(file)
            return text === undefined ? undefined : ts.createSourceFile(file, text, version, true)
        }
        return host
    }

    private throwIfThereAreAnyErrors(diagnostics: readonly ts.Diagnostic[]): void {
        const errors = diagnostics.filter(diagnostic => diagnostic.category === ts.DiagnosticCategory.Error)
        if (errors.length > 0)
            throw new ActorDefinitionError(
                ts
                    .formatDiagnostics(errors, {
                        getCurrentDirectory: this.system.getCurrentDirectory,
                        getCanonicalFileName: file => file,
                        getNewLine: () => this.system.newLine
                    })
                    .trim()
            )
    }
}

function analyzeActors(program: ts.Program, entrypoint: ts.SourceFile, sdk: SdkSymbols): ActorAnalysis {
    const checker = program.getTypeChecker()
    const discovered = discoverActors(entrypoint, checker, sdk)
    const actors = discovered.actors.map(actor => readActor(actor, checker, sdk))
    return validateActors(actors, discovered.diagnostics)
}

function discoverActors(source: ts.SourceFile, checker: ts.TypeChecker, sdk: SdkSymbols) {
    const module = checker.getSymbolAtLocation(source)
    const exports = module === undefined ? [] : checker.getExportsOfModule(module)
    const actors: ts.ClassDeclaration[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const exported of exports) {
        if (exported.flags & ts.SymbolFlags.Alias && exported.declarations?.every(isTypeOnlyExport)) continue
        const symbol = canonicalSymbol(checker, exported)
        if (!(symbol.flags & ts.SymbolFlags.Value) || !isActorExport(symbol, checker, sdk)) continue
        const actor = actorDeclaration(exported, checker)
        const error = actorExportError(exported.name, actor, checker, sdk)
        if (error !== undefined) diagnostics.push(definitionDiagnostic(exported.declarations?.[0] ?? source, error))
        else actors.push(actor!)
    }
    if (actors.length === 0 && diagnostics.length === 0)
        diagnostics.push(definitionDiagnostic(source, "actor entrypoint must have named actor exports"))
    return { actors, diagnostics }
}

function isActorExport(symbol: ts.Symbol, checker: ts.TypeChecker, sdk: SdkSymbols): boolean {
    const declaration = symbol.valueDeclaration ?? symbol.declarations?.[0]
    if (declaration === undefined) return false
    const type = checker.getTypeOfSymbolAtLocation(symbol, declaration)
    return type.getConstructSignatures().some(signature => extendsActor(signature.getReturnType(), checker, sdk))
}

function extendsActor(type: ts.Type, checker: ts.TypeChecker, sdk: SdkSymbols): boolean {
    const symbol = type.getSymbol()
    if (symbol === undefined) return false
    const declared = checker.getDeclaredTypeOfSymbol(symbol)
    return (
        declared.getBaseTypes()?.some(base => base.getSymbol() === sdk.Actor || extendsActor(base, checker, sdk)) ??
        false
    )
}

function actorDeclaration(symbol: ts.Symbol, checker: ts.TypeChecker): ts.ClassDeclaration | undefined {
    return canonicalSymbol(checker, symbol).declarations?.find(ts.isClassDeclaration)
}

function actorExportError(
    name: string,
    actor: ts.ClassDeclaration | undefined,
    checker: ts.TypeChecker,
    sdk: SdkSymbols
): string | undefined {
    if (name === "default") return "actor entrypoint must use named exports, not a default export"
    const base = actor?.heritageClauses?.find(clause => clause.token === ts.SyntaxKind.ExtendsKeyword)?.types[0]
    if (actor === undefined || base === undefined || symbolAt(checker, base.expression) !== sdk.Actor)
        return `actor entrypoint export ${name} must be a class that directly extends Actor`
    if (actor.name?.text !== name) return `actor entrypoint export ${name} must have the same class name`
    try {
        validateActorComponent("actor name", name)
    } catch (error) {
        return error instanceof Error ? error.message : String(error)
    }
    if (actor.typeParameters?.length) return `actor class ${name} cannot have type parameters`
    if (actor.modifiers?.some(modifier => modifier.kind === ts.SyntaxKind.AbstractKeyword))
        return `actor class ${name} cannot be abstract`
}

function isTypeOnlyExport(node: ts.Declaration): boolean {
    return ts.isExportSpecifier(node) && (node.isTypeOnly || node.parent.parent.isTypeOnly)
}

function readActor(node: ts.ClassDeclaration, checker: ts.TypeChecker, sdk: SdkSymbols): ParsedActor {
    return {
        name: node.name!.text,
        ...readDecorators(node, checker, sdk),
        members: node.members.map(member => ({ node: member, ...readDecorators(member, checker, sdk) }))
    }
}

function readDecorators(target: ts.Node, checker: ts.TypeChecker, sdk: SdkSymbols): DecoratorResult {
    const annotations: Annotation[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const node of ts.canHaveDecorators(target) ? (ts.getDecorators(target) ?? []) : []) {
        const call = ts.isCallExpression(node.expression) ? node.expression : undefined
        const symbol = symbolAt(checker, call?.expression ?? node.expression)
        const use = { node, target, called: call !== undefined }
        const result = readDecorator(symbol, use, sdk)
        annotations.push(...result.annotations)
        diagnostics.push(...result.diagnostics)
    }
    return { annotations, diagnostics }
}

function readDecorator(symbol: ts.Symbol | undefined, use: DecoratorUse, sdk: SdkSymbols): DecoratorResult {
    if (symbol === undefined) return { annotations: [], diagnostics: [] }
    switch (symbol) {
        case sdk.Persisted:
            return readPersistence(use, Persistence.Persisted)
        case sdk.Ephemeral:
            return readPersistence(use, Persistence.Ephemeral)
        case sdk.Emittable:
            return readEmission(use)
        case sdk.Interleave:
            return readReentrancy(use)
        case sdk.Compute:
            return readCompute(use)
        default:
            return { annotations: [], diagnostics: [] }
    }
}

function validateActors(actors: readonly ParsedActor[], discoveryDiagnostics: readonly ts.Diagnostic[]): ActorAnalysis {
    const diagnostics = [...discoveryDiagnostics]
    const schemas: ActorSchema[] = []
    for (const actor of actors) {
        diagnostics.push(...actor.diagnostics, ...actor.members.flatMap(member => member.diagnostics))
        const persistence = validatePersistence(actor)
        diagnostics.push(...persistence.diagnostics)
        const reentrancy = validateReentrancy(actor)
        diagnostics.push(...reentrancy.diagnostics)
        const compute = validateCompute(actor)
        diagnostics.push(...compute.diagnostics)
        schemas.push({
            actorName: actor.name,
            fields: persistence.fields,
            ...(compute.options === undefined ? {} : { sandbox: compute.options }),
            ...(reentrancy.methods.length ? { reentrantMethods: reentrancy.methods } : {})
        })
    }
    return { schemas: diagnostics.length === 0 ? schemas : [], diagnostics }
}

function resolveSdkSymbols(checker: ts.TypeChecker, source: ts.SourceFile): SdkSymbols {
    const module = checker.getSymbolAtLocation(source)
    const exports = module === undefined ? [] : checker.getExportsOfModule(module)
    const resolve = (name: string) => {
        const symbol = exports.find(candidate => candidate.name === name)
        if (symbol === undefined) throw new ActorDefinitionError(`cannot resolve SDK export ${name}`)
        return canonicalSymbol(checker, symbol)
    }
    const interleave = exports.find(symbol => symbol.name === "Interleave")
    const compute = exports.find(symbol => symbol.name === "Compute")
    return {
        Actor: resolve("Actor"),
        Persisted: resolve("Persisted"),
        Ephemeral: resolve("Ephemeral"),
        Emittable: resolve("Emittable"),
        ...(compute === undefined ? {} : { Compute: canonicalSymbol(checker, compute) }),
        ...(interleave === undefined ? {} : { Interleave: canonicalSymbol(checker, interleave) })
    }
}

function canonicalSymbol(checker: ts.TypeChecker, symbol: ts.Symbol): ts.Symbol {
    return symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol
}

function symbolAt(checker: ts.TypeChecker, node: ts.Node): ts.Symbol | undefined {
    const symbol = checker.getSymbolAtLocation(node)
    return symbol === undefined ? undefined : canonicalSymbol(checker, symbol)
}

function definitionDiagnostic(node: ts.Node, messageText: string, related: readonly ts.Node[] = []): ts.Diagnostic {
    return {
        category: ts.DiagnosticCategory.Error,
        code: 95001,
        file: node.getSourceFile(),
        start: node.getStart(),
        length: node.getWidth(),
        messageText,
        ...(related.length === 0
            ? {}
            : {
                  relatedInformation: related.map(annotation =>
                      definitionDiagnostic(annotation, "persistence annotation declared here")
                  )
              })
    }
}

export { ActorCompiler, analyzeActors, definitionDiagnostic, Persistence, resolveSdkSymbols }
export type { CompilerOptions }
export type {
    ActorApi,
    TypeReference,
    PublicActorContract,
    RpcContract,
    RpcMethod,
    RpcParameter
} from "../wire/public-contract.js"

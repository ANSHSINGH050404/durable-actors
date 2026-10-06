import type ts from "typescript"

import type { ComputeOptions } from "../actor/compute.js"
import { Persistence } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"

interface CompilerOptions {
    readonly configFile?: string
}

interface SdkSymbols {
    readonly Actor: ts.Symbol
    readonly Persisted: ts.Symbol
    readonly Ephemeral: ts.Symbol
    readonly Emittable: ts.Symbol
    readonly Interleave?: ts.Symbol
    readonly Compute?: ts.Symbol
}

enum AnnotationKind {
    Persistence = "persistence",
    Emission = "emission",
    Reentrancy = "reentrancy",
    Compute = "compute"
}

interface PersistenceAnnotation {
    readonly kind: AnnotationKind.Persistence
    readonly mode: Persistence
    readonly node: ts.Decorator
}

type Annotation =
    | PersistenceAnnotation
    | { readonly kind: AnnotationKind.Compute; readonly node: ts.Decorator; readonly options: ComputeOptions }
    | { readonly kind: AnnotationKind.Emission | AnnotationKind.Reentrancy; readonly node: ts.Decorator }

interface DecoratorUse {
    readonly node: ts.Decorator
    readonly target: ts.Node
    readonly called: boolean
}

interface DecoratorResult {
    readonly annotations: readonly Annotation[]
    readonly diagnostics: readonly ts.Diagnostic[]
}

interface ParsedMember extends DecoratorResult {
    readonly node: ts.ClassElement
}

interface ParsedActor extends DecoratorResult {
    readonly name: string
    readonly members: readonly ParsedMember[]
}

interface ActorAnalysis {
    readonly schemas: readonly ActorSchema[]
    readonly diagnostics: readonly ts.Diagnostic[]
}

export { AnnotationKind, Persistence }
export type {
    ActorAnalysis,
    ActorSchema,
    Annotation,
    CompilerOptions,
    DecoratorResult,
    DecoratorUse,
    ParsedActor,
    ParsedMember,
    SdkSymbols
}
export type { ActorFieldSchema } from "../actor/schema.js"

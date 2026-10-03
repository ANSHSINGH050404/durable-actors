import { ActorDefinitionError } from "../errors.js"

/**
 * Lets other calls run during awaits. State may change while waiting.
 * Disables error rollback for the whole actor class. State is saved on successful completion.
 * @experimental
 */
function Interleave(_value: Function, context: ClassMethodDecoratorContext): void {
    if (context.kind !== "method" || context.static || context.private || typeof context.name !== "string")
        throw new ActorDefinitionError("@Interleave requires a public instance async method")
}

/** Saves a field after successful calls. Cannot decorate `#private` fields. */
function Persisted(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Persisted", context)
    if (context.private) throw new ActorDefinitionError("@Persisted cannot decorate a JavaScript private field")
}

/** Marks a temporary field that may reset between calls. */
function Ephemeral(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Ephemeral", context)
}

/** Sends browser state updates for a public `@Persisted` field. */
function Emittable(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Emittable", context)
    if (context.private) throw new ActorDefinitionError("@Emittable requires a public field")
}

function validateField(name: string, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field" || context.static || typeof context.name !== "string")
        throw new ActorDefinitionError(`@${name} requires an instance field with a string name`)
}

export { Emittable, Ephemeral, Interleave, Persisted }

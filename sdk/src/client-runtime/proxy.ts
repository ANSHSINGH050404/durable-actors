import { ActorProtocolError, ActorSerializationError, ActorValidationError } from "./errors.js"
import { isRecord } from "./http.js"
import {
    authorizationHeaders,
    configuredSettings,
    environmentSettings,
    projectActorPath,
    validateActorComponent
} from "./settings.js"

/** Overrides for the backend's DURABLE_ACTORS environment settings. */
interface SocketProxyOptions {
    readonly projectId?: string
    readonly controlPlaneUrl?: string
    readonly apiKey?: string
    /** Defaults to 180000 ms; accepts 1000–600000 ms. */
    readonly setupTimeoutMs?: number
}

interface SocketProxyDependencies {
    readonly fetch?: typeof globalThis.fetch
}

interface ProxyActor<Metadata = unknown> {
    readonly types?: Metadata
}

/** Actor access your backend has already authorized. */
type SocketAuthorization<Actors extends Record<string, ProxyActor>> = {
    [Name in keyof Actors & string]: {
        readonly actorName: Name
        readonly actorId: string
        readonly metadata: Actors[Name] extends ProxyActor<infer Metadata> ? Metadata : never
        readonly homeRegion?: string
        readonly authorizationLifetimeMs?: number
    }
}[keyof Actors & string]

/** Browser connection URL and deadlines in Unix milliseconds. Treat the URL as a credential. */
interface SocketGrant {
    websocketUrl: string
    homeRegion: string
    connectByMs: number
    authorizedUntilMs: number
}

/** Issues browser connection URLs from your backend. */
class SocketProxy<Actors extends Record<string, ProxyActor>> {
    private readonly projectId: string
    private readonly origin: string
    private readonly apiKey: string | undefined
    private readonly setupTimeoutMs: number
    private readonly fetchRequest: typeof globalThis.fetch

    constructor(
        private readonly actors: Actors,
        options: SocketProxyOptions = {},
        dependencies: SocketProxyDependencies = {}
    ) {
        this.setupTimeoutMs = options.setupTimeoutMs ?? 180000
        if (!Number.isSafeInteger(this.setupTimeoutMs) || this.setupTimeoutMs < 1000 || this.setupTimeoutMs > 600000)
            throw new Error("Socket setup timeout must be between one second and ten minutes")
        const settings = configuredSettings(proxySettings(options))
        this.origin = settings.controlPlaneUrl
        this.apiKey = settings.credential
        this.projectId = settings.projectId
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
    }

    /**
     * Call after authenticating the user and checking actor access.
     * Pass `websocketUrl` to the browser's `new WebSocket()`.
     */
    async handle(authorization: SocketAuthorization<Actors>): Promise<SocketGrant> {
        const actorName = validateActorComponent("actor name", authorization.actorName)
        const actorId = validateActorComponent("actor ID", authorization.actorId)
        if (!Object.hasOwn(this.actors, actorName)) throw new Error(`Unknown actor name: ${actorName}`)
        const metadata = socketMetadata(authorization.metadata)
        const authorizationLifetimeMs = authorization.authorizationLifetimeMs ?? 900000
        if (
            !Number.isSafeInteger(authorizationLifetimeMs) ||
            authorizationLifetimeMs < 1000 ||
            authorizationLifetimeMs > 86400000
        )
            throw new Error("Socket authorization lifetime must be between one second and one day")
        const homeRegion =
            authorization.homeRegion === undefined
                ? undefined
                : validateActorComponent("home region", authorization.homeRegion)
        const response = await this.fetchRequest(
            `${this.origin}${projectActorPath(this.projectId, actorName, actorId)}/find-websocket`,
            {
                method: "POST",
                redirect: "manual",
                headers: { ...authorizationHeaders(this.apiKey), "content-type": "application/json" },
                body: JSON.stringify({ metadata, authorizationLifetimeMs, homeRegion }),
                signal: AbortSignal.timeout(this.setupTimeoutMs)
            }
        )
        if (!response.ok) throw new Error(`WebSocket authorization could not be issued (HTTP ${response.status})`)
        return socketGrant(await response.json())
    }
}

function proxySettings(options: SocketProxyOptions) {
    const environment = environmentSettings()
    const controlPlaneUrl = options.controlPlaneUrl ?? environment.controlPlaneUrl
    const apiKey = options.apiKey ?? environment.apiKey
    return {
        projectId: options.projectId ?? environment.projectId,
        controlPlaneUrl: controlPlaneUrl ?? "http://127.0.0.1:7100",
        apiKey
    }
}

export { SocketProxy }
export type { ProxyActor, SocketAuthorization, SocketGrant, SocketProxyDependencies, SocketProxyOptions }

function socketGrant(value: unknown): SocketGrant {
    if (
        !isRecord(value) ||
        typeof value.websocketUrl !== "string" ||
        !URL.canParse(value.websocketUrl) ||
        !/^wss?:$/u.test(new URL(value.websocketUrl).protocol) ||
        typeof value.homeRegion !== "string" ||
        !value.homeRegion ||
        !Number.isSafeInteger(value.connectByMs) ||
        !Number.isSafeInteger(value.authorizedUntilMs)
    )
        throw new ActorProtocolError("invalid WebSocket authorization response")
    return {
        websocketUrl: value.websocketUrl,
        homeRegion: value.homeRegion,
        connectByMs: value.connectByMs as number,
        authorizedUntilMs: value.authorizedUntilMs as number
    }
}

function socketMetadata(value: unknown): unknown {
    let encoded: string
    try {
        encoded = JSON.stringify(value)
        if (!isJsonValue(value)) throw new Error("invalid JSON value")
    } catch (error) {
        throw new ActorSerializationError("socket metadata must be a JSON value", { cause: error })
    }
    if (new TextEncoder().encode(encoded).length > 16 * 1024)
        throw new ActorValidationError("socket metadata exceeds 16384 bytes")
    return value
}

function isJsonValue(value: unknown): boolean {
    if (value === null || typeof value === "string" || typeof value === "boolean") return true
    if (typeof value === "number") return Number.isFinite(value)
    if (Array.isArray(value)) return Array.from(value).every(isJsonValue)
    return (
        isRecord(value) &&
        [Object.prototype, null].includes(Object.getPrototypeOf(value)) &&
        Object.values(value).every(isJsonValue)
    )
}

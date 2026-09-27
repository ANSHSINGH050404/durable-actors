import { HttpActorClient } from "./client.js"
import { ActorSessionRejectedError } from "./errors.js"
import { validateActorComponent, validateOrigin } from "./settings.js"
import type { DurableActorsClientOptions } from "./settings.js"
import type { ActorRpcTransport } from "./stub.js"

export interface ActorSession {
    readonly projectId: string
    readonly controlPlaneUrl: string
    readonly token: string
    readonly expiresAtMs: number
}

export interface ActorSessionTransportOptions {
    readonly projectId: string
    /** Reauthenticate with your application backend. Throw ActorSessionRejectedError on an explicit denial. */
    readonly getSession: () => Promise<ActorSession>
}

export interface ActorSessionTransportDependencies {
    readonly now?: () => number
    readonly createTransport?: (options: DurableActorsClientOptions) => ActorRpcTransport
    readonly schedule?: (callback: () => void, delayMs: number) => () => void
}

export { ActorSessionRejectedError } from "./errors.js"

/**
 * Reuses actor targets and renews runtime-issued sessions through a trusted backend.
 * Sessions require an HTTPS origin (except localhost) and at most 60 seconds of validity.
 * Renewal stops after a minute without invocations; dispose cancels it immediately.
 */
export class ActorSessionTransport implements ActorRpcTransport {
    private transport: ActorRpcTransport | undefined
    private expiresAtMs = 0
    private lastUsedAt = 0
    private pending: Promise<ActorRpcTransport> | undefined
    private cancel: (() => void) | undefined
    private closed = false
    private readonly now: () => number
    private readonly createTransport: (options: DurableActorsClientOptions) => ActorRpcTransport
    private readonly schedule: (callback: () => void, delayMs: number) => () => void

    constructor(
        private readonly options: ActorSessionTransportOptions,
        dependencies: ActorSessionTransportDependencies = {}
    ) {
        validateActorComponent("project ID", options.projectId)
        this.now = dependencies.now ?? (() => performance.timeOrigin + performance.now())
        this.createTransport = dependencies.createTransport ?? (settings => new HttpActorClient(settings))
        this.schedule = dependencies.schedule ?? scheduleRefresh
    }

    async invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        if (this.closed) throw new Error("Actor session transport is disposed")
        this.lastUsedAt = this.now()
        const transport =
            this.transport && this.expiresAtMs > this.now() + 5_000 ? this.transport : await this.refresh()
        return transport.invoke(actorName, actorId, method, args)
    }

    dispose(): void {
        this.closed = true
        this.cancel?.()
        this.transport = undefined
    }

    private refresh(): Promise<ActorRpcTransport> {
        if (this.pending) return this.pending
        this.cancel?.()
        this.pending = this.exchange().finally(() => {
            this.pending = undefined
        })
        return this.pending
    }

    private async exchange(): Promise<ActorRpcTransport> {
        let session: ActorSession
        try {
            session = await this.options.getSession()
        } catch (error) {
            if (error instanceof ActorSessionRejectedError) {
                this.transport = undefined
                this.expiresAtMs = 0
            }
            throw error
        }
        const remaining = this.validateSession(session)
        if (this.closed) throw new Error("Actor session transport is disposed")
        const transport = this.createTransport({
            projectId: session.projectId,
            controlPlaneUrl: session.controlPlaneUrl,
            apiKey: session.token
        })
        this.transport = transport
        this.expiresAtMs = session.expiresAtMs
        this.scheduleRenewal(remaining * (0.75 + Math.random() * 0.05))
        return transport
    }

    private validateSession(session: ActorSession): number {
        if (
            !session ||
            session.projectId !== this.options.projectId ||
            typeof session.token !== "string" ||
            !session.token.trim() ||
            !Number.isSafeInteger(session.expiresAtMs)
        )
            throw new Error("Invalid actor session")
        const origin = new URL(validateOrigin(session.controlPlaneUrl))
        if (origin.protocol !== "https:" && !["localhost", "127.0.0.1", "[::1]"].includes(origin.hostname))
            throw new Error("Actor sessions require HTTPS outside localhost")
        const remaining = session.expiresAtMs - this.now()
        if (remaining <= 5_000 || remaining > 65_000) throw new Error("Actor session is expired or invalid")
        return remaining
    }

    private scheduleRenewal(delayMs: number): void {
        this.cancel = this.schedule(() => {
            if (this.closed || this.now() - this.lastUsedAt >= 60_000) return
            void this.refresh().catch(() => {
                if (!this.closed && this.expiresAtMs > this.now() + 5_000) this.scheduleRenewal(5_000)
            })
        }, delayMs)
    }
}

export function createActorSessionTransport(options: ActorSessionTransportOptions): ActorSessionTransport {
    return new ActorSessionTransport(options)
}

function scheduleRefresh(callback: () => void, delayMs: number): () => void {
    const timer = setTimeout(callback, delayMs)
    ;(timer as unknown as { unref?: () => void }).unref?.()
    return () => clearTimeout(timer)
}

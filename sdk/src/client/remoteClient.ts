import WebSocket from "ws"
import { z } from "zod"

import { projectActorPath, validateActorComponent } from "../actor/identity.js"
import { socketMessage } from "../actor/socket.js"
import type { ActorConnection, ActorSocketMessage } from "../actor/socket.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import { socketMetadata } from "../actor/socketValidation.js"
import type { ActorSchemas } from "../actor/socketValidation.js"
import { HttpActorClient } from "../client-runtime/client.js"
import type { DurableActorsClientOptions, HttpActorClientDependencies } from "../client-runtime/client.js"
import type { ActorHostTarget, ActorInvocation } from "../client-runtime/http.js"
import { ActorInvocationError } from "../errors.js"

import { authorizationHeaders } from "./clientSettings.js"
import { SocketConnection } from "./socketConnection.js"
import { LatencyTimeline, stderrTelemetry } from "./telemetry.js"

class RemoteActorClient extends HttpActorClient {
    private readonly connectWebSocket: WebSocketConnector
    constructor(options?: DurableActorsClientOptions, dependencies: RemoteActorClientDependencies = {}) {
        super(options, {
            ...dependencies,
            telemetry: dependencies.telemetry ?? stderrTelemetry
        })
        this.connectWebSocket = dependencies.connectWebSocket ?? openWebSocket
    }
    async connect(
        actorName: string,
        actorId: string,
        metadata: unknown,
        schemas: ActorSchemas = {}
    ): Promise<ActorConnection> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const timeline = new LatencyTimeline(this.monotonicNow)
        let outcome = "failed"
        try {
            const url = await this.socketGrant(actorName, actorId, metadata, schemas, requestId)
            timeline.mark("grant_received")
            const connection = await this.connectWebSocket(url, schemas, event => {
                if (event === "opened") timeline.mark("socket_opened")
                else
                    this.telemetry({
                        event: "actor_client_socket_first_message",
                        request_id: requestId,
                        actor_name: actorName,
                        actor_id: actorId,
                        ...timeline.finish()
                    })
            })
            outcome = "connected"
            return connection
        } finally {
            this.telemetry({
                event: "actor_client_socket_connect",
                request_id: requestId,
                actor_name: actorName,
                actor_id: actorId,
                outcome,
                ...timeline.finish()
            })
        }
    }

    async broadcast(actorName: string, actorId: string, message: ActorSocketMessage): Promise<void> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const actor = {
            requestId,
            projectId: this.settings.projectId,
            actorName: validateActorComponent("actor name", actorName),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const target = await this.target(actor, new LatencyTimeline(this.monotonicNow))
        await this.deliverSocketEffects(
            target,
            actor,
            [{ type: "broadcast", message: socketMessage(message), except_connection_ids: [], tags: [] }],
            "actor socket broadcast"
        )
    }

    private async socketGrant(
        actorName: string,
        actorId: string,
        metadata: unknown,
        schemas: ActorSchemas,
        requestId: string
    ): Promise<string> {
        const actor = {
            projectId: this.settings.projectId,
            actorName: validateActorComponent("actor name", actorName),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const attachment = socketMetadata(metadata, schemas)
        const response = await this.fetchRequest(
            `${this.settings.controlPlaneUrl}${projectActorPath(this.settings.projectId, actor.actorName, actor.actorId)}/find-websocket`,
            {
                method: "POST",
                headers: { ...authorizationHeaders(this.settings.credential), "content-type": "application/json" },
                body: JSON.stringify({
                    metadata: attachment,
                    homeRegion: this.settings.homeRegion
                }),
                signal: AbortSignal.timeout(180000)
            }
        )
        if (!response.ok)
            throw new ActorInvocationError("unavailable", requestId, `Socket setup returned HTTP ${response.status}`)
        const grant = z
            .object({
                websocketUrl: z.url().refine(url => ["ws:", "wss:"].includes(new URL(url).protocol))
            })
            .parse(await response.json())
        return grant.websocketUrl
    }

    private async deliverSocketEffects(
        target: ActorHostTarget,
        actor: ActorAddress,
        effects: readonly SocketEffect[],
        context: string
    ): Promise<void> {
        try {
            await this.actorHost.publish(target, actor, effects)
        } catch (error) {
            this.targets.delete(`${actor.actorName}\u001f${actor.actorId}`)
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError(
                "outcome_unknown",
                actor.requestId,
                `${context} could not be delivered: ${message}`
            )
        }
    }
}
function openWebSocket(
    url: string,
    schemas: ActorSchemas,
    observe: (event: "opened" | "first_message") => void
): Promise<ActorConnection> {
    const socket = new WebSocket(url)
    const connection = new SocketConnection(socket, schemas, observe)
    return new Promise((resolve, reject) => {
        let opened = false
        socket.addEventListener(
            "open",
            () => {
                opened = true
                resolve(connection)
            },
            { once: true }
        )
        socket.addEventListener("error", () => {
            if (!opened) reject(new Error("actor WebSocket connection failed"))
        })
        socket.addEventListener("close", () => {
            if (!opened) reject(new Error("actor WebSocket closed before connecting"))
        })
    })
}

interface RemoteActorClientDependencies extends HttpActorClientDependencies {
    readonly connectWebSocket?: WebSocketConnector
}
type WebSocketConnector = (
    url: string,
    schemas: ActorSchemas,
    observe: (event: "opened" | "first_message") => void
) => Promise<ActorConnection>
type ActorAddress = Pick<ActorInvocation, "requestId" | "projectId" | "actorName" | "actorId">
export { RemoteActorClient }
export type { DurableActorsClientOptions, RemoteActorClientDependencies }

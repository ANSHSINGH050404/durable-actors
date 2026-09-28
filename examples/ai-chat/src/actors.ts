import type { UIMessage } from "ai"
import { Actor, Persisted } from "durable-actors"

export class ChatHistory extends Actor {
    @Persisted private messages: UIMessage[] = []

    async load() {
        return this.messages
    }

    async append(...messages: UIMessage[]) {
        this.messages.push(...messages)
    }
}

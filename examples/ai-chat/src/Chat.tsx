import { createRoot } from "react-dom/client"

import { useChat } from "@ai-sdk/react"
import type { UIMessage } from "ai"

const initialMessages: UIMessage[] = await fetch("/api/chat/lobby").then(response => response.json())

function Chat() {
    const { messages, sendMessage, regenerate, status, error } = useChat({ id: "lobby", messages: initialMessages })
    const busy = status === "submitted" || status === "streaming"

    return (
        <main>
            <h1>AI chat</h1>
            <ul role="log" aria-label="Messages">
                {messages.map(message => (
                    <li key={message.id}>
                        <strong>{message.role}:</strong> {message.parts.map(part => (part.type === "text" ? part.text : "")).join("")}
                    </li>
                ))}
            </ul>
            <form
                action={async form => {
                    await sendMessage({ text: String(form.get("message")) })
                }}
            >
                <label>
                    Message <input name="message" required disabled={busy} />
                </label>
                <button disabled={busy}>{busy ? "Thinking…" : "Send"}</button>
            </form>
            {error && (
                <p role="alert">
                    {error.message}{" "}
                    <button onClick={() => regenerate()} disabled={busy}>
                        Retry
                    </button>
                </p>
            )}
        </main>
    )
}

createRoot(document.getElementById("root")!).render(<Chat />)

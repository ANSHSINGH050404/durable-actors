# Examples

Each example includes an app and a local actor runtime:

| Example | What it demonstrates | Create a project |
| --- | --- | --- |
| [AI chat](ai-chat/README.md) | Streaming AI replies and saved conversations | `npx durable-actors init my-ai-chat --template ai-chat` |
| [Chatroom](chat/README.md) | Shared chat over WebSockets | `npx durable-actors init my-chat --template chat` |
| [Collaborative documents](documents/README.md) | Shared document editing | `npx durable-actors init my-documents --template documents` |

Follow the example's README to install dependencies, copy `.env.example` to `.env`, and start its two processes. AI chat also requires an OpenAI API key.

## Run the examples together

Each app needs its own app port and actor runtime port. Set these values in each project's `.env`:

| Example | `PORT` | `DURABLE_ACTORS_PORT` | `DURABLE_ACTORS_CONTROL_PLANE_URL` |
| --- | --- | --- | --- |
| AI chat | `3000` | `7100` | `http://127.0.0.1:7100` |
| Chatroom | `3001` | `7101` | `http://127.0.0.1:7101` |
| Collaborative documents | `3002` | `7102` | `http://127.0.0.1:7102` |

The control-plane URL must match that project's actor runtime port. Leave each project in its own directory so its `.durable-actors/` state is separate. If you configure `DURABLE_ACTORS_DATA_DIR`, use a different directory for each project.

In each project, run `npm run dev:actors`, wait for `Ready`, then run `npm run dev` in a second terminal. Open the corresponding app port above.

If a file/watch limit disables automatic actor reload, the runtime continues serving requests. Restart `npm run dev:actors` after source changes. You can also opt out of watching with `npm run dev:actors -- --no-watch`.

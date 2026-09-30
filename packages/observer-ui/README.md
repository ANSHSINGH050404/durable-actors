# Durable Actors Observer

React 19 components for embedding Durable Actors observability in a hosted app.

```tsx
import { HttpObserverClient, Overview, WebSocketObserver } from "durable-actors-observer"
import "durable-actors-observer/styles.css"
import "durable-actors-observer/theme.css"

const projectId = "my-project"
const client = new HttpObserverClient(`/api/projects/${encodeURIComponent(projectId)}/observe`)

export function Observability() {
    return (
        <>
            <Overview client={client} onSelectActor={actorName => console.log(actorName)} />
            <WebSocketObserver client={client} />
        </>
    )
}
```

Import `styles.css` once. The optional `theme.css` supplies light and dark theme tokens; omit it when the host supplies those tokens. Apply the `dark` class to an ancestor for dark mode.

The package exports `ActorObserver`, `RequestObserver`, `Overview`, `WebSocketObserver`, `ConsoleApp`, `RequestTimeline`, `SocketTimeline`, `TimeRangePicker`, and `FilterCombobox`, along with their named props types. It also exports the shared badge, button, calendar, command, input, popover, sheet, and table components from the package root. Use React's `ComponentProps<typeof Button>` (and equivalent) for primitive props.

Pass your own `ObserverClient` to connect these views to a hosted backend, or configure `HttpObserverClient` with your API prefix and fetch implementation. React remains a peer dependency. `ConsoleApp` provides the complete navigation shell and requires a `toggleTheme` callback; individual views can be placed inside your own navigation.

Control-plane endpoints require a project in the path: `/v1/projects/{project_id}/observe/...`. The hosted backend should authorize access to that project and proxy the routes using its server-side admin credential. The CLI's `/api/observe` proxy uses its configured `DURABLE_ACTORS_PROJECT_ID`. Request traces include a required `projectId`; older stored traces without one are excluded from project results.

The observer version follows the SDK and runtime version. Run `pnpm release:prepare <version>` at the repository root before publishing a GitHub release. Release preflight checks all versions, and the workflow publishes the observer before the SDK. The SDK's `workspace:*` dependency becomes the exact observer version when packed.

The localhost-only `durable-actors dev` runtime allows observability requests without a secret, even when a secret is configured for application routes. Hosted runtimes retain their configured admin authentication.

## Invocation waterfall

`RequestObserver` opens with a waterfall of retained method calls and WebSocket events, including inside an actor instance. Instance inspection combines identity, residency, and queue metrics in a compact header; Requests, State, and WebSockets views preserve inspection controls when switching. Each row shares a relative time axis and shows total duration, queue wait, and the gap or overlap with preceding calls on that instance. Select a row to inspect identifiers, outcomes, and exact timings; switch to Table for the tabular view. Live updates, pause, history filters, and loading older requests apply to both views.

Durations include queue wait, actor processing, and persistence. Gaps are calculated only from the calls loaded in the current view; filtered or unretained calls are not included. The waterfall uses request traces, so internal helper calls within one invocation are not separate spans. Embed `RequestTimeline` directly with `records` and an `onSelect(record, trigger)` callback when providing your own request controls.

## Persisted actor state

Select an actor instance in `ActorObserver` to inspect persisted fields, JSON types, nested values, and version diffs. Request and connection links filter the existing request history. The standard `HttpObserverClient` and `durable-actors observe` include this capability; custom clients implement `getState` and `listStateHistory` to enable it.

Values come from the actor's existing snapshot objects in the configured storage bucket, including dormant actors. Analytics stores only a state-version signal. Request events trigger a refresh; polling recovers from missed events and asynchronous bucket uploads. History uses rebuildable metadata-only sidecars beside snapshots and retains no duplicate state bodies. Available versions follow the bucket's snapshot retention/lifecycle policy.

Attribution identifies the committing request. A reentrant commit may include mutations from other interleaved requests; the UI labels this explicitly. Declared field types describe the current deployment schema, which may differ from older stored values. Inspection is read-only and uses the existing authenticated project observability API.

import { useEffect, useMemo, useRef, useState } from "react"

import { RefreshCw } from "lucide-react"

import { ConnectionInventoryNotice } from "./ConnectionInventoryNotice.js"
import { FilterCombobox } from "./FilterCombobox.js"
import type { FilterSuggestion } from "./FilterCombobox.js"
import { SocketTimeline, durationLabel, shortId, statusLabel } from "./SocketTimeline.js"
import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ActorInventory, ObserverClient } from "./client.js"
import { Button } from "./components/ui/button.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Sheet, SheetContent, SheetDescription, SheetTitle } from "./components/ui/sheet.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useInventory } from "./observer-hooks.js"
import { useSocketHistory } from "./socket-history.js"
import { connectionKey, formatDuration, sessionDuration, sessionSummary, socketSessions } from "./socket-sessions.js"
import type { SocketSession, SocketSessionStatus } from "./socket-sessions.js"
import { defaultTimeRange, rangeLabel, rangePhrase, resolveRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

export interface WebSocketObserverProps {
    client: ObserverClient
    onSelectActor?: (actorName: string) => void
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

export function WebSocketObserver({ client, onSelectActor, timeRange, onTimeRangeChange }: WebSocketObserverProps) {
    const { inventory, failed, retry } = useInventory(client)
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const setRange = onTimeRangeChange ?? setLocalRange
    const [now, setNow] = useState(Date.now)
    const [query, setQuery] = useState("")
    const [status, setStatus] = useState<SocketSessionStatus | "all">("all")
    const [selected, setSelected] = useState<SocketSession>()
    const container = useRef<HTMLElement>(null)
    const trigger = useRef<HTMLElement | null>(null)
    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 5_000)
        return () => clearInterval(timer)
    }, [])
    const resolved = resolveRange(range, now)
    const end = resolved.toMs ?? now
    const history = useSocketHistory(client, resolved)
    const firstSeen = useFirstSeenConnections(inventory, history.retry)
    const sessions = useMemo(() => socketSessions(history.rows ?? [], inventory, firstSeen), [history.rows, inventory, firstSeen])
    const ready = !!inventory || failed
    const needle = query.trim().toLocaleLowerCase()
    const visible = sessions
        .filter(
            session =>
                (status === "all" || session.status === status) &&
                (!needle || `${session.connectionId} ${session.actorName} ${session.actorId} ${session.hostId ?? ""}`.toLocaleLowerCase().includes(needle))
        )
        .sort((a, b) => Number(b.status === "open") - Number(a.status === "open") || (b.openedAtMs ?? b.lastSeenMs ?? Infinity) - (a.openedAtMs ?? a.lastSeenMs ?? Infinity))
    const summary = sessionSummary(sessions, now)
    const longest = Math.max(0, ...sessions.filter(session => session.status !== "open").map(session => sessionDuration(session, now)?.ms ?? 0)) || summary.p95
    const timelineStart = timelineOrigin(visible, resolved.fromMs, end)
    const suggestions = useMemo(() => filterSuggestions(sessions, inventory), [sessions, inventory])
    useEffect(() => setSelected(undefined), [client, range])
    return (
        <section ref={container} className="la-observer overview websockets" aria-label="WebSocket observer">
            <div className="overview-heading">
                <div>
                    <h1>WebSockets</h1>
                    <p>
                        {ready
                            ? `${summary.open.toLocaleString()} open now${history.supported ? ` · ${summary.total.toLocaleString()} ${summary.total === 1 ? "session" : "sessions"} ${rangePhrase(range)}` : ""}`
                            : "Connecting to inventory…"}
                    </p>
                </div>
                <div className="overview-controls">
                    <span className="overview-updated">
                        {history.failed ? (
                            "History reconnecting…"
                        ) : history.updatedAt ? (
                            <>
                                Updated <time dateTime={new Date(history.updatedAt).toISOString()}>{new Date(history.updatedAt).toLocaleTimeString([], { hour12: false })}</time>
                            </>
                        ) : null}
                    </span>
                    {history.supported && <TimeRangePicker value={range} onChange={setRange} className="overview-range" />}
                    <Button
                        variant="outline"
                        size="icon"
                        aria-label="Refresh connections"
                        onClick={() => {
                            retry()
                            history.retry()
                        }}
                    >
                        <RefreshCw aria-hidden="true" />
                    </Button>
                </div>
            </div>
            {failed && (
                <p role="alert" className="overview-alert">
                    Could not refresh connections.{" "}
                    {inventory ? "Showing the last received inventory; it may be out of date." : "Open connections cannot be confirmed, so unfinished sessions show as lost."} Retrying automatically.
                </p>
            )}
            {history.failed && (
                <p role="alert" className="overview-alert">
                    Connection history unavailable. {history.rows ? "Showing the last loaded sessions." : "Only currently open connections are shown."} Retrying automatically.
                </p>
            )}
            <SummaryTiles summary={summary} ready={ready} history={history.supported && !!history.rows} range={range} />
            <ConnectionInventoryNotice inventory={inventory} />
            <div className="socket-filterbar">
                <FilterCombobox label="Filter connections" placeholder="Filter by connection, actor, instance, or host…" value={query} onChange={setQuery} suggestions={suggestions} />
                <NativeSelect aria-label="Filter by status" size="sm" value={status} onChange={event => setStatus(event.target.value as SocketSessionStatus | "all")}>
                    <NativeSelectOption value="all">All statuses</NativeSelectOption>
                    <NativeSelectOption value="open">Open</NativeSelectOption>
                    <NativeSelectOption value="closed">Closed</NativeSelectOption>
                    <NativeSelectOption value="lost">Lost</NativeSelectOption>
                    <NativeSelectOption value="unknown">Unknown</NativeSelectOption>
                </NativeSelect>
            </div>
            {history.supported && (
                <section className="overview-panel socket-timeline-panel" aria-label="Connection timeline">
                    <div className="socket-panel-heading">
                        <h2>
                            Timeline <span>{visible.length.toLocaleString()}</span>
                        </h2>
                        <ul className="socket-legend" aria-label="Timeline legend">
                            <li>
                                <i className="socket-swatch socket-swatch-open" /> Open
                            </li>
                            <li>
                                <i className="socket-swatch socket-swatch-closed" /> Closed
                            </li>
                            <li>
                                <i className="socket-swatch socket-swatch-lost" /> Lost · no disconnect recorded
                            </li>
                        </ul>
                    </div>
                    {visible.length ? (
                        <SocketTimeline
                            sessions={visible}
                            start={timelineStart}
                            end={end}
                            endLabel={resolved.toMs === undefined ? "now" : new Date(resolved.toMs).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })}
                            selected={selected}
                            onSelect={(session, element) => {
                                trigger.current = element
                                setSelected(session)
                            }}
                        />
                    ) : (
                        <div className="overview-empty socket-timeline-empty" role="status">
                            <strong>{emptyTitle(ready, history, sessions.length)}</strong>
                            <p>{sessions.length ? "Try another filter or a wider time window." : "Sessions appear here as clients connect to actors."}</p>
                        </div>
                    )}
                </section>
            )}
            <div className="overview-panel">
                <Table
                    className="overview-table socket-table"
                    aria-label="WebSocket connections"
                    containerProps={{ className: "overview-table-scroll", role: "region", "aria-label": "WebSocket connections", tabIndex: 0 }}
                >
                    <TableHeader>
                        <TableRow>
                            <TableHead scope="col">Status</TableHead>
                            <TableHead scope="col">Connection</TableHead>
                            <TableHead scope="col">Actor class</TableHead>
                            <TableHead scope="col">Instance</TableHead>
                            <TableHead scope="col">Opened</TableHead>
                            <TableHead scope="col">Duration</TableHead>
                            <TableHead scope="col">Messages</TableHead>
                            <TableHead scope="col">Metadata</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {visible.map(session => (
                            <SessionRow
                                key={JSON.stringify([session.actorName, session.actorId, session.connectionId])}
                                session={session}
                                now={now}
                                longest={longest}
                                selected={selected === session}
                                onSelect={element => {
                                    trigger.current = element
                                    setSelected(session)
                                }}
                            />
                        ))}
                    </TableBody>
                </Table>
                {!visible.length && (
                    <div className="overview-empty" role="status">
                        <strong>{emptyTitle(ready, history, sessions.length)}</strong>
                        <p>{sessions.length ? "Try another connection, actor, or status." : "Open an actor WebSocket to see its connection and metadata here."}</p>
                        {!!sessions.length && (
                            <Button
                                variant="outline"
                                onClick={() => {
                                    setQuery("")
                                    setStatus("all")
                                }}
                            >
                                Clear filter
                            </Button>
                        )}
                    </div>
                )}
            </div>
            <div className="overview-data-scope">
                <p>
                    {history.supported
                        ? `Sessions pair each connection’s onConnect and onDisconnect events from retained request history${history.capped ? "; only the 500 most recent sessions are shown" : ""}. Open connections follow the latest host report. Lost connections have no recorded disconnect and are no longer reported by a host, so their duration is a lower bound.`
                        : "Connections are active WebSockets, not unique people. Counts follow the latest host report."}
                </p>
            </div>
            <Sheet
                open={!!selected}
                onOpenChange={open => {
                    if (!open) setSelected(undefined)
                }}
            >
                <SheetContent
                    container={container.current}
                    onCloseAutoFocus={event => {
                        event.preventDefault()
                        trigger.current?.focus()
                    }}
                >
                    <SheetTitle>Connection details</SheetTitle>
                    <SheetDescription>Identifiers, timings, and metadata for this WebSocket.</SheetDescription>
                    {selected && <SessionDetails session={selected} now={now} onSelectActor={onSelectActor} />}
                </SheetContent>
            </Sheet>
        </section>
    )
}

function SummaryTiles({ summary, ready, history, range }: { summary: ReturnType<typeof sessionSummary>; ready: boolean; history: boolean; range: TimeRange }) {
    return (
        <div className="overview-metrics socket-metrics">
            <section className="overview-metric">
                <h2>Open now</h2>
                <div className="overview-value">
                    <strong aria-label="Open WebSocket connections">{ready ? summary.open.toLocaleString() : "—"}</strong>
                    <span>live connections</span>
                </div>
                <div className="overview-metric-foot">
                    <span>Latest gateway report</span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Sessions</h2>
                <div className="overview-value">
                    <strong aria-label="WebSocket sessions">{history ? summary.total.toLocaleString() : "—"}</strong>
                    <span>{range.kind === "absolute" ? rangeLabel(range) : rangePhrase(range)}</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history ? summary.closed.toLocaleString() : "—"}</b> closed
                    </span>
                    <span>
                        <b>{history ? summary.lost.toLocaleString() : "—"}</b> lost
                    </span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Session duration</h2>
                <div className="overview-value">
                    <strong aria-label="Median session duration">{history && summary.median !== null ? formatDuration(summary.median) : "—"}</strong>
                    <span>median</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history && summary.p95 !== null ? formatDuration(summary.p95) : "—"}</b> p95
                    </span>
                    <span>Open sessions count up to now</span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Messages</h2>
                <div className="overview-value">
                    <strong aria-label="WebSocket messages">{history ? summary.messages.toLocaleString() : "—"}</strong>
                    <span>received</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history && summary.messagesPerSession !== null ? summary.messagesPerSession.toLocaleString(undefined, { maximumFractionDigits: 1 }) : "—"}</b> per session
                    </span>
                </div>
            </section>
        </div>
    )
}

function SessionRow({ session, now, longest, selected, onSelect }: { session: SocketSession; now: number; longest: number | null; selected: boolean; onSelect: (element: HTMLElement) => void }) {
    const duration = sessionDuration(session, now)
    const share = duration && longest ? Math.min(1, duration.ms / longest) : 0
    return (
        <TableRow className="la-clickable-row" data-state={selected ? "selected" : undefined} onClick={event => onSelect(event.currentTarget.querySelector("button") ?? event.currentTarget)}>
            <TableCell>
                <span className={`socket-status socket-status-${session.status}`}>
                    <i className={`socket-swatch socket-swatch-${session.status}`} aria-hidden="true" />
                    {statusLabel(session.status)}
                </span>
            </TableCell>
            <TableCell>
                <button type="button" className="socket-connection" title={session.connectionId} aria-label={`Inspect connection ${session.connectionId}`}>
                    {shortId(session.connectionId)}
                </button>
            </TableCell>
            <TableCell title={session.actorName}>{session.actorName}</TableCell>
            <TableCell title={session.actorId}>{session.actorId}</TableCell>
            <TableCell>
                {session.openedAtMs === null ? (
                    <span title="The connect event is not in retained history">—</span>
                ) : (
                    <time
                        dateTime={new Date(session.openedAtMs).toISOString()}
                        title={session.estimatedStart ? "Approximate: first reported by the host; the connect event has not been saved yet" : new Date(session.openedAtMs).toLocaleString()}
                    >
                        {session.estimatedStart ? "≈ " : ""}
                        {new Date(session.openedAtMs).toLocaleTimeString([], { hour12: false })}
                    </time>
                )}
            </TableCell>
            <TableCell>
                <span className="socket-duration">
                    <span>{durationLabel(session, now)}</span>
                    <i className={`socket-duration-bar socket-duration-${session.status}`} aria-hidden="true">
                        <b style={{ width: `${share * 100}%` }} />
                    </i>
                </span>
            </TableCell>
            <TableCell>{session.messages.toLocaleString()}</TableCell>
            <TableCell>
                {session.metadata === undefined ? (
                    <span title="No metadata was recorded for this connection">—</span>
                ) : (
                    <code className="socket-metadata" title={JSON.stringify(session.metadata)}>
                        {JSON.stringify(session.metadata)}
                    </code>
                )}
            </TableCell>
        </TableRow>
    )
}

function SessionDetails({ session, now, onSelectActor }: { session: SocketSession; now: number; onSelectActor?: (actorName: string) => void }) {
    const fields: Record<string, string> = {
        Status: session.status === "lost" ? "Lost — no disconnect recorded" : statusLabel(session.status),
        ...(session.status === "unknown" ? { Inventory: "Gateway unavailable; connection status unconfirmed" } : {}),
        Connection: session.connectionId,
        "Actor class": session.actorName,
        "Instance ID": session.actorId,
        Opened: session.openedAtMs === null ? "Before retained history" : `${session.estimatedStart ? "≈ " : ""}${new Date(session.openedAtMs).toLocaleString()}`,
        ...(session.closedAtMs !== null ? { Closed: new Date(session.closedAtMs).toLocaleString() } : {}),
        ...(session.status === "lost" && session.lastSeenMs !== null ? { "Last activity": new Date(session.lastSeenMs).toLocaleString() } : {}),
        Duration: durationLabel(session, now),
        Messages: session.messages.toLocaleString(),
        Failures: session.failures.toLocaleString(),
        Host: session.hostId ?? "Unknown"
    }
    return (
        <>
            <dl className="la-request-details">
                {Object.entries(fields).map(([label, value]) => (
                    <div key={label}>
                        <dt>{label}</dt>
                        <dd>{value}</dd>
                    </div>
                ))}
            </dl>
            {session.metadata !== undefined && (
                <details className="socket-metadata-details" open>
                    <summary>Metadata</summary>
                    <pre>{JSON.stringify(session.metadata, null, 2)}</pre>
                </details>
            )}
            {onSelectActor && (
                <Button variant="outline" onClick={() => onSelectActor(session.actorName)}>
                    Open {session.actorName}
                </Button>
            )}
        </>
    )
}

// Zoom the axis to the sessions in view so short-lived connections stay visible in a wide window.
function timelineOrigin(sessions: SocketSession[], windowStart: number | undefined, now: number) {
    const earliest = Math.min(...sessions.map(session => session.openedAtMs ?? session.lastSeenMs ?? now))
    const origin = Number.isFinite(earliest) ? earliest - Math.max(5_000, (now - earliest) * 0.06) : now - 60_000
    return Math.max(windowStart ?? -Infinity, Math.min(origin, now - 10_000))
}

// Connections already present in the first snapshot keep an unknown start; ones that appear later
// started roughly when they appeared. A changed connection set also refreshes history right away.
function useFirstSeenConnections(inventory: ActorInventory | undefined, refresh: () => void) {
    const [firstSeen, setFirstSeen] = useState<ReadonlyMap<string, number>>(new Map())
    const previous = useRef<Set<string>>(undefined)
    useEffect(() => {
        if (!inventory) return
        const current = new Set(
            inventory.actors.flatMap(actor => actor.instances.flatMap(instance => instance.connections.map(connection => connectionKey(actor.actorName, instance.actorId, connection.id))))
        )
        const before = previous.current
        previous.current = current
        if (!before) return
        const added = [...current].filter(key => !before.has(key))
        const removed = [...before].filter(key => !current.has(key))
        if (!added.length && !removed.length) return
        if (added.length) {
            const now = Date.now()
            setFirstSeen(seen => new Map([...seen, ...added.map(key => [key, now] as const)]))
        }
        refresh()
    }, [inventory])
    return firstSeen
}

function filterSuggestions(sessions: SocketSession[], inventory: ActorInventory | undefined): FilterSuggestion[] {
    const actors = new Map<string, Set<string>>()
    for (const actor of inventory?.actors ?? []) actors.set(actor.actorName, new Set(actor.instances.map(instance => instance.actorId)))
    for (const session of sessions) {
        const instances = actors.get(session.actorName) ?? new Set<string>()
        actors.set(session.actorName, instances.add(session.actorId))
    }
    return [
        ...[...actors.keys()].map(actorName => ({
            group: "Actor class",
            value: actorName,
            hint: `${actors.get(actorName)!.size.toLocaleString()} ${actors.get(actorName)!.size === 1 ? "instance" : "instances"}`
        })),
        ...[...actors].flatMap(([actorName, instances]) => [...instances].map(actorId => ({ group: "Instances", value: actorId, hint: actorName })))
    ]
}

function emptyTitle(ready: boolean, history: { supported: boolean; rows?: unknown; failed: boolean }, total: number) {
    if (!ready && !history.rows) return history.failed ? "Connections unavailable" : "Loading connections…"
    if (total) return "No matching connections"
    return history.supported ? "No WebSocket sessions" : "No active WebSockets"
}

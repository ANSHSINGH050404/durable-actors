import { useEffect, useId, useRef, useState } from "react"

import { Box, CircleHelp, RefreshCw, Search, Unplug } from "lucide-react"

import { ConnectionInventoryNotice } from "./ConnectionInventoryNotice.js"
import { FilterCombobox } from "./FilterCombobox.js"
import type { FilterSuggestion } from "./FilterCombobox.js"
import { RequestObserver } from "./RequestObserver.js"
import { StateInspector } from "./StateInspector.js"
import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ActorInstance, ActorInventory, ObserverClient } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useInventory } from "./observer-hooks.js"
import { useQueueWaits } from "./queue-wait-history.js"
import { formatWait, queueWaitByActor, queueWaitByInstance, queueWaitTotal } from "./queue-wait.js"
import type { QueueWaitStats } from "./queue-wait.js"
import { defaultTimeRange, rangePhrase, resolveRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

interface ActorObserverProps {
    client: ObserverClient
    className?: string
    initialActorName?: string
    navigation?: { actorName?: string; onSelectActor: (actorName?: string) => void }
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

function ActorObserver({ client, className = "", initialActorName, navigation, timeRange, onTimeRangeChange }: ActorObserverProps) {
    const { inventory, loading, failed, retry } = useInventory(client)
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const setRange = onTimeRangeChange ?? setLocalRange
    const [localActorName, setLocalActorName] = useState(initialActorName)
    const selectedActorName = navigation ? navigation.actorName : localActorName
    const setSelectedActorName = navigation ? navigation.onSelectActor : setLocalActorName
    const [selectedInstanceId, setSelectedInstanceId] = useState<string>()
    const [query, setQuery] = useState("")
    const heading = useRef<HTMLHeadingElement>(null)
    const selection = JSON.stringify([selectedActorName, selectedInstanceId])
    const previousSelection = useRef(JSON.stringify([initialActorName, undefined]))
    const detailsId = useId()
    const selectedActor = inventory?.actors.find(actor => actor.actorName === selectedActorName)
    const selectedInstance = selectedActor?.instances.find(instance => instance.actorId === selectedInstanceId)
    const waits = useQueueWaits(client, resolveRange(range, Date.now()), selectedActorName)
    const waitsByActor = waits.rows && queueWaitByActor(waits.rows)
    const waitsByInstance = waits.rows && selectedActorName !== undefined ? queueWaitByInstance(waits.rows, selectedActorName) : undefined
    const totalWait = waits.supported ? (waits.rows ? queueWaitTotal(waits.rows) : undefined) : undefined
    useEffect(() => {
        setLocalActorName(initialActorName)
        setSelectedInstanceId(undefined)
        setQuery("")
    }, [client, initialActorName])
    useEffect(() => {
        if (previousSelection.current !== selection) heading.current?.focus()
        previousSelection.current = selection
    }, [selection])
    const selectActor = (actorName?: string) => {
        setSelectedInstanceId(undefined)
        setSelectedActorName(actorName)
    }
    const selectInstance = (actorName: string, actorId: string) => {
        setSelectedInstanceId(actorId)
        setSelectedActorName(actorName)
    }
    return (
        <section className={`la-observer ${selectedInstanceId !== undefined ? "la-observer-instance-page" : ""} ${className}`} aria-label="Actor observer">
            {selectedActorName !== undefined && (
                <nav className="la-observer-breadcrumb" aria-label="Breadcrumb">
                    <Button variant="link" type="button" aria-label="Back to actors" onClick={() => selectActor(undefined)}>
                        Actors
                    </Button>
                    <span aria-hidden="true">/</span>
                    {selectedInstanceId === undefined ? (
                        <span aria-current="page">{selectedActorName}</span>
                    ) : (
                        <Button variant="link" aria-label="Back to instances" onClick={() => setSelectedInstanceId(undefined)}>
                            {selectedActorName}
                        </Button>
                    )}
                    {selectedInstanceId !== undefined && (
                        <>
                            <span aria-hidden="true">/</span>
                            <span aria-current="page">{selectedInstanceId}</span>
                        </>
                    )}
                </nav>
            )}
            <div className="la-observer-toolbar">
                <div>
                    <div className="la-observer-title">
                        <h1 ref={heading} tabIndex={-1}>
                            {selectedInstanceId ?? selectedActorName ?? "Actors"}
                        </h1>
                        {selectedInstance && (
                            <Badge variant="outline" className={`la-observer-status-${selectedInstance.status}`}>
                                <span className={`la-observer-dot la-observer-dot-${selectedInstance.status}`} />
                                {labelStatus(selectedInstance.status)}
                            </Badge>
                        )}
                    </div>
                    {selectedInstanceId === undefined && (
                        <p>{selectedActorName === undefined ? "Inspect instances, residency, and connections." : "Inspect this actor class’s instances, residency, and connections."}</p>
                    )}
                </div>
                <div className="la-observer-actions">
                    {waits.supported && <TimeRangePicker value={range} onChange={setRange} />}
                    <Button variant="outline" type="button" disabled={loading} onClick={retry}>
                        <RefreshCw aria-hidden="true" className={loading ? "la-observer-spin" : undefined} />
                        {loading ? "Refreshing…" : failed ? "Try again" : "Refresh"}
                    </Button>
                </div>
            </div>
            {failed && (
                <div className="la-observer-error" role="alert">
                    <CircleHelp aria-hidden="true" />
                    <p>{inventory ? "Could not refresh. These counts may be out of date." : "Could not load actors. Check your connection and access, then try again."}</p>
                </div>
            )}
            {!inventory && !failed && <InventorySkeleton />}
            <ConnectionInventoryNotice inventory={inventory} />
            {inventory && (
                <>
                    {selectedActorName !== undefined ? (
                        selectedActor ? (
                            <>
                                {selectedInstanceId === undefined && (
                                    <InventorySummary
                                        inventory={{ ...inventory, actors: [selectedActor] }}
                                        actorName={selectedActorName}
                                        queueWait={waits.supported ? (totalWait ?? null) : undefined}
                                        range={range}
                                    />
                                )}
                                <ActorInstances
                                    client={client}
                                    key={selectedActorName}
                                    id={detailsId}
                                    actorName={selectedActorName}
                                    instances={selectedActor.instances}
                                    waits={waits.supported ? waitsByInstance : undefined}
                                    range={range}
                                    selectedInstanceId={selectedInstanceId}
                                    onSelectInstance={setSelectedInstanceId}
                                />
                            </>
                        ) : (
                            <EmptyState title="Actor class unavailable" description="This actor class is no longer in the latest inventory. Return to Actors to see available classes." />
                        )
                    ) : (
                        <>
                            <InventorySummary inventory={inventory} queueWait={waits.supported ? (totalWait ?? null) : undefined} range={range} />
                            {inventory.actors.length ? (
                                <ActorTable
                                    inventory={inventory}
                                    query={query}
                                    onQueryChange={setQuery}
                                    onSelectActor={selectActor}
                                    onSelectInstance={selectInstance}
                                    waits={waits.supported ? waitsByActor : undefined}
                                />
                            ) : (
                                <EmptyState title="No actors yet" description="Deploy your actor classes to see them here. Instance counts appear as actors are used." />
                            )}
                        </>
                    )}
                    <div className="la-observer-footnote">
                        <span className="la-observer-refresh-status">
                            <span className={`la-observer-dot ${failed ? "la-observer-dot-unknown" : client.watchActors ? "la-observer-dot-live" : ""}`} />
                            {failed ? "Reconnecting…" : client.watchActors ? "Live updates" : "Auto-refresh every 5s"}
                        </span>
                        <span>{client.watchActors ? "Changes stream from the control plane." : "Counts follow the latest host heartbeat."}</span>
                    </div>
                </>
            )}
        </section>
    )
}

function InventorySkeleton() {
    return (
        <div className="la-observer-skeleton" role="status" aria-label="Loading actors">
            <span className="la:sr-only">Loading actors…</span>
            <div />
            <div />
            <div />
        </div>
    )
}

function InventorySummary({ inventory, actorName, queueWait, range }: { inventory: ActorInventory; actorName?: string; queueWait?: QueueWaitStats | null; range: TimeRange }) {
    const totals = inventory.actors.reduce((sum, actor) => ({ live: sum.live + actor.live, dormant: sum.dormant + actor.dormant, unknown: sum.unknown + actor.unknown }), {
        live: 0,
        dormant: 0,
        unknown: 0
    })
    return (
        <dl className="la-observer-summary">
            <div>
                <dt>Total instances</dt>
                <dd aria-label="Total instances">{(totals.live + totals.dormant + totals.unknown).toLocaleString()}</dd>
                <p>{actorName === undefined ? `${inventory.actors.length} actor ${inventory.actors.length === 1 ? "type" : "types"}` : "In this actor class"}</p>
            </div>
            <div>
                <dt>
                    <span className="la-observer-dot la-observer-dot-live" />
                    Live
                </dt>
                <dd aria-label="Live instances">{totals.live.toLocaleString()}</dd>
                <p>Loaded in memory</p>
            </div>
            <div>
                <dt>
                    <span className="la-observer-dot" />
                    Dormant
                </dt>
                <dd aria-label="Dormant instances">{totals.dormant.toLocaleString()}</dd>
                <p>Currently unloaded</p>
            </div>
            {totals.unknown > 0 && (
                <div>
                    <dt>
                        <span className="la-observer-dot la-observer-dot-unknown" />
                        Unknown
                    </dt>
                    <dd aria-label="Unknown instances">{totals.unknown.toLocaleString()}</dd>
                    <p>Awaiting host report</p>
                </div>
            )}
            {queueWait !== undefined && (
                <div className="la-observer-queue-wait">
                    <dt>Avg queue wait</dt>
                    <dd aria-label="Average queue wait">{formatWait(queueWait?.averageMs)}</dd>
                    <p>{queueWait ? `Max ${formatWait(queueWait.maxMs)} · ${queueWait.admitted.toLocaleString()} admitted ${rangePhrase(range)}` : `No admitted requests ${rangePhrase(range)}`}</p>
                </div>
            )}
        </dl>
    )
}

function ActorTable({
    inventory,
    query,
    onQueryChange,
    onSelectActor,
    onSelectInstance,
    waits
}: {
    inventory: ActorInventory
    query: string
    onQueryChange: (query: string) => void
    onSelectActor: (actorName: string) => void
    onSelectInstance: (actorName: string, actorId: string) => void
    waits?: Map<string, QueueWaitStats>
}) {
    const hasUnknown = inventory.actors.some(actor => actor.unknown > 0)
    const actors = inventory.actors.filter(actor => matchesActor(actor, query)).sort((a, b) => Number(b.live > 0) - Number(a.live > 0))
    const suggestions = actorSuggestions(inventory)
    return (
        <>
            <div className="la-observer-list-toolbar">
                <h2>
                    Actor names <Badge>{inventory.actors.length}</Badge>
                </h2>
                <FilterCombobox
                    label="Search actors and instances"
                    placeholder="Search actor name or instance ID…"
                    value={query}
                    onChange={onQueryChange}
                    onSelectSuggestion={suggestion => {
                        const target = JSON.parse(suggestion.id!) as { actorName: string; actorId?: string }
                        if (target.actorId === undefined) onSelectActor(target.actorName)
                        else onSelectInstance(target.actorName, target.actorId)
                    }}
                    suggestions={suggestions}
                    limit={12}
                    className="la-observer-lookup"
                />
            </div>
            <div className="la-observer-table-frame">
                {actors.length ? (
                    <Table aria-label="Actor instance counts" className="la-observer-table">
                        <TableHeader>
                            <TableRow>
                                <TableHead scope="col">Actor</TableHead>
                                <TableHead scope="col">Live</TableHead>
                                <TableHead scope="col">Dormant</TableHead>
                                {hasUnknown && <TableHead scope="col">Unknown</TableHead>}
                                <TableHead scope="col">Total</TableHead>
                                {waits && (
                                    <TableHead scope="col" className="la-observer-wait-cell">
                                        Avg queue wait
                                    </TableHead>
                                )}
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {actors.map(actor => (
                                <TableRow key={actor.actorName} className="la-clickable-row" onClick={() => onSelectActor(actor.actorName)}>
                                    <TableCell className="la-observer-name">
                                        <Button variant="ghost" type="button" className="la-observer-actor-button la-observer-class-button">
                                            <Box className="la-observer-type-icon" aria-hidden="true" />
                                            <span>{actor.actorName}</span>
                                        </Button>
                                    </TableCell>
                                    <TableCell>
                                        <span className={actor.live > 0 ? "la-observer-live" : undefined}>{actor.live.toLocaleString()}</span>
                                    </TableCell>
                                    <TableCell>{actor.dormant.toLocaleString()}</TableCell>
                                    {hasUnknown && <TableCell>{actor.unknown.toLocaleString()}</TableCell>}
                                    <TableCell>{(actor.live + actor.dormant + actor.unknown).toLocaleString()}</TableCell>
                                    {waits && (
                                        <TableCell className="la-observer-wait-cell">
                                            <QueueWait stats={waits.get(actor.actorName)} />
                                        </TableCell>
                                    )}
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                ) : (
                    <EmptyState title="No matching actors" description="Try a different actor name." action="Clear search" onReset={() => onQueryChange("")} />
                )}
            </div>
            {hasUnknown && (
                <p className="la-observer-unknown">
                    <CircleHelp aria-hidden="true" />
                    Some hosts have not reported which instances are loaded. Their counts are shown as unknown.
                </p>
            )}
        </>
    )
}

function actorSuggestions(inventory: ActorInventory): FilterSuggestion[] {
    return inventory.actors.flatMap(actor => [
        {
            id: JSON.stringify({ actorName: actor.actorName }),
            group: "Actor classes",
            value: actor.actorName,
            hint: `${actor.instances.length.toLocaleString()} ${actor.instances.length === 1 ? "instance" : "instances"}`
        },
        ...actor.instances.map(instance => ({
            id: JSON.stringify({ actorName: actor.actorName, actorId: instance.actorId }),
            group: "Instances",
            value: instance.actorId,
            keywords: [actor.actorName],
            hint: `${actor.actorName} · ${labelStatus(instance.status)} · ${instance.connections.length.toLocaleString()} ${instance.connections.length === 1 ? "connection" : "connections"}`
        }))
    ])
}

function matchesActor(actor: ActorInventory["actors"][number], query: string) {
    const terms = query.trim().toLocaleLowerCase().split(/\s+/u).filter(Boolean)
    const value = [actor.actorName, ...actor.instances.map(instance => instance.actorId)].join(" ").toLocaleLowerCase()
    return terms.every(term => value.includes(term))
}

function ActorInstances({
    client,
    id,
    actorName,
    instances,
    waits,
    range,
    selectedInstanceId,
    onSelectInstance
}: {
    client: ObserverClient
    id: string
    actorName: string
    instances: ActorInstance[]
    waits?: Map<string, QueueWaitStats>
    range: TimeRange
    selectedInstanceId?: string
    onSelectInstance: (actorId?: string) => void
}) {
    const [query, setQuery] = useState("")
    const [status, setStatus] = useState("all")
    const filtered = instances.filter(instance => matches(instance.actorId, query) && (status === "all" || instance.status === status))
    filtered.sort((a, b) => Number(b.status === "live") - Number(a.status === "live"))
    const selectedInstance = instances.find(instance => instance.actorId === selectedInstanceId)
    if (selectedInstanceId !== undefined)
        return <InstanceDetails key={selectedInstanceId} client={client} id={id} actorName={actorName} actorId={selectedInstanceId} instance={selectedInstance} waits={waits} range={range} />
    return (
        <section id={id} className="la-observer-instances" aria-labelledby={`${id}-heading`}>
            <div className="la-observer-instances-heading">
                <div>
                    <h2 id={`${id}-heading`}>
                        {actorName} instances <Badge aria-hidden="true">{instances.length.toLocaleString()}</Badge>
                    </h2>
                    <p>Select an instance to inspect its persisted state, requests, and WebSocket connections.</p>
                </div>
            </div>
            {instances.length ? (
                <>
                    <div className="la-observer-filters">
                        <SearchField label="Search instances" placeholder="Search instance IDs…" value={query} onChange={setQuery} />
                        <NativeSelect className="la-observer-select" aria-label="Instance state" value={status} onChange={event => setStatus(event.target.value)}>
                            <NativeSelectOption value="all">All states</NativeSelectOption>
                            <NativeSelectOption value="live">Live</NativeSelectOption>
                            <NativeSelectOption value="dormant">Dormant</NativeSelectOption>
                            <NativeSelectOption value="unknown">Unknown</NativeSelectOption>
                        </NativeSelect>
                    </div>
                    <div className="la-observer-table-frame">
                        {filtered.length ? (
                            <Table aria-label={`${actorName} instances`} className="la-observer-instance-table">
                                <TableHeader>
                                    <TableRow>
                                        <TableHead scope="col">Instance</TableHead>
                                        <TableHead scope="col">State</TableHead>
                                        <TableHead scope="col">Connections</TableHead>
                                        {waits && (
                                            <TableHead scope="col" className="la-observer-wait-cell">
                                                Avg queue wait
                                            </TableHead>
                                        )}
                                        <TableHead scope="col" className="la-observer-waiting-cell">
                                            Waiting
                                        </TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {filtered.map(instance => (
                                        <TableRow key={instance.actorId} className="la-clickable-row" onClick={() => onSelectInstance(instance.actorId)}>
                                            <TableCell className="la-observer-instance-id">
                                                <Button variant="ghost" type="button" className="la-observer-actor-button">
                                                    <span>{instance.actorId}</span>
                                                </Button>
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline" className={`la-observer-status-${instance.status}`}>
                                                    <span className={`la-observer-dot la-observer-dot-${instance.status}`} />
                                                    {labelStatus(instance.status)}
                                                </Badge>
                                            </TableCell>
                                            <TableCell>{instance.connections.length.toLocaleString()}</TableCell>
                                            {waits && (
                                                <TableCell className="la-observer-wait-cell">
                                                    <QueueWait stats={waits.get(instance.actorId)} />
                                                </TableCell>
                                            )}
                                            <TableCell className="la-observer-waiting-cell">
                                                <QueueBubbles waiting={instance.waiting} compact />
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        ) : (
                            <EmptyState
                                title="No matching instances"
                                description="Try another instance ID or state."
                                action="Clear filters"
                                onReset={() => {
                                    setQuery("")
                                    setStatus("all")
                                }}
                            />
                        )}
                    </div>
                    <p className="la-observer-connection-note">Connections are active WebSocket connections, not unique people.</p>
                </>
            ) : (
                <p className="la-observer-instance-empty" role="status">
                    No {actorName} instances have been created yet.
                </p>
            )}
        </section>
    )
}

function InstanceDetails({
    client,
    id,
    actorName,
    actorId,
    instance,
    waits,
    range
}: {
    client: ObserverClient
    id: string
    actorName: string
    actorId: string
    instance?: ActorInstance
    waits?: Map<string, QueueWaitStats>
    range: TimeRange
}) {
    const [view, setView] = useState("requests")
    const [requestFocus, setRequestFocus] = useState<{ requestId?: string; connectionId?: string }>()
    const requestButton = useRef<HTMLButtonElement>(null)
    const hasState = !!client.getState && !!client.listStateHistory
    return (
        <section className="la-observer-instance-detail" aria-label={`${actorName} / ${actorId}`}>
            {!instance && <p role="status">This instance is no longer in the current inventory. Its retained requests are still available below.</p>}
            <div className="la-observer-instance-metrics">
                {waits && <InstanceQueueWait stats={waits.get(actorId)} range={range} />}
                {instance && <WaitingRequests waiting={instance.waiting} />}
            </div>
            <div className="la-observer-instance-views" role="group" aria-label="Instance view">
                <Button ref={requestButton} variant="ghost" aria-pressed={view === "requests"} onClick={() => setView("requests")}>
                    Requests
                </Button>
                {hasState && (
                    <Button variant="ghost" aria-pressed={view === "state"} onClick={() => setView("state")}>
                        State
                    </Button>
                )}
                {instance && (
                    <Button variant="ghost" aria-pressed={view === "websockets"} onClick={() => setView("websockets")}>
                        WebSockets <Badge>{instance.connections.length}</Badge>
                    </Button>
                )}
            </div>
            <div hidden={view !== "requests"}>
                {client.watchRequests || client.listRequests ? (
                    <RequestObserver client={client} actor={{ actorName, actorId }} timeRange={range} focus={requestFocus} />
                ) : (
                    <section>
                        <h3>Requests</h3>
                        <p>Request timings are unavailable for this connection.</p>
                    </section>
                )}
            </div>
            <div hidden={view !== "state"}>
                {hasState && (
                    <StateInspector
                        client={client}
                        actorName={actorName}
                        actorId={actorId}
                        onInspectRequest={focus => {
                            setRequestFocus(focus)
                            setView("requests")
                            requestButton.current?.focus()
                        }}
                    />
                )}
            </div>
            <div hidden={view !== "websockets"}>{instance && <WebSocketConnections id={`${id}-connections`} actorId={actorId} connections={instance.connections} />}</div>
        </section>
    )
}

function QueueWait({ stats }: { stats?: QueueWaitStats }) {
    if (!stats) return <span title="No admitted requests in the selected time range">—</span>
    return (
        <span className="la-observer-wait" title={`Average of ${stats.admitted.toLocaleString()} admitted requests in the selected time range; longest wait ${formatWait(stats.maxMs)}`}>
            {formatWait(stats.averageMs)}
            <small>max {formatWait(stats.maxMs)}</small>
        </span>
    )
}

function InstanceQueueWait({ stats, range }: { stats?: QueueWaitStats; range: TimeRange }) {
    return (
        <section className="la-observer-queue-wait-detail" aria-label="Queue wait" title={`Time waiting to enter this actor ${rangePhrase(range)}`}>
            {stats ? (
                <dl>
                    <div>
                        <dt>Avg queue wait</dt>
                        <dd aria-label="Average queue wait">{formatWait(stats.averageMs)}</dd>
                    </div>
                    <div>
                        <dt>Max</dt>
                        <dd aria-label="Longest queue wait">{formatWait(stats.maxMs)}</dd>
                    </div>
                    <div>
                        <dt>Admitted</dt>
                        <dd aria-label="Admitted requests">{stats.admitted.toLocaleString()}</dd>
                    </div>
                </dl>
            ) : (
                <p>No requests admitted {rangePhrase(range)}.</p>
            )}
        </section>
    )
}

function WaitingRequests({ waiting }: { waiting: ActorInstance["waiting"] }) {
    return (
        <section className="la-observer-waiting" aria-label="Waiting requests">
            {waiting?.length ? (
                <>
                    <span className="la-observer-waiting-label">
                        Waiting <strong>{waiting.length}</strong>
                    </span>
                    <QueueBubbles waiting={waiting} />
                </>
            ) : (
                <p>{waiting == null ? "Queue reporting unavailable" : "No requests waiting."}</p>
            )}
        </section>
    )
}

function QueueBubbles({ waiting, compact = false }: { waiting: ActorInstance["waiting"]; compact?: boolean }) {
    if (waiting == null) return <span title="Queue reporting is unavailable">Unavailable</span>
    if (!waiting.length) return <span className="la-observer-queue-empty">None</span>
    const visible = compact ? waiting.slice(0, 3) : waiting
    return (
        <ol className={`la-observer-queue${compact ? " la-observer-queue-compact" : ""}`} aria-label={`${waiting.length} waiting requests, next to run first`}>
            {visible.map((request, index) => (
                <li key={request.id}>
                    <Badge variant="outline" className="la-observer-queue-bubble" title={request.operation}>
                        {!compact && <span className="la-observer-queue-position">{index + 1}</span>}
                        <span className="la-observer-queue-operation">{request.operation}</span>
                    </Badge>
                </li>
            ))}
            {compact && waiting.length > visible.length && (
                <li>
                    <Badge aria-label={`${waiting.length - visible.length} more waiting requests`}>+{waiting.length - visible.length}</Badge>
                </li>
            )}
        </ol>
    )
}

function WebSocketConnections({ id, actorId, connections }: { id: string; actorId: string; connections: ActorInstance["connections"] }) {
    return (
        <section id={id} className="la-observer-connections" aria-labelledby={`${id}-heading`}>
            <div className="la-observer-instances-heading">
                <div>
                    <h3 id={`${id}-heading`}>
                        {actorId} WebSockets <Badge aria-hidden="true">{connections.length.toLocaleString()}</Badge>
                    </h3>
                    <p>{`${connections.length.toLocaleString()} active ${connections.length === 1 ? "connection" : "connections"}`}</p>
                </div>
            </div>
            {connections.length ? (
                <div className="la-observer-table-frame">
                    <Table aria-label={`${actorId} WebSocket connections`} className="la-observer-connection-table">
                        <TableHeader>
                            <TableRow>
                                <TableHead scope="col">Socket</TableHead>
                                <TableHead scope="col">Metadata</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {connections.map(connection => (
                                <TableRow key={connection.id}>
                                    <TableCell className="la-observer-socket-id">{connection.id}</TableCell>
                                    <TableCell>
                                        <pre>{JSON.stringify(connection.metadata, null, 2)}</pre>
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            ) : (
                <div className="la-observer-no-connections" role="status">
                    <Unplug aria-hidden="true" />
                    <p>No active WebSocket connections.</p>
                </div>
            )}
        </section>
    )
}

function SearchField({ label, placeholder, value, onChange }: { label: string; placeholder: string; value: string; onChange: (value: string) => void }) {
    return (
        <div className="la-observer-search">
            <Search aria-hidden="true" />
            <Input type="search" aria-label={label} placeholder={placeholder} value={value} onInput={event => onChange(event.currentTarget.value)} />
        </div>
    )
}

function EmptyState({ title, description, action, onReset }: { title: string; description: string; action?: string; onReset?: () => void }) {
    return (
        <div className="la-observer-empty">
            <Search aria-hidden="true" />
            <h3>{title}</h3>
            <p>{description}</p>
            {action && (
                <Button type="button" variant="outline" onClick={onReset}>
                    {action}
                </Button>
            )}
        </div>
    )
}

function matches(value: string, query: string) {
    return value.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase())
}
function labelStatus(status: ActorInstance["status"]): string {
    return status[0]!.toUpperCase() + status.slice(1)
}

export { ActorObserver }
export type { ActorObserverProps }

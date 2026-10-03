import { useEffect, useState } from "react"

import { RefreshCw, Search } from "lucide-react"

import { ConnectionInventoryNotice } from "./ConnectionInventoryNotice.js"
import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ActorInventory, ObserverClient, RequestTracePage } from "./client.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useInventory, usePolledQuery, useRequests } from "./observer-hooks.js"
import { inventorySummary, tracesInRange } from "./overview-data.js"
import { liveOverviewMetrics } from "./overview-metrics.js"
import type { ClassMetrics, OverviewMetrics } from "./overview-metrics.js"
import { defaultTimeRange, rangePhrase, resolveRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

export interface OverviewProps {
    client: ObserverClient
    onSelectActor: (actorName: string) => void
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

export function Overview({ client, onSelectActor, timeRange, onTimeRangeChange }: OverviewProps) {
    const actors = useInventory(client)
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const setRange = onTimeRangeChange ?? setLocalRange
    const [now, setNow] = useState(Date.now)
    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 10_000)
        return () => clearInterval(timer)
    }, [])
    const resolved = resolveRange(range, now)
    const saved = usePolledQuery(client, resolved, client.getMetrics)
    const requests = useRequests(client, !saved.supported)
    const live = requests.page ? liveOverviewMetrics(tracesInRange(requests.page.records, resolved, Math.max(now, Date.now()))) : undefined
    const metrics: OverviewMetrics | undefined = saved.supported ? saved.value : live
    const failed = saved.supported ? saved.failed : requests.failed
    return (
        <section className="la-observer overview" aria-label="Runtime overview">
            <div className="overview-heading">
                <h1>Durable Actors</h1>
                <div className="overview-controls">
                    <span className="overview-updated">
                        {actors.failed ? (
                            "Inventory reconnecting…"
                        ) : actors.updatedAt ? (
                            <>
                                Last inventory update <time dateTime={new Date(actors.updatedAt).toISOString()}>{new Date(actors.updatedAt).toLocaleTimeString([], { hour12: false })}</time>
                            </>
                        ) : (
                            "Connecting…"
                        )}
                    </span>
                    <TimeRangePicker value={range} onChange={setRange} className="overview-range" />
                    <Button
                        variant="outline"
                        size="icon"
                        aria-label="Refresh overview"
                        onClick={() => {
                            actors.retry()
                            saved.retry()
                            requests.retry()
                        }}
                    >
                        <RefreshCw aria-hidden="true" />
                    </Button>
                </div>
            </div>
            {actors.failed && (
                <p className="overview-alert" role="alert">
                    Inventory unavailable. {actors.inventory ? "Showing the last received counts; they may be out of date." : "Check your connection and access."} Retrying automatically.
                </p>
            )}
            {failed && (
                <p className="overview-alert" role="alert">
                    {saved.supported ? "Request history unavailable." : "Request stream unavailable."} {metrics ? "Showing the last received metrics." : "Request metrics are not available yet."}{" "}
                    Retrying automatically.
                </p>
            )}
            <SummaryCards inventory={actors.inventory} metrics={metrics} range={range} />
            <ConnectionInventoryNotice inventory={actors.inventory} />
            <ClassTable inventory={actors.inventory} metrics={metrics} failed={actors.failed} onSelectActor={onSelectActor} />
            <DataScope page={saved.supported ? undefined : requests.page} range={range} saved={saved.supported} />
        </section>
    )
}

function SummaryCards({ inventory, metrics, range }: { inventory?: ActorInventory; metrics?: OverviewMetrics; range: TimeRange }) {
    const totals = inventory ? inventorySummary(inventory) : undefined
    const total = totals ? totals.live + totals.dormant + totals.unknown : undefined
    const requests = metrics?.total
    return (
        <div className="overview-metrics">
            <section className="overview-metric">
                <h2>Actor instances</h2>
                <div className="overview-value">
                    <strong aria-label="Actor instances">{number(total)}</strong>
                    <span>{inventory ? `${inventory.actors.length} actor ${inventory.actors.length === 1 ? "class" : "classes"}` : "Waiting for inventory"}</span>
                </div>
                <div className="overview-residency" aria-hidden="true">
                    {totals &&
                        Object.entries(totals)
                            .filter(([key]) => key !== "connections")
                            .map(([key, value]) => <span key={key} className={`residency-${key}`} style={{ flexGrow: value }} />)}
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{number(totals?.live)}</b> live
                    </span>
                    <span>
                        <b>{number(totals?.dormant)}</b> dormant
                    </span>
                    {!!totals?.unknown && (
                        <span>
                            <b>{number(totals.unknown)}</b> unknown
                        </span>
                    )}
                </div>
            </section>
            <section className="overview-metric">
                <h2>Requests</h2>
                <div className="overview-value">
                    <strong aria-label="Retained requests">{number(requests?.count)}</strong>
                    <span>{rangePhrase(range)}</span>
                </div>
                <div className="overview-health">
                    <span>
                        <Health value={requests?.success} kind="success" /> success
                    </span>
                    <span>
                        <Health value={requests?.p95} kind="latency" /> p95 latency
                    </span>
                    <span>
                        <b>{milliseconds(requests?.queueP95)}</b> p95 queue wait
                    </span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>WebSocket connections</h2>
                <div className="overview-value">
                    <strong aria-label="Open WebSocket connections">{number(totals?.connections)}</strong>
                    <span>open</span>
                </div>
                <div className="overview-health">
                    <span>Current inventory snapshot</span>
                    <span>Active sockets</span>
                </div>
            </section>
        </div>
    )
}

function ClassTable({ inventory, metrics, failed, onSelectActor }: { inventory?: ActorInventory; metrics?: OverviewMetrics; failed: boolean; onSelectActor: OverviewProps["onSelectActor"] }) {
    const [query, setQuery] = useState("")
    const [residency, setResidency] = useState("all")
    const actors =
        inventory?.actors
            .filter(actor => actor.actorName.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()) && (residency === "all" || actor[residency as "live" | "dormant" | "unknown"] > 0))
            .sort((a, b) => Number(b.live > 0) - Number(a.live > 0)) ?? []
    return (
        <section aria-label="Actor classes">
            <div className="overview-section-heading">
                <h2>
                    Actor classes <span>{number(inventory?.actors.length)}</span>
                </h2>
                <Thresholds />
            </div>
            <div className="overview-panel">
                <div className="overview-filterbar">
                    <div className="overview-search">
                        <Search aria-hidden="true" />
                        <Input aria-label="Filter actor classes" placeholder="Filter actor classes…" value={query} onInput={event => setQuery(event.currentTarget.value)} />
                    </div>
                    <NativeSelect aria-label="Filter by residency" size="sm" value={residency} onChange={event => setResidency(event.target.value)}>
                        <NativeSelectOption value="all">All states</NativeSelectOption>
                        <NativeSelectOption value="live">Live instances</NativeSelectOption>
                        <NativeSelectOption value="dormant">Dormant instances</NativeSelectOption>
                        <NativeSelectOption value="unknown">Unknown residency</NativeSelectOption>
                    </NativeSelect>
                </div>
                <Table
                    className="overview-table"
                    aria-label="Actor class metrics"
                    containerProps={{ className: "overview-table-scroll", role: "region", "aria-label": "Actor class metrics", tabIndex: 0 }}
                >
                    <colgroup>
                        <col className="overview-class-column" />
                        <col span={6} />
                    </colgroup>
                    <TableHeader>
                        <TableRow className="overview-groups">
                            <TableHead scope="colgroup" colSpan={2}>
                                Actors
                            </TableHead>
                            <TableHead scope="colgroup" colSpan={4}>
                                Requests
                            </TableHead>
                            <TableHead scope="colgroup">WebSockets</TableHead>
                        </TableRow>
                        <TableRow>
                            <TableHead scope="col">Class</TableHead>
                            <TableHead scope="col">Instances</TableHead>
                            <TableHead scope="col">Total</TableHead>
                            <TableHead scope="col">Success</TableHead>
                            <TableHead scope="col">p95 latency</TableHead>
                            <TableHead scope="col">p95 queue wait</TableHead>
                            <TableHead scope="col">Connected</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {actors.map(actor => (
                            <ClassRow
                                key={actor.actorName}
                                actor={actor}
                                metrics={metrics && (metrics.classes.find(row => row.actorName === actor.actorName) ?? empty(actor.actorName))}
                                onSelectActor={onSelectActor}
                            />
                        ))}
                    </TableBody>
                </Table>
                {!actors.length && (
                    <div className="overview-empty" role="status">
                        <strong>{!inventory ? (failed ? "Inventory unavailable" : "Loading actor classes…") : inventory.actors.length ? "No matching actor classes" : "No actors yet"}</strong>
                        <p>{inventory?.actors.length ? "Try another class name or residency state." : "Actor classes appear after deployment."}</p>
                        {!!inventory?.actors.length && (
                            <Button
                                variant="outline"
                                onClick={() => {
                                    setQuery("")
                                    setResidency("all")
                                }}
                            >
                                Clear filters
                            </Button>
                        )}
                    </div>
                )}
            </div>
        </section>
    )
}

function ClassRow({ actor, metrics, onSelectActor }: { actor: ActorInventory["actors"][number]; metrics?: ClassMetrics; onSelectActor: OverviewProps["onSelectActor"] }) {
    return (
        <TableRow className="la-clickable-row" onClick={() => onSelectActor(actor.actorName)}>
            <TableCell>
                <Button variant="ghost" type="button" className="overview-class-link" aria-label={`Inspect ${actor.actorName}`}>
                    <span title={actor.actorName}>{actor.actorName}</span>
                </Button>
            </TableCell>
            <TableCell>{number(actor.live + actor.dormant + actor.unknown)}</TableCell>
            <TableCell>{number(metrics?.count)}</TableCell>
            <TableCell>
                <Health value={metrics?.success} kind="success" />
            </TableCell>
            <TableCell>
                <Health value={metrics?.p95} kind="latency" />
            </TableCell>
            <TableCell>{milliseconds(metrics?.queueP95)}</TableCell>
            <TableCell>{number(actor.instances.reduce((sum, instance) => sum + instance.connections.length, 0))}</TableCell>
        </TableRow>
    )
}

function Health({ value, kind }: { value?: number | null; kind: "success" | "latency" }) {
    if (value == null) return <span title="No retained execution attempts">—</span>
    const level = kind === "success" ? (value < 95 ? "bad" : value < 99 ? "warn" : "good") : value > 100 ? "bad" : "good"
    return (
        <span className={`overview-signal signal-${level}`} title={level === "bad" ? "Critical threshold breached" : level === "warn" ? "Warning threshold breached" : "Within threshold"}>
            {level !== "good" && <span aria-hidden="true">{level === "warn" ? "!" : "×"}</span>}
            {kind === "success" ? `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}%` : milliseconds(value)}
        </span>
    )
}

function Thresholds() {
    return (
        <details className="overview-thresholds">
            <summary>
                <span className="threshold-dots" aria-hidden="true">
                    <i />
                    <i />
                    <i />
                </span>
                Thresholds
            </summary>
            <div>
                <strong>Display thresholds</strong>
                <p>Success: warning below 99%; critical below 95%.</p>
                <p>Latency: critical when p95 exceeds 100 ms.</p>
                <p>Reroutes are excluded from success and latency. Queue wait has no health threshold.</p>
            </div>
        </details>
    )
}

function DataScope({ page, range, saved }: { page?: RequestTracePage; range: TimeRange; saved: boolean }) {
    return (
        <div className="overview-data-scope">
            <p>
                {saved
                    ? `Request metrics cover every retained request ${rangePhrase(range)}${range.kind === "relative" ? " (refreshed every 10 seconds)" : ""}. Inventory counts are current snapshots.`
                    : `Request metrics cover the latest ${page?.capacity ?? 500} retained traces within the selected window. Inventory counts are current snapshots.`}
            </p>
            {!!page?.dropped && <p role="alert">{number(page.dropped)} traces were not delivered. Request metrics are incomplete.</p>}
            {page?.persistenceFailed && <p role="alert">Some request events could not be saved. This history may be incomplete.</p>}
            {!!page?.evicted && <p>Earlier traces have expired; these metrics do not represent the full time window.</p>}
        </div>
    )
}

function empty(actorName: string): ClassMetrics {
    return { actorName, count: 0, success: null, p95: null, queueP95: null }
}
function number(value?: number) {
    return value === undefined ? "—" : value.toLocaleString()
}
function milliseconds(value?: number | null) {
    return value == null ? "—" : `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

import { useEffect, useId, useMemo, useRef, useState } from "react"

import { ChartNoAxesGantt, List, Pause, Play, RefreshCw, X } from "lucide-react"

import { RequestTimeline } from "./RequestTimeline.js"
import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ObserverClient, RequestTrace, RequestTracePage } from "./client.js"
import type { RequestHistoryQuery as HistoryFilters } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Drawer, DrawerClose, DrawerContent, DrawerDescription, DrawerHeader, DrawerTitle } from "./components/ui/drawer.js"
import { Input } from "./components/ui/input.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useRequests } from "./observer-hooks.js"
import { useRequestHistory } from "./request-history.js"
import { duration, gapLabel, requestTimeline } from "./request-timeline.js"
import { defaultTimeRange, resolveRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

interface RequestObserverProps {
    focus?: { requestId?: string; connectionId?: string }
    client: Pick<ObserverClient, "watchRequests" | "listRequests">
    actor?: Pick<RequestTrace, "actorName" | "actorId">
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

function RequestObserver({ client, actor, timeRange, onTimeRangeChange, focus }: RequestObserverProps) {
    const [query, setQuery] = useState<HistoryFilters>()
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const setRange = onTimeRangeChange ?? setLocalRange
    const scopedQuery = useMemo(
        () => (query ? { ...query, ...actor, ...(query.requestId || query.connectionId ? {} : resolveRange(range, Date.now())) } : undefined),
        [query, actor?.actorName, actor?.actorId, range]
    )
    const history = useRequestHistory(client, scopedQuery)
    const { page, failed, retry } = useRequests(client)
    const [frozen, setFrozen] = useState<RequestTracePage>()
    const [selected, setSelected] = useState<RequestTrace>()
    const [view, setView] = useState<"waterfall" | "table">("waterfall")
    const container = useRef<HTMLElement>(null)
    const trigger = useRef<HTMLButtonElement | null>(null)
    const Heading = actor ? "h3" : "h1"
    const shown = query ? history.page : (frozen ?? page)
    const statusPage = page ?? history.page
    const records = (shown?.records ?? []).filter(record => !actor || (record.actorName === actor.actorName && record.actorId === actor.actorId))
    useEffect(() => {
        setFrozen(undefined)
        setQuery(undefined)
    }, [client, actor?.actorName, actor?.actorId])
    useEffect(() => {
        if (focus) setQuery(focus)
    }, [focus])
    useEffect(() => setSelected(undefined), [client, page?.epoch, query, actor?.actorName, actor?.actorId])
    return (
        <section ref={container} className="la-observer la-requests" aria-label="Request observer">
            {(query?.requestId || query?.connectionId) && (
                <p>
                    Showing {query.requestId ? `request ${query.requestId}` : `connection ${query?.connectionId}`}{" "}
                    <Button variant="link" onClick={() => setQuery(undefined)}>
                        Clear filter
                    </Button>
                </p>
            )}
            <div className="la-observer-toolbar">
                <div className={actor ? "la:sr-only" : undefined}>
                    <Heading>Requests</Heading>
                    <p>{actor ? "Method calls and WebSocket events for this instance." : "Method calls and WebSocket events, with time spent waiting and processing."}</p>
                </div>
                <div className="la-request-actions">
                    <div className="request-view-toggle" role="group" aria-label="Request view">
                        <Button variant={view === "waterfall" ? "secondary" : "ghost"} aria-pressed={view === "waterfall"} onClick={() => setView("waterfall")}>
                            <ChartNoAxesGantt aria-hidden="true" />
                            Waterfall
                        </Button>
                        <Button variant={view === "table" ? "secondary" : "ghost"} aria-pressed={view === "table"} onClick={() => setView("table")}>
                            <List aria-hidden="true" />
                            Table
                        </Button>
                    </div>
                    {client.listRequests && (
                        <>
                            <Button
                                variant={!query ? "secondary" : "outline"}
                                aria-pressed={!query}
                                onClick={() => {
                                    setQuery(undefined)
                                    setFrozen(undefined)
                                }}
                            >
                                Live
                            </Button>
                            <Button
                                variant={query ? "secondary" : "outline"}
                                aria-pressed={!!query}
                                onClick={() => {
                                    if (!query) setQuery({})
                                }}
                            >
                                History
                            </Button>
                        </>
                    )}
                    {!query && (
                        <>
                            <Button variant="outline" disabled={!shown} onClick={() => setFrozen(frozen ? undefined : page)}>
                                {frozen ? <Play aria-hidden="true" /> : <Pause aria-hidden="true" />}
                                {frozen ? "Resume" : "Pause"}
                            </Button>
                            {failed && (
                                <Button variant="outline" onClick={retry}>
                                    <RefreshCw aria-hidden="true" />
                                    Retry
                                </Button>
                            )}
                        </>
                    )}
                </div>
            </div>
            {query && <HistoryFilters query={query} range={range} onRangeChange={setRange} loading={history.loading} onSearch={setQuery} scoped={!!actor} />}
            {query && history.failed && (
                <div className="la-observer-error" role="alert">
                    Request history unavailable.{" "}
                    <Button variant="outline" onClick={history.retry}>
                        Retry history
                    </Button>
                </div>
            )}
            {!query && failed && (
                <div className="la-observer-error" role="alert">
                    Request stream disconnected. Reconnecting… {page ? "Showing the last received requests." : "Check your connection and runtime version."}
                </div>
            )}
            {!!statusPage?.dropped && (
                <div className="la-observer-error" role="alert">
                    {statusPage.dropped.toLocaleString()} request traces were not delivered. This history is incomplete.
                </div>
            )}
            {statusPage?.persistenceFailed && (
                <div className="la-observer-error" role="alert">
                    Some request events could not be saved. This history may be incomplete.
                </div>
            )}
            {shown?.reset && (
                <div className="la-observer-error" role="alert">
                    Some older events are no longer retained. Showing available history.
                </div>
            )}
            {view === "waterfall" && records.length > 0 && (
                <RequestTimeline
                    records={records}
                    selected={selected}
                    onSelect={(record, element) => {
                        trigger.current = element
                        setSelected(record)
                    }}
                />
            )}
            <div className={view === "table" || !records.length ? "la-observer-table-frame" : undefined}>
                {view === "table" && (
                    <Table aria-label={query ? "Saved requests" : "Recent requests"} className="la-request-table">
                        <TableHeader>
                            <TableRow>
                                <TableHead scope="col">Time</TableHead>
                                <TableHead scope="col">Request</TableHead>
                                <TableHead scope="col">Instance</TableHead>
                                <TableHead scope="col">Transport</TableHead>
                                <TableHead scope="col">Outcome</TableHead>
                                <TableHead scope="col">Total</TableHead>
                                <TableHead scope="col">Queue wait</TableHead>
                                <TableHead scope="col">
                                    <span className="la:sr-only">Details</span>
                                </TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {records.map(record => (
                                <TableRow
                                    key={record.eventId ?? `${shown!.epoch}-${record.sequence}`}
                                    className="la-clickable-row"
                                    data-state={selected === record ? "selected" : undefined}
                                    onClick={event => {
                                        trigger.current = event.currentTarget.querySelector("button")
                                        setSelected(record)
                                    }}
                                >
                                    <TableCell>
                                        <time dateTime={new Date(record.startedAtMs).toISOString()} title={new Date(record.startedAtMs).toLocaleString()}>
                                            {query ? new Date(record.startedAtMs).toLocaleString([], { hour12: false }) : new Date(record.startedAtMs).toLocaleTimeString([], { hour12: false })}
                                        </time>
                                    </TableCell>
                                    <TableCell>
                                        <span className="la-request-operation" title={record.operation}>
                                            {record.operation}
                                        </span>
                                    </TableCell>
                                    <TableCell>
                                        <span className="la-request-actor" title={`${record.actorName} / ${record.actorId}`}>
                                            {record.actorName} / {record.actorId}
                                        </span>
                                    </TableCell>
                                    <TableCell>{record.kind === "method" ? "Method" : "WebSocket"}</TableCell>
                                    <TableCell>
                                        <Badge variant="outline" className={`la-request-outcome-${record.outcome}`}>
                                            {record.outcome[0]!.toUpperCase() + record.outcome.slice(1)}
                                        </Badge>
                                    </TableCell>
                                    <TableCell>{duration(record.durationMs)}</TableCell>
                                    <TableCell>{record.queueWaitMs === null ? <span title="Request did not begin processing">—</span> : duration(record.queueWaitMs)}</TableCell>
                                    <TableCell>
                                        <Button variant="ghost" size="sm" aria-label={`Inspect ${record.operation} request`} aria-haspopup="dialog">
                                            Details
                                        </Button>
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                )}
                {!records.length && (
                    <div className="la-request-empty" role="status">
                        <strong>
                            {query
                                ? history.loading
                                    ? "Loading history…"
                                    : history.failed
                                      ? "History unavailable"
                                      : "No saved requests in this range"
                                : !shown
                                  ? failed
                                      ? "Requests unavailable"
                                      : "Connecting to requests…"
                                  : "No requests yet"}
                        </strong>
                        <p>
                            {query
                                ? "Try a wider time range or fewer filters. Local history retains the latest 10,000 events."
                                : "Call an actor method or send a WebSocket message to see its timings."}
                        </p>
                    </div>
                )}
            </div>
            {query && shown?.nextCursor && (
                <Button className="la-request-load-older" variant="outline" disabled={history.loading} onClick={history.loadOlder}>
                    {history.loading ? "Loading…" : "Load older"}
                </Button>
            )}
            <div className="la-observer-footnote">
                <span className="la-observer-refresh-status">
                    {!query && <span className={`la-observer-dot ${failed ? "la-observer-dot-unknown" : "la-observer-dot-live"}`} />}
                    {query ? "Saved history" : frozen ? "Display paused · collection continues" : failed ? "Reconnecting…" : page ? "Live updates" : "Connecting…"}
                </span>
                <span>
                    {query
                        ? `${records.length.toLocaleString()} saved requests shown`
                        : actor
                          ? `${records.length} matching requests from the latest ${shown?.capacity ?? 500} on this control plane.`
                          : `Latest ${shown?.capacity ?? 500} requests on this control plane.`}
                </span>
            </div>
            {!query && !!shown?.evicted && <p className="la-request-note">{shown.evicted.toLocaleString()} older records have left this history window.</p>}
            <p className="la-request-note">
                Total includes queue wait, actor processing, and persistence. Queue wait includes the WebSocket message queue. Timings exclude the caller’s network round trip.
            </p>
            <Drawer
                direction="right"
                autoFocus
                open={!!selected}
                onOpenChange={open => {
                    if (!open) setSelected(undefined)
                }}
            >
                <DrawerContent
                    container={container.current}
                    className="la:data-[vaul-drawer-direction=right]:w-full la:data-[vaul-drawer-direction=right]:sm:max-w-[480px]"
                    onCloseAutoFocus={event => {
                        event.preventDefault()
                        trigger.current?.focus()
                    }}
                >
                    <DrawerHeader className="la:relative la:shrink-0 la:gap-2 la:p-6 la:pr-16">
                        <DrawerTitle>Request details</DrawerTitle>
                        <DrawerDescription>Full identifiers and timings for this request.</DrawerDescription>
                        <DrawerClose asChild>
                            <Button className="la:absolute la:top-4 la:right-4" variant="ghost" size="icon" aria-label="Close">
                                <X aria-hidden="true" />
                            </Button>
                        </DrawerClose>
                    </DrawerHeader>
                    <div className="la:min-h-0 la:flex-1 la:overflow-y-auto la:px-6 la:pb-6">
                        {selected && <RequestDetails record={selected} timing={requestTimeline(records).calls.find(call => call.record === selected)} />}
                    </div>
                </DrawerContent>
            </Drawer>
        </section>
    )
}

function HistoryFilters({
    query,
    range,
    onRangeChange,
    loading,
    onSearch,
    scoped
}: {
    query: HistoryFilters
    range: TimeRange
    onRangeChange: (range: TimeRange) => void
    loading: boolean
    onSearch: (query: HistoryFilters) => void
    scoped: boolean
}) {
    const actorId = useId()
    const outcomeId = useId()
    return (
        <form
            className="la-request-history-filters"
            onSubmit={event => {
                event.preventDefault()
                const data = new FormData(event.currentTarget)
                onSearch({
                    actorId: String(data.get("actorId") || "").trim() || undefined,
                    outcome: (String(data.get("outcome") || "") as HistoryFilters["outcome"]) || undefined
                })
            }}
        >
            <div className="la-request-history-range">
                <span>Time range</span>
                <TimeRangePicker value={range} onChange={onRangeChange} align="start" />
            </div>
            {!scoped && (
                <div className="la-request-history-field">
                    <label htmlFor={actorId}>Actor ID</label>
                    <Input id={actorId} name="actorId" placeholder="All actors" maxLength={256} defaultValue={query.actorId} />
                </div>
            )}
            <div className="la-request-history-field">
                <label htmlFor={outcomeId}>Outcome</label>
                <NativeSelect id={outcomeId} className="la-observer-select" name="outcome" defaultValue={query.outcome ?? ""}>
                    <NativeSelectOption value="">All outcomes</NativeSelectOption>
                    {["completed", "failed", "rejected", "rerouted", "interrupted"].map(outcome => (
                        <NativeSelectOption key={outcome} value={outcome}>
                            {outcome[0]!.toUpperCase() + outcome.slice(1)}
                        </NativeSelectOption>
                    ))}
                </NativeSelect>
            </div>
            <Button type="submit" variant="outline" disabled={loading}>
                Search
            </Button>
        </form>
    )
}

function RequestDetails({ record, timing }: { record: RequestTrace; timing?: { offsetMs: number; gapMs: number | null } }) {
    const fields = {
        Operation: record.operation,
        "Actor class": record.actorName,
        "Instance ID": record.actorId,
        "Request ID": record.requestId,
        Time: new Date(record.startedAtMs).toLocaleString(),
        Transport: record.kind === "method" ? "Method" : "WebSocket",
        Outcome: record.outcome,
        "Host state": { cold: "Cold", warm: "Warm" }[record.hostState],
        Total: duration(record.durationMs),
        "Queue wait": record.queueWaitMs === null ? "Did not begin processing" : duration(record.queueWaitMs),
        ...(timing ? { "Start offset in view": duration(timing.offsetMs), "Gap from preceding calls": gapLabel(timing.gapMs) } : {}),
        Host: record.hostId,
        ...(record.connectionId ? { Connection: record.connectionId } : {})
    }
    return (
        <dl className="la-request-details">
            {Object.entries(fields).map(([label, value]) => (
                <div key={label}>
                    <dt>{label}</dt>
                    <dd>{value}</dd>
                </div>
            ))}
        </dl>
    )
}

export { RequestObserver }
export type { RequestObserverProps }

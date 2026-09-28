import { type FSWatcher, watch } from "chokidar"
import path from "node:path"

interface ActorSourceWatcherOptions {
    projectDirectory: string
    dataDirectory?: string
}

async function watchActorSources(
    options: ActorSourceWatcherOptions,
    refresh: () => Promise<void>
): Promise<ActorSourceWatcher> {
    const watcher = new ActorSourceWatcher(options, refresh, watch, message => console.warn(message))
    await watcher.start()
    return watcher
}

class ActorSourceWatcher {
    private watcher?: FSWatcher
    private timer?: ReturnType<typeof setTimeout>
    private updates = Promise.resolve()
    private closing?: Promise<void>
    private disabled = false

    constructor(
        private readonly options: ActorSourceWatcherOptions,
        private readonly refresh: () => Promise<void>,
        private readonly createWatcher: typeof watch,
        private readonly warn: (message: string) => void
    ) {}

    async start(): Promise<void> {
        try {
            this.watcher = this.createWatcher(this.options.projectDirectory, {
                ignored: (watchedPath, stats) =>
                    ignoredActorPath(watchedPath, this.options) ||
                    (stats !== undefined && !stats.isDirectory() && (!stats.isFile() || !isActorSource(watchedPath))),
                ignoreInitial: true,
                followSymlinks: false,
                atomic: true,
                awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 20 }
            })
            this.watcher.on("all", (_event, changedPath) => this.scheduleRefresh(changedPath))
            await this.waitUntilReady(this.watcher)
        } catch (error) {
            if (isWatchLimit(error)) await this.disable(error)
            else {
                await this.close()
                throw error
            }
        }
    }

    async close(): Promise<void> {
        this.disabled = true
        clearTimeout(this.timer)
        if (!this.closing) {
            this.closing = this.watcher?.close() ?? Promise.resolve()
            // Chokidar removes listeners on close, but pending filesystem operations can still emit errors.
            this.watcher?.on("error", () => {})
        }
        await this.closing
        await this.updates
    }

    private scheduleRefresh(changedPath: string): void {
        if (this.disabled || !isActorSource(changedPath)) return
        clearTimeout(this.timer)
        this.timer = setTimeout(() => {
            this.updates = this.updates.then(this.refresh).catch(reportWatchError)
        }, 75)
    }

    private waitUntilReady(watcher: FSWatcher): Promise<void> {
        return new Promise((resolve, reject) => {
            let ready = false
            watcher.once("ready", () => {
                ready = true
                resolve()
            })
            watcher.on("error", error => {
                if (this.disabled) return
                if (isWatchLimit(error)) void this.disable(error).then(resolve, reject)
                else if (!ready) reject(error)
                else reportWatchError(error)
            })
        })
    }

    private async disable(error: NodeJS.ErrnoException): Promise<void> {
        if (!this.disabled)
            this.warn(
                `Actor source watcher reached the OS file/watch limit (${error.code}). Automatic reload is disabled; ` +
                    "the development server will continue running. Restart it to apply source changes. " +
                    "Close other watchers or raise your OS file/watch limit, then restart to restore automatic reload. " +
                    "Use --no-watch to disable watching explicitly."
            )
        await this.close()
    }
}

function isWatchLimit(error: unknown): error is NodeJS.ErrnoException {
    return error instanceof Error && "code" in error && ["EMFILE", "ENFILE", "ENOSPC"].includes(String(error.code))
}

function ignoredActorPath(candidate: string, options: ActorSourceWatcherOptions): boolean {
    const relative = path.relative(options.projectDirectory, candidate)
    if (!relative || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return false
    const components = relative.split(path.sep)
    if (components.includes(".git") || components.includes("node_modules")) return true
    if ([".durable-actors", "generated"].includes(components[0])) return true
    if (!options.dataDirectory) return false
    const state = path.resolve(options.dataDirectory)
    return candidate === state || candidate.startsWith(`${state}${path.sep}`)
}

function isActorSource(candidate: string): boolean {
    return /\.(?:[cm]?[jt]sx?|json|ya?ml)$/u.test(candidate) || path.basename(candidate) === "bun.lock"
}

function reportWatchError(error: unknown): void {
    console.error(`Actor source update failed: ${error instanceof Error ? error.message : String(error)}`)
}

export { ActorSourceWatcher, watchActorSources }

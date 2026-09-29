import { readFileSync } from "node:fs"

for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
        console.log(`runtime stopped: ${signal}`)
        process.exit(0)
    })
}
process.on("SIGUSR1", () => process.exit(9))
console.log(
    JSON.stringify({
        runtimeStarted: true,
        pid: process.pid,
        args: process.argv.slice(2),
        url: process.env.DURABLE_ACTORS_CONTROL_PLANE_URL,
        savedUrl: readFileSync(".env", "utf8").match(/^DURABLE_ACTORS_CONTROL_PLANE_URL=(.*)$/m)?.[1]
    })
)
setInterval(() => {}, 1000)

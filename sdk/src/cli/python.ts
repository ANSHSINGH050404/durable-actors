import { execFile } from "node:child_process"
import { access, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"

async function pythonExecutable(project: string): Promise<string> {
    if (process.env.DURABLE_ACTORS_PYTHON) return process.env.DURABLE_ACTORS_PYTHON
    const directory = process.env.VIRTUAL_ENV ?? path.join(project, ".venv")
    const executable = path.join(directory, process.platform === "win32" ? "Scripts/python.exe" : "bin/python")
    return (await access(executable).then(
        () => true,
        () => false
    ))
        ? executable
        : "python3"
}

async function checkPython(project: string, targets: string[], executable?: string): Promise<void> {
    await runPython(executable ?? (await pythonExecutable(project)), project, "mypy", ["--strict", ...targets])
}

async function compilePythonContract(entrypoint: string): Promise<unknown> {
    const project = process.cwd()
    const executable = await pythonExecutable(project)
    await checkPython(project, [entrypoint], executable)
    const directory = await mkdtemp(path.join(tmpdir(), "durable-actors-build-"))
    try {
        return JSON.parse(
            await runPython(executable, project, "durable_actors.build", [project, entrypoint, directory, "local"])
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

async function generatePythonClient(contract: unknown, directory: string): Promise<void> {
    const project = process.cwd()
    const executable = await pythonExecutable(project)
    await runPython(executable, project, "durable_actors.codegen", [directory], JSON.stringify(contract))
    await checkPython(project, [directory], executable)
}

function runPython(
    executable: string,
    project: string,
    module: string,
    args: string[],
    input?: string
): Promise<string> {
    return new Promise((resolve, reject) => {
        const child = execFile(
            executable,
            ["-m", module, ...args],
            { cwd: project, maxBuffer: Infinity },
            (error, stdout, stderr) => {
                if (!error) return resolve(stdout)
                const hint =
                    error.code === "ENOENT" || stderr.includes("No module named")
                        ? "\nInstall durable-actors[codegen] in the project virtual environment; DURABLE_ACTORS_PYTHON can select another interpreter."
                        : ""
                reject(new Error(`${module} failed: ${stdout}${stderr || error.message}${hint}`))
            }
        )
        child.stdin?.on("error", () => {})
        child.stdin?.end(input)
    })
}

export { checkPython, compilePythonContract, generatePythonClient, pythonExecutable }

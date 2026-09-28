#!/usr/bin/env node
import { Command, Option } from "commander"
import { config } from "dotenv"
import { cp, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { styleText } from "node:util"
import validatePackageName from "validate-npm-package-name"

import { registerDevCommand } from "./cli/dev.js"
import { registerGenerateCommand } from "./cli/generate.js"
import { registerObserveCommand } from "./cli/observe.js"
import { registerStartCommand } from "./cli/start.js"
import { actorEnvironment } from "./environment.js"

try {
    config({ path: [".env.local", ".env"], quiet: true })
    Object.assign(process.env, actorEnvironment(process.env))
    const program = new Command()
        .name("durable-actors")
        .description("Run durable TypeScript and Python actors locally or in the cloud")
        .version(await version())
        .enablePositionalOptions()
        .addHelpCommand(false)
        .showHelpAfterError()
    program
        .command("init <directory>")
        .description("Create a sample actor project")
        .addOption(
            new Option("--template <name>", "project template")
                .choices(["actor", "chat", "ai-chat", "documents", "python"])
                .default("actor")
        )
        .action(initializeProject)
    registerDevCommand(program)
    registerGenerateCommand(program)
    registerObserveCommand(program)
    registerStartCommand(program)
    if (process.argv.length === 2) program.help()
    await program.parseAsync(process.argv)
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
}

async function initializeProject(directory: string, options: { template: string }): Promise<void> {
    const destination = path.resolve(directory)
    await mkdir(destination).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "EEXIST") throw new Error(`${destination} already exists. Choose a new directory.`)
        throw error
    })
    try {
        await cp(new URL(`./templates/${options.template}/`, import.meta.url), destination, {
            recursive: true,
            force: false
        })
        await rename(path.join(destination, "gitignore"), path.join(destination, ".gitignore"))
        if (options.template === "python")
            await rename(path.join(destination, ".env.example"), path.join(destination, ".env"))
        await nameProject(destination)
    } catch (error) {
        await rm(destination, { recursive: true, force: true })
        throw error
    }
    console.log(projectInstructions(destination, options.template))
}

async function nameProject(destination: string): Promise<void> {
    const manifest = path.join(destination, "package.json")
    const metadata = JSON.parse(await readFile(manifest, "utf8"))
    const name = path
        .basename(destination)
        .toLowerCase()
        .replace(/[^a-z0-9._-]+/g, "-")
        .replace(/^[._-]+|[._-]+$/g, "")
        .slice(0, 214)
    if (validatePackageName(name).validForNewPackages) metadata.name = name
    await writeFile(manifest, JSON.stringify(metadata, null, 4) + "\n")
}

function projectInstructions(destination: string, template: string): string {
    if (template === "python")
        return `Created Python actors in ${destination}.

From that directory, run:
  pnpm install
  uv sync
  pnpm exec durable-actors dev

Edit src/actors.py. The CLI checks types before starting and reloading.
Generate typed Python clients with: pnpm exec durable-actors generate`
    if (template === "actor") return actorProjectInstructions(destination)

    return `Created ${template} app in ${destination}.

From that directory, run:
  npm install
  cp .env.example .env${template === "ai-chat" ? "\n  # Add your OpenAI API key to .env" : ""}
  npm run dev:actors

Add the connection settings printed by durable-actors dev to .env, then in another terminal:
  npm run dev

Open http://127.0.0.1:3000. The README walks through the app.`
}

function actorProjectInstructions(destination: string): string {
    const directory = path.relative(process.cwd(), destination) || "."
    const quoted = `'${directory.replaceAll("'", "'\\''")}'`
    return `
${styleText(["bold", "cyan"], "durable actors")} ${styleText("dim", "/ new project")}

  ${styleText("green", "✓")} ${styleText("bold", path.basename(destination))} is ready.
    ${styleText("dim", destination)}

  ${styleText("bold", "Start here")}
    ${styleText("cyan", `cd -- ${quoted}`)}
    ${styleText("cyan", "pnpm install")}

  ${styleText("bold", "Start the actor server")}
    ${styleText("cyan", "pnpm exec durable-actors dev")}

  ${styleText("bold", "Make it yours")}
    Your first actor is a counter that remembers.
    Edit ${styleText("cyan", "src/actors.ts")} to make it your own.

  ${styleText("bold", "Connect your app")}
    Follow dev's .env and generate instructions
    in your separate application project.
`
}

async function version(): Promise<string> {
    return JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")).version
}

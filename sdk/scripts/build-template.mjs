import { copyFile, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "node:path"

const { version } = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"))

for (const template of ["actor", "chat", "ai-chat", "documents", "python"]) await buildTemplate(template)

async function buildTemplate(template) {
    const source = new URL(
        ["actor", "python"].includes(template) ? `../templates/${template}/` : `../../examples/${template}/`,
        import.meta.url
    )
    const destination = new URL(`../dist/templates/${template}/`, import.meta.url)
    await rm(destination, { recursive: true, force: true })
    await mkdir(destination, { recursive: true })
    const files =
        template === "python"
            ? ["package.json", "pyproject.toml", "README.md", "src", ".env.example"]
            : ["package.json", "tsconfig.json", "README.md", "src"]
    if (!["actor", "python"].includes(template)) files.push("index.html", ".env.example")
    for (const file of files)
        await cp(new URL(file, source), new URL(file, destination), {
            recursive: true,
            filter: file => !["generated", "node_modules", ".durable-actors", "dist"].includes(path.basename(file))
        })
    // npm excludes .gitignore; init restores its name after copying the template.
    await copyFile(new URL(".gitignore", source), new URL("gitignore", destination))
    await copyFile(
        new URL("../templates/pnpm-workspace.yaml", import.meta.url),
        new URL("pnpm-workspace.yaml", destination)
    )
    if (template === "python") {
        const manifest = new URL("pyproject.toml", destination)
        await writeFile(
            manifest,
            (await readFile(manifest, "utf8")).replace("durable-actors[codegen]", `durable-actors[codegen]==${version}`)
        )
    }
    const metadata = JSON.parse(await readFile(new URL("package.json", destination), "utf8"))
    metadata.dependencies["durable-actors"] = version
    await writeFile(new URL("package.json", destination), JSON.stringify(metadata, null, 4) + "\n")
}

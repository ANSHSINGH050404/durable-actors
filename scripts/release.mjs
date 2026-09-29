#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const manifestFiles = {
    cargoLock: "Cargo.lock",
    cargoToml: "Cargo.toml",
    npmPackage: "sdk/package.json",
    observerPackage: "packages/observer-ui/package.json",
    pythonPackage: "sdk-python/pyproject.toml",
    pythonLock: "sdk-python/uv.lock",
    helmChart: "charts/terse/Chart.yaml"
}

export const releaseManifestPaths = Object.values(manifestFiles)

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    const [command, rawVersion] = process.argv.slice(2)
    const version = parseVersion(rawVersion?.replace(/^v/u, ""))
    const manifests = readManifests()

    if (command === "prepare") prepare(manifests, version)
    else if (command === "verify") verifyReleaseVersion(manifests, version)
    else throw new Error("Usage: release.mjs <prepare|verify> <version>")
}

export function parseVersion(value) {
    if (!/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/u.test(value ?? "")) {
        throw new Error(`Version must look like 1.2.3 (got '${value}')`)
    }
    return value
}

export function readReleaseVersion(manifests) {
    const versions = manifestVersions(manifests)
    const uniqueVersions = new Set(versions.map(manifest => manifest.version))
    if (uniqueVersions.size === 1) return parseVersion(versions[0].version)
    throw new Error(`Release manifests disagree:\n${versions.map(manifest => `  ${manifest.path}: ${manifest.version}`).join("\n")}`)
}

export function verifyReleaseVersion(manifests, expected) {
    const mismatched = manifestVersions(manifests).filter(manifest => manifest.version !== expected)
    if (mismatched.length === 0) return
    throw new Error(`Release manifests do not match ${expected}:\n${mismatched.map(manifest => `  ${manifest.path}: ${manifest.version}`).join("\n")}`)
}

export function stampReleaseVersion(manifests, version) {
    parseVersion(version)
    return {
        cargoLock: replaceOne(manifests.cargoLock, /^(\[\[package\]\]\nname = "durable-actors"\nversion = ")[^"]+(")/mu, `$1${version}$2`, "Cargo.lock"),
        cargoToml: replaceOne(manifests.cargoToml, /^(version = ")[^"]+(")/mu, `$1${version}$2`, "Cargo.toml"),
        npmPackage: replaceOne(manifests.npmPackage, /^( {4}"version": ")[^"]+(",?)/mu, `$1${version}$2`, "sdk/package.json"),
        pythonPackage: replaceOne(manifests.pythonPackage, /^(version = ")[^"]+(")/mu, `$1${version}$2`, "sdk-python/pyproject.toml"),
        pythonLock: replaceOne(manifests.pythonLock, /^(\[\[package\]\]\nname = "durable-actors"\nversion = ")[^"]+(")/mu, `$1${version}$2`, "sdk-python/uv.lock"),
        helmChart: replaceOne(
            replaceOne(manifests.helmChart, /^version: .+$/mu, `version: ${version}`, "charts/terse/Chart.yaml"),
            /^appVersion: .+$/mu,
            `appVersion: "${version}"`,
            "charts/terse/Chart.yaml"
        ),
        observerPackage: replaceOne(manifests.observerPackage, /^( {4}"version": ")[^"]+(",?)/mu, `$1${version}$2`, "packages/observer-ui/package.json")
    }
}

function prepare(manifests, version) {
    const previous = readReleaseVersion(manifests)
    const stamped = stampReleaseVersion(manifests, version)
    writeManifests(stamped)
    for (const path of releaseManifestPaths) console.log(`${path}: ${previous} → ${version}`)
    console.log(`\nCommit these files, push main, then publish GitHub Release v${version}.`)
}

function readManifests() {
    return Object.fromEntries(Object.entries(manifestFiles).map(([key, path]) => [key, readFileSync(join(root, path), "utf8")]))
}

function writeManifests(manifests) {
    for (const [key, path] of Object.entries(manifestFiles)) writeFileSync(join(root, path), manifests[key])
}

function manifestVersions(manifests) {
    return [
        { path: manifestFiles.cargoToml, version: matchVersion(manifests.cargoToml, /^version = "([^"]+)"/mu, manifestFiles.cargoToml) },
        {
            path: manifestFiles.cargoLock,
            version: matchVersion(manifests.cargoLock, /^\[\[package\]\]\nname = "durable-actors"\nversion = "([^"]+)"/mu, manifestFiles.cargoLock)
        },
        { path: manifestFiles.pythonPackage, version: matchVersion(manifests.pythonPackage, /^version = "([^"]+)"/mu, manifestFiles.pythonPackage) },
        { path: manifestFiles.pythonLock, version: matchVersion(manifests.pythonLock, /^\[\[package\]\]\nname = "durable-actors"\nversion = "([^"]+)"/mu, manifestFiles.pythonLock) },
        { path: manifestFiles.helmChart, version: matchVersion(manifests.helmChart, /^version: ([^\s]+)$/mu, manifestFiles.helmChart) },
        { path: manifestFiles.helmChart, version: matchVersion(manifests.helmChart, /^appVersion: "([^"]+)"$/mu, manifestFiles.helmChart) },
        { path: manifestFiles.npmPackage, version: JSON.parse(manifests.npmPackage).version },
        { path: manifestFiles.observerPackage, version: JSON.parse(manifests.observerPackage).version }
    ]
}

function matchVersion(source, pattern, path) {
    const version = source.match(pattern)?.[1]
    if (!version) throw new Error(`Could not read the release version from ${path}`)
    return version
}

function replaceOne(source, pattern, replacement, path) {
    const matches = source.match(new RegExp(pattern.source, pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`))
    if (matches?.length !== 1) throw new Error(`Expected one version in ${path}, found ${matches?.length ?? 0}`)
    return source.replace(pattern, replacement)
}

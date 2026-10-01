#[path = "support/local_project.rs"]
mod local_project;

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn sqlite_and_object_fields_survive_runtime_restart_together() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(
        project.path(),
        r#"
        @Persisted count = 0;
        @Persisted payload = "";
        async initialize(): Promise<void> {
            this.payload = "x".repeat(17 * 1024 * 1024);
            this.db.exec("CREATE TABLE entries (value TEXT NOT NULL)");
            this.db.exec("CREATE TABLE payload (data BLOB)");
            this.db.exec("INSERT INTO payload VALUES (zeroblob(?))", 25 * 1024 * 1024);
        }
        async insert(value: string): Promise<void> { this.db.exec("INSERT INTO entries (value) VALUES (?)", value); }
        async increment(): Promise<number> { return ++this.count; }
        async migrate(): Promise<void> {
            this.db.exec("ALTER TABLE entries ADD COLUMN enabled INTEGER DEFAULT 1");
            this.db.exec("PRAGMA user_version = 2");
        }
        async fail(): Promise<void> {
            ++this.count;
            this.db.exec("INSERT INTO entries VALUES ('discarded')");
            throw new Error("rollback");
        }
        async read(): Promise<{count: number; schemaVersion: number; fieldBytes: number; databaseBytes: number; entries: {value: string}[]}> {
            return {
                count: this.count,
                schemaVersion: this.db.exec<{user_version: number}>("PRAGMA user_version")[0]!.user_version,
                fieldBytes: this.payload.length,
                databaseBytes: this.db.exec<{bytes: number}>("SELECT length(data) AS bytes FROM payload")[0]!.bytes,
                entries: this.db.exec<{value: string}>("SELECT value FROM entries ORDER BY rowid")
            };
        }
    "#,
    )?;
    for initialize in [true, false] {
        let runtime = LocalRuntime::start(project.path(), None).await?;
        let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist/backend.js");
        let script = format!(
            r#"
            import assert from 'node:assert/strict';
            import {{ createActorTransport }} from {};
            const client = createActorTransport({{ controlPlaneUrl: process.argv[1] }});
            const call = (method, args = []) => client.invoke('Counter', 'one', method, args);
            if ({initialize}) {{
                await call('initialize');
                await call('insert', ['retained']);
                assert.equal(await call('increment'), 1);
                await assert.rejects(call('fail'), /rollback/);
            }} else {{
                await call('migrate');
                await call('insert', ['after-restart']);
                assert.equal(await call('increment'), 2);
            }}
            assert.deepEqual(await call('read'), {{
                count: {initialize} ? 1 : 2, schemaVersion: {initialize} ? 0 : 2, fieldBytes: 17 * 1024 * 1024, databaseBytes: 25 * 1024 * 1024,
                entries: {initialize} ? [{{value: 'retained'}}] : [{{value: 'retained'}}, {{value: 'after-restart'}}]
            }});
        "#,
            serde_json::to_string(&sdk)?
        );
        let output = timeout(
            Duration::from_secs(60),
            Command::new("node")
                .args(["--input-type=module", "--eval", &script, &runtime.origin])
                .env("DURABLE_ACTORS_TELEMETRY", "0")
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        ensure!(
            output.status.success(),
            "SQLite client failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        runtime.stop().await?;
    }
    use base64::Engine;
    use durable_actors::bucket::{Bucket, FileBucket};
    use durable_actors::state_log::StateSnapshot;
    let bucket = FileBucket::new(project.path().join(".durable-actors/objects"))?;
    let mut snapshots = Vec::new();
    for key in bucket.list("durable-actors/v3/snapshots/").await? {
        snapshots.push(StateSnapshot::decode(
            &bucket.get(&key).await?.unwrap().bytes,
        )?);
    }
    snapshots.sort_by_key(|snapshot| snapshot.state_version);
    assert_eq!(
        snapshots.len(),
        6,
        "reads and failed calls must not publish a version"
    );
    let mut segment_sizes = Vec::new();
    for snapshot in &snapshots {
        let size = snapshot
            .sqlite
            .files
            .iter()
            .map(|file| {
                base64::engine::general_purpose::STANDARD
                    .decode(&file.data)
                    .map(|bytes| bytes.len())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>();
        segment_sizes.push(size);
    }
    assert_eq!(segment_sizes.len(), 6);
    for index in [1, 2, 4, 5] {
        assert!(segment_sizes[index] < segment_sizes[0] / 5);
        assert!(snapshots[index].sqlite.parent.is_some());
    }
    assert!(snapshots[0].sqlite.parent.is_none());
    assert!(snapshots[3].sqlite.parent.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_publishes_the_compiled_contract_before_readiness_and_refreshes_it_on_restart()
-> Result<()> {
    let project = tempfile::Builder::new()
        .prefix("actor's project ")
        .tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let first: Value = runtime.contract().await?.error_for_status()?.json().await?;
    assert_eq!(first["contract"]["actors"][0]["actorName"], "Counter");
    assert_eq!(
        first["contract"]["actors"][0]["rpc"]["methods"][0]["name"],
        "read"
    );
    runtime.stop().await?;

    local_project::write_actor(
        project.path(),
        "async reset(): Promise<number> { return 0 }",
    )?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let second: Value = runtime.contract().await?.error_for_status()?.json().await?;
    assert_eq!(
        second["contract"]["actors"][0]["rpc"]["methods"][0]["name"],
        "reset"
    );
    assert_ne!(second["contractHash"], first["contractHash"]);
    runtime.stop().await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_observability_is_open_while_application_routes_enforce_the_secret() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), Some("optional-secret")).await?;
    assert_eq!(runtime.contract().await?.status(), 401);
    let url = format!("{}/v1/projects/local/deployment/contract", runtime.origin);
    let client = reqwest::Client::new();
    assert_eq!(
        client.get(&url).bearer_auth("wrong").send().await?.status(),
        401
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth("optional-secret")
            .send()
            .await?
            .status(),
        200
    );
    for endpoint in [
        "actors",
        "events",
        "requests",
        "requests/events",
        "metrics",
        "queue-waits",
        "websockets",
    ] {
        let url = format!("{}/v1/projects/local/observe/{endpoint}", runtime.origin);
        for credential in [None, Some("stale-secret")] {
            let mut request = client.get(&url).timeout(Duration::from_secs(5));
            if let Some(credential) = credential {
                request = request.bearer_auth(credential);
            }
            assert_eq!(request.send().await?.status(), 200, "{endpoint}");
        }
    }
    runtime.stop().await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_rejects_an_invalid_actor_contract_before_publishing_readiness() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(
        project.path(),
        "async read(): Promise<Date> { return new Date() }",
    )?;
    let output = timeout(
        Duration::from_secs(20),
        Command::new(env!("CARGO_BIN_EXE_durable-actors"))
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .env("DURABLE_ACTORS_SECRET", "test-key")
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project-id")
            .arg("default")
            .arg("--project")
            .arg(project.path())
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    assert!(!output.status.success());
    let logs = String::from_utf8_lossy(&output.stdout);
    assert!(logs.contains("JSON-compatible"), "{logs}");
    assert!(!logs.contains("  Ready  "), "{logs}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_supports_backend_rpc_and_cli_generation_without_credentials() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    let backend = format!(
        "import {{ createActorTransport }} from {}; const client = createActorTransport({{ controlPlaneUrl: process.argv[1] }}); if (await client.invoke('Counter', 'one', 'read', []) !== 1) throw new Error('unexpected result');",
        serde_json::to_string(&sdk.join("backend.js"))?
    );
    let output = timeout(
        Duration::from_secs(30),
        Command::new("node")
            .args(["--input-type=module", "--eval", &backend, &runtime.origin])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success(),
        "backend failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let consumer = tempfile::tempdir()?;
    let output = Command::new("node")
        .arg(sdk.join("cli.js"))
        .args(["generate"])
        .current_dir(consumer.path())
        .env_remove("DURABLE_ACTORS_PROJECT_ID")
        .env_remove("DURABLE_ACTORS_SECRET")
        .env_remove("DURABLE_ACTORS_API_KEY")
        .env("DURABLE_ACTORS_CONTROL_PLANE_URL", &runtime.origin)
        .kill_on_drop(true)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(consumer.path().join("generated/index.js").exists());
    assert!(consumer.path().join("generated/index.d.ts").exists());
    runtime.stop().await
}

struct LocalRuntime {
    child: Child,
    output: BufReader<ChildStdout>,
    origin: String,
}

impl LocalRuntime {
    async fn start(project: &Path, api_key: Option<&str>) -> Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
        command
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project")
            .arg(project)
            .env_remove("DURABLE_ACTORS_PROJECT_ID")
            .env_remove("DURABLE_ACTORS_SECRET")
            .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(api_key) = api_key {
            command.env("DURABLE_ACTORS_SECRET", api_key);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let origin = timeout(Duration::from_secs(20), async {
            let mut line = String::new();
            let mut logs = String::new();
            let mut origin = None;
            loop {
                ensure!(
                    output.read_line(&mut line).await? != 0,
                    "runtime exited before readiness: {logs}"
                );
                logs.push_str(&line);
                if let Some((_, value)) = line.split_once("  Ready  ") {
                    origin = Some(value.trim().to_owned());
                }
                if line.contains("durable-actors generate") {
                    return origin.context("missing origin");
                }
                line.clear();
            }
        })
        .await??;
        Ok(Self {
            child,
            output,
            origin,
        })
    }

    async fn contract(&self) -> Result<reqwest::Response> {
        Ok(reqwest::Client::new()
            .get(format!(
                "{}/v1/projects/local/deployment/contract",
                self.origin
            ))
            .send()
            .await?)
    }

    async fn stop(mut self) -> Result<()> {
        drop(self.child.stdin.take());
        let mut output = String::new();
        let (status, _) = timeout(Duration::from_secs(5), async {
            tokio::try_join!(self.child.wait(), self.output.read_to_string(&mut output))
        })
        .await??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        Ok(())
    }
}

#![cfg(unix)]

#[path = "support/local_project.rs"]
mod local_project;

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    time::timeout,
};

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn interrupt_exits_while_the_parent_stdin_pipe_is_open() -> Result<()> {
    assert_shutdown(Some("-INT")).await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn terminate_exits_while_the_parent_stdin_pipe_is_open() -> Result<()> {
    assert_shutdown(Some("-TERM")).await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn closing_parent_stdin_stops_the_runtime() -> Result<()> {
    assert_shutdown(None).await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn local_hosts_use_the_configured_idle_timeout() -> Result<()> {
    assert_idle_behavior(
        "@Persisted count = 0;
         async increment(): Promise<number> { return ++this.count }
         async read(): Promise<number> { return this.count }
         async processId(): Promise<number> { return process.pid }",
        "host-idle-client.mjs",
    )
    .await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn idle_actors_rehydrate_with_live_websockets() -> Result<()> {
    assert_idle_behavior(
        r#"
        @Persisted count = 0;
        @Ephemeral instance = crypto.randomUUID();
        async inspect() {
            return { instance: this.instance, count: this.count, pid: process.pid,
                sockets: (await this.getConnections()).map(socket => ({ id: socket.id, metadata: socket.metadata, tags: socket.tags })) };
        }
        async hold() {
            await new Promise(resolve => setTimeout(resolve, 800));
            return this.inspect();
        }
        async onConnect(socket: ActorSocket) {
            this.count++;
            socket.metadata = { user: "restored" };
            socket.setTags("room");
        }
        async onMessage(socket: ActorSocket) {
            this.count++;
            socket.send(await this.inspect());
        }
        "#,
        "socket-idle-client.mjs",
    )
    .await
}

async fn assert_idle_behavior(actor: &str, client: &str) -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), actor)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_durable-actors"))
        .args([
            "dev",
            "--port",
            "0",
            "--entrypoint",
            "actors.ts",
            "--project-id",
            "default",
            "--api-key",
            "test-key",
        ])
        .arg("--sdk-host")
        .arg(local_project::sdk_host())
        .arg("--project")
        .arg(project.path())
        .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
        .env("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", "500")
        .env("RUST_LOG", "info")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
    let origin = timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
    let logs = tokio::spawn(async move {
        let mut remaining = String::new();
        output.read_to_string(&mut remaining).await?;
        Ok::<_, std::io::Error>(remaining)
    });
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = timeout(
        Duration::from_secs(20),
        Command::new("node")
            .arg(root.join("tests/fixtures").join(client))
            .arg(root.join("sdk/dist/client/remoteClient.js"))
            .env("DURABLE_ACTORS_CONTROL_PLANE_URL", origin)
            .env("DURABLE_ACTORS_PROJECT_ID", "default")
            .env("DURABLE_ACTORS_SECRET", "test-key")
            .env("DURABLE_ACTORS_TELEMETRY", "0")
            .kill_on_drop(true)
            .output(),
    )
    .await;
    drop(child.stdin.take());
    let status = timeout(Duration::from_secs(10), child.wait()).await??;
    let logs = logs.await??;
    ensure!(
        status.success(),
        "local runtime shutdown failed: {status}: {logs}"
    );
    let result = result??;
    ensure!(
        result.status.success(),
        "idle host calls failed: {}\n{logs}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(())
}

async fn assert_shutdown(signal: Option<&str>) -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_durable-actors"))
        .args([
            "dev",
            "--port",
            "0",
            "--entrypoint",
            "actors.ts",
            "--api-key",
            "test-key",
        ])
        .arg("--sdk-host")
        .arg(local_project::sdk_host())
        .arg("--project-id")
        .arg("default")
        .arg("--project")
        .arg(project.path())
        .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
        .env("RUST_LOG", "info")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut parent_stdin = child.stdin.take();
    let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
    timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Some(signal) = signal {
        let status = Command::new("kill")
            .args([
                signal,
                &child.id().context("runtime exited early")?.to_string(),
            ])
            .status()
            .await?;
        ensure!(status.success(), "send runtime shutdown signal");
    } else {
        drop(parent_stdin.take());
    }

    let status = timeout(Duration::from_secs(3), child.wait())
        .await
        .context("runtime did not exit after shutdown")??;
    ensure!(status.success(), "runtime exited with {status}");
    drop(parent_stdin);
    let mut remaining = String::new();
    output.read_to_string(&mut remaining).await?;
    let message = if signal.is_some() {
        "shutdown signal received"
    } else {
        "parent process exited"
    };
    ensure!(
        remaining.contains(message),
        "shutdown was not handled: {remaining}"
    );
    Ok(())
}

async fn wait_until_ready(output: &mut BufReader<tokio::process::ChildStdout>) -> Result<String> {
    let mut line = String::new();
    let mut startup = String::new();
    loop {
        ensure!(
            output.read_line(&mut line).await? != 0,
            "runtime exited before readiness: {startup}"
        );
        startup.push_str(&line);
        if let Some((_, origin)) = line.split_once("  Ready  ") {
            return Ok(origin.trim().to_owned());
        }
        line.clear();
    }
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn local_deployments_reload_code_and_preserve_state_across_restarts() -> Result<()> {
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let project = tempfile::tempdir_in(&sdk)?;
    let shell_directory = tempfile::tempdir()?;
    std::fs::write(
        project.path().join("actors.ts"),
        format!(
            r#"
        import {{ Actor, Persisted }} from {};
        export class Counter extends Actor {{
            @Persisted value = 0;
            async read() {{ return this.value; }}
            async increment() {{ return ++this.value; }}
            async label() {{ return "before"; }}
        }}
    "#,
            serde_json::to_string(&sdk.join("dist/index.js"))?
        ),
    )?;
    std::fs::write(
        project.path().join("tsconfig.json"),
        r#"{"compilerOptions":{"target":"ES2022","module":"NodeNext","moduleResolution":"NodeNext","strict":true,"skipLibCheck":true},"include":["actors.ts"]}"#,
    )?;
    let script = project.path().join("invoke.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
        import assert from 'node:assert/strict';
        import {{ RemoteActorClient }} from {};
        let client = new RemoteActorClient();
        const before = Number(process.argv[2]);
        const concurrent = await Promise.all(Array.from({{ length: 4 }}, () => new RemoteActorClient().invoke('Counter', 'one', 'read', [])));
        assert.deepEqual(concurrent, [before, before, before, before]);
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before);
        assert.equal(await client.invoke('Counter', 'one', 'increment', []), before + 1);
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before + 1);
        const origin = process.env.DURABLE_ACTORS_CONTROL_PLANE_URL;
        const apiKey = process.env.DURABLE_ACTORS_SECRET;
        const workingDirectory = (await import('node:path')).dirname((await import('node:url')).fileURLToPath(import.meta.url));
        assert.equal(await client.invoke('Counter', 'one', 'label', []), 'before');
        const fs = await import('node:fs/promises');
        const source = new URL('actors.ts', import.meta.url);
        const original = await fs.readFile(source, 'utf8');
        await fs.writeFile(source, 'this is invalid TypeScript');
        assert.equal(await new RemoteActorClient().invoke('Counter', 'fresh', 'label', []), 'before');
        const rejected = await fetch(`${{origin}}/v1/projects/default/deployment`, {{
            method: 'PUT', headers: {{ authorization: `Bearer ${{apiKey}}`, 'content-type': 'application/json' }},
            body: JSON.stringify({{ imageRef: 'local', workingDirectory, actorEntrypoint: 'actors.ts', secretRefs: [] }})
        }});
        assert.notEqual(rejected.status, 200);
        assert.equal(await new RemoteActorClient().invoke('Counter', 'after-error', 'label', []), 'before');
        await fs.writeFile(source, original.replace('return "before"', 'return "after"'));
        const deployed = await fetch(`${{origin}}/v1/projects/default/deployment`, {{
            method: 'PUT', headers: {{ authorization: `Bearer ${{apiKey}}`, 'content-type': 'application/json' }},
            body: JSON.stringify({{ imageRef: 'local', workingDirectory, actorEntrypoint: 'actors.ts', secretRefs: [] }})
        }});
        assert.equal(deployed.status, 200, await deployed.text());
        client = new RemoteActorClient();
        assert.equal(await client.invoke('Counter', 'one', 'label', []), 'after');
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before + 1);
        await fs.writeFile(source, original);
        for (const [projectId, increment] of [['team-a', 1], ['team-b', 2]]) {{
            const response = await fetch(`${{origin}}/v1/projects/${{projectId}}/deployment`, {{
                method: 'PUT', headers: {{ authorization: `Bearer ${{apiKey}}`, 'content-type': 'application/json' }},
                body: JSON.stringify({{ imageRef: 'local', workingDirectory, actorEntrypoint: 'actors.ts', secretRefs: [] }})
            }});
            assert.equal(response.status, 200, await response.text());
            const scoped = new RemoteActorClient({{ projectId, controlPlaneUrl: origin, apiKey }});
            assert.equal(await scoped.invoke('Counter', 'one', 'read', []), before * increment);
            for (let i = 0; i < increment; i++) await scoped.invoke('Counter', 'one', 'increment', []);
            assert.equal(await scoped.invoke('Counter', 'one', 'read', []), (before + 1) * increment);
        }}
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before + 1);
        const response = await fetch(`${{origin}}/v1/projects/default/actors/Counter/one/find-websocket`, {{
            method: 'POST', headers: {{ authorization: `Bearer ${{apiKey}}`, 'content-type': 'application/json' }},
            body: JSON.stringify({{ metadata: null }})
        }});
        const grant = await response.json();
        assert.equal(response.status, 200, JSON.stringify(grant));
        const ticket = new URL(grant.websocketUrl).searchParams.get('key');
        const claims = JSON.parse(Buffer.from(ticket.split('.')[1], 'base64url'));
        assert.equal(claims.actor.project_id, 'default');

    "#,
            serde_json::to_string(&sdk.join("dist/client/remoteClient.js"))?
        ),
    )?;

    for before in 0..2 {
        let mut runtime = Command::new(env!("CARGO_BIN_EXE_durable-actors"))
            .args(["dev", "--sdk-host"])
            .arg(sdk.join("dist/host.js"))
            .current_dir(shell_directory.path())
            .env("DURABLE_ACTORS_PROJECT", project.path())
            .env("DURABLE_ACTORS_ENTRYPOINT", "actors.ts")
            .env("DURABLE_ACTORS_PORT", "0")
            .env("DURABLE_ACTORS_DATA_DIR", "state")
            .env("DURABLE_ACTORS_STORAGE", "local")
            .env("DURABLE_ACTORS_PROJECT_ID", "default")
            .env("DURABLE_ACTORS_SECRET", "local-test-key")
            .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut output = BufReader::new(runtime.stdout.take().context("runtime stdout")?);
        let origin = timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
        let logs = tokio::spawn(async move {
            let mut remaining = String::new();
            output.read_to_string(&mut remaining).await?;
            Ok::<_, std::io::Error>(remaining)
        });
        assert!(!shell_directory.path().join("state/runtime.json").exists());
        let result = timeout(
            // Each pass runs four builds and starts hosts for three projects.
            Duration::from_secs(60),
            Command::new("node")
                .arg(&script)
                .arg(before.to_string())
                .env("DURABLE_ACTORS_CONTROL_PLANE_URL", origin)
                .env("DURABLE_ACTORS_PROJECT_ID", "default")
                .env("DURABLE_ACTORS_SECRET", "local-test-key")
                .env("DURABLE_ACTORS_TELEMETRY", "0")
                .kill_on_drop(true)
                .output(),
        )
        .await;
        drop(runtime.stdin.take());
        let status = timeout(Duration::from_secs(10), runtime.wait()).await??;
        let logs = logs.await??;
        ensure!(
            status.success(),
            "local runtime shutdown failed: {status}\n{logs}"
        );
        let result = result.with_context(|| {
            format!("deployment checks timed out on restart pass {before}\n{logs}")
        })??;
        ensure!(
            result.status.success(),
            "actor calls failed: {}\n{logs}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert!(
        shell_directory
            .path()
            .join("state/objects/durable-actors/v3")
            .is_dir()
    );
    Ok(())
}

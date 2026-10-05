use super::*;
use crate::{
    actor::ActorKey,
    bucket::{Bucket, FileBucket},
    control_plane::ActorJwtIssuer,
    host::HostId,
};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{collections::HashMap, path::PathBuf, process::Stdio};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires Bun and pnpm --dir sdk build"]
async fn generic_bun_host_restores_committed_state_before_becoming_ready() -> Result<()> {
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let project = tempfile::tempdir_in(&sdk)?;
    let data = tempfile::tempdir()?;
    let artifact = compile_counter(&sdk, project.path()).await?;
    let issuer = issuer()?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    for before in -1..2 {
        run_activation(
            project.path(),
            data.path(),
            &sdk,
            &artifact,
            &issuer,
            &actor,
            before,
        )
        .await?;
    }
    Ok(())
}

async fn run_activation(
    project: &Path,
    data: &Path,
    sdk: &Path,
    artifact: &[u8],
    issuer: &ActorJwtIssuer,
    actor: &ActorKey,
    before: i64,
) -> Result<()> {
    let socket_dir = tempfile::tempdir_in("/tmp")?;
    let socket = socket_dir.path().join("executor.sock");
    let code = project.join(format!("assigned-{before}.mjs"));
    let ready = project.join(if before < 0 {
        "missing/ready".into()
    } else {
        format!("ready-{before}")
    });
    let host_id = HostId::new(format!("host.v3.test.{}", uuid::Uuid::new_v4()));
    let actor_spool = std::env::temp_dir().join(format!("durable-actors-{host_id}"));
    assert!(!actor_spool.exists());
    let session = uuid::Uuid::new_v4().to_string();
    let token = issuer
        .issue_host(&host_id, &session, "test", "north-america-east", actor)?
        .token;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}", listener.local_addr()?);
    let ipc = ActorExecutorListener::bind(&socket).await?;
    let javascript = Command::new("bun")
        .args([
            "--eval",
            "await import(process.env.DURABLE_ACTORS_SDK_HOST).then(m => m.runGenericHost())",
        ])
        .env("DURABLE_ACTORS_SDK_HOST", sdk.join("dist/host.js"))
        .env("DURABLE_ACTORS_EXECUTOR_SOCKET", &socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let executor = tokio::time::timeout(Duration::from_secs(10), ipc.accept_warm()).await??;
    assert!(
        !code.exists(),
        "generic executor must warm without customer code"
    );
    let environment: HashMap<String, String> = serde_json::from_value(serde_json::json!({
        "DURABLE_ACTORS_CONTROL_PLANE_URL": "http://127.0.0.1:1",
        "DURABLE_ACTORS_HOST_TOKEN": token,
        "DURABLE_ACTORS_JWT_PUBLIC_KEYS": issuer.verifier_keys_json()?,
        "DURABLE_ACTORS_HOST_ID": host_id.as_str(), "DURABLE_ACTORS_SESSION_ID": session,
        "DURABLE_ACTORS_HOST_ROUTE": route, "DURABLE_ACTORS_JWT_ISSUER": "issuer",
        "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE": "invocation",
        "DURABLE_ACTORS_HOST_READY_FILE": ready.to_str().unwrap(),
        "DURABLE_ACTORS_ACTOR": serde_json::to_string(actor)?,
        "DURABLE_ACTORS_ACTOR_IS_NEW": (before < 0).to_string(),
        "DURABLE_ACTORS_RUNTIME_CONFIG": serde_json::json!({
            "bucket": {"type": "file", "directory": data}, "region": "north-america-east", "persistence": {"type":"local"},
            "token": null
        }).to_string()
    }))?;
    let config = ActorHostConfig::from_lookup(|key| environment.get(key).cloned())?;
    let (readiness, ready_response) = tokio::sync::oneshot::channel();
    let warm = WarmHost {
        control_plane: None,
        readiness: Some(readiness),
        listener,
        executor,
        javascript,
        entrypoint: code.to_str().unwrap().into(),
        storage: WarmGcs::new().await?,
        replication: Arc::new(Litestream::default()),
    };
    let stop = CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    tokio::fs::write(&code, artifact).await?;
    let task = tokio::spawn(serve_assigned_host(
        config,
        Some(warm),
        stop.clone().cancelled_owned(),
    ));
    let bucket = FileBucket::new(data.to_path_buf())?;
    let owner = crate::storage_paths::owner(&actor.storage_key())?;
    if before < 0 {
        assert!(
            tokio::time::timeout(Duration::from_secs(10), task)
                .await??
                .is_err()
        );
        assert!(
            ready_response.await.is_err(),
            "failed activation reported ready"
        );
        let record: serde_json::Value =
            serde_json::from_slice(&bucket.get(&owner).await?.unwrap().bytes)?;
        assert_eq!(
            record["lease"]["expires_at_ms"], 0,
            "failed startup must release its lease"
        );
        return Ok(());
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while !ready.exists() {
            ensure!(!task.is_finished(), "host exited before hydration");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    let client = reqwest::Client::new();
    let actor_url = format!(
        "{route}/v1/projects/{}/actors/{}/{}",
        actor.project_id, actor.actor_name, actor.actor_id
    );
    let response = tokio::time::timeout(Duration::from_secs(1), ready_response).await??;
    assert_eq!(response.host_id, host_id);
    assert_eq!(response.session_id, session);
    let epoch = response.owner_epoch;
    assert!(epoch > 0);
    let marker: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&ready).await?)?;
    assert_eq!(marker["ownerEpoch"], epoch);
    for (method, expected) in [
        ("read", before),
        ("increment", before + 1),
        ("read", before + 1),
    ] {
        let result: serde_json::Value = client.post(format!("{actor_url}/invoke")).bearer_auth(&token).json(&serde_json::json!({"requestId":uuid::Uuid::new_v4().to_string(), "ownerEpoch":epoch, "routingMs":0.0, "method":method, "args":[]})).send().await?.error_for_status()?.json().await?;
        assert_eq!(result["type"], "completed");
        assert_eq!(result["result"], expected);
        let metadata = &result["metadata"];
        let duration = metadata["durationMs"].as_f64().unwrap();
        let queue_wait = metadata["queueWaitMs"].as_f64().unwrap();
        assert!(duration.is_finite() && duration >= 0.0);
        assert!(queue_wait.is_finite() && queue_wait >= 0.0 && queue_wait <= duration);
        assert_eq!(metadata["hostState"], "warm");
    }
    assert_eq!(client.post(format!("{route}/v1/projects/{}/actors/{}/other/invoke", actor.project_id, actor.actor_name)).bearer_auth(&token).json(&serde_json::json!({"requestId":"wrong-actor", "ownerEpoch":epoch, "routingMs":0.0, "method":"read", "args":[]})).send().await?.status(), reqwest::StatusCode::FORBIDDEN);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), task).await???;
    assert!(
        !actor_spool.exists(),
        "actor host created a local snapshot spool"
    );
    Ok(())
}

fn issuer() -> Result<ActorJwtIssuer> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(1800),
    )
}

async fn compile_counter(sdk: &Path, project: &Path) -> Result<Vec<u8>> {
    let source = project.join("actors.ts");
    let compiled = project.join("actors.mjs");
    tokio::fs::write(&source, "import { Actor, Persisted } from 'durable-actors'; export class Counter extends Actor { @Persisted value = 0; async read() { return this.value; } async increment() { return ++this.value; } }").await?;
    tokio::fs::write(project.join("tsconfig.json"), r#"{"compilerOptions":{"target":"ES2022","module":"NodeNext","moduleResolution":"NodeNext","strict":true,"skipLibCheck":true},"include":["actors.ts"]}"#).await?;
    let result = Command::new("node").args(["--input-type=module", "--eval", "const { buildActor } = await import(process.argv[1]); await buildActor(process.argv[2], process.argv[3]);"])
        .arg(sdk.join("dist/compiler/actor-build.js")).arg(source).arg(&compiled).output().await?;
    ensure!(
        result.status.success(),
        "compile fixture: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(tokio::fs::read(compiled).await?)
}

#[tokio::test]
async fn control_plane_failure_prevents_spare_readiness() {
    assert!(
        prewarm_control_plane(Some("http://127.0.0.1:1".into()))
            .await
            .is_err()
    );
    assert!(prewarm_control_plane(None).await.is_err());
}

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use durable_actors::state_log::SqliteSnapshot;
use serde_json::{Value, json};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

pub fn snapshot(fields: Value) -> Result<SqliteSnapshot> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(capture(fields))
    })
    .join()
    .expect("SQLite fixture thread")
}

async fn capture(fields: Value) -> Result<SqliteSnapshot> {
    let directory = tempfile::Builder::new()
        .prefix("sqlite-fixture-")
        .tempdir_in("/tmp")?;
    let path = directory.path().join("actor.sqlite");
    let replica = directory.path().join("replica");
    let socket = directory.path().join("control.sock");
    let config = directory.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({
            "socket": {"enabled": true, "path": socket}, "levels": []
        }))?,
    )?;
    let _process = Process(
        Command::new("litestream")
            .args(["replicate", "-config"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let client = reqwest::Client::builder()
        .unix_socket(socket)
        .timeout(Duration::from_secs(10))
        .build()?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client.get("http://localhost/info").send().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))")?;
    for (name, value) in fields.as_object().context("object fixture")? {
        database.execute(
            "INSERT INTO __terse_fields VALUES (?, ?)",
            [name.as_str(), &value.to_string()],
        )?;
    }
    client
        .post("http://localhost/register")
        .json(&json!({"path": path, "replica_url": format!("file://{}", replica.display())}))
        .send()
        .await?
        .error_for_status()?;
    let reply: Value = client
        .post("http://localhost/sync")
        .json(&json!({"path": path, "wait": true, "timeout": 10}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let txid = reply["txid"].as_u64().context("fixture transaction")?;
    let mut files = Vec::new();
    for entry in std::fs::read_dir(replica.join("ltx/0"))? {
        let path = entry?.path();
        if path.extension().is_none_or(|value| value != "ltx") {
            continue;
        }
        let name = path.file_stem().unwrap().to_str().unwrap();
        let (first, last) = name.split_once('-').context("LTX filename")?;
        files.push(json!({"level": 0, "first": u64::from_str_radix(first, 16)?, "last": u64::from_str_radix(last, 16)?, "data": STANDARD.encode(std::fs::read(path)?)}));
    }
    files.sort_by_key(|file| file["first"].as_u64().unwrap());
    ensure!(!files.is_empty(), "fixture replication files");
    Ok(serde_json::from_value(
        json!({"txid": txid, "files": files}),
    )?)
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

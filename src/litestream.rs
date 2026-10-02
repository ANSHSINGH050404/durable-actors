use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};

pub(crate) mod compaction;
pub(crate) mod storage;

pub(crate) const VERSION: &str = "0.5.17";

#[async_trait]
pub(crate) trait Replicator: DatabaseRestore {
    fn socket(&self) -> &Path;
    async fn register(&self, path: &Path, replica: &Path) -> Result<()>;
    async fn sync(&self, path: &Path) -> Result<u64>;
    async fn unregister(&self, path: &Path) -> Result<()>;
}

#[async_trait]
pub(crate) trait DatabaseRestore: Send + Sync {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()>;
}

pub(crate) struct RestoreCommand(pub(crate) PathBuf);

pub(crate) struct Litestream {
    client: reqwest::Client,
    socket: PathBuf,
    binary: PathBuf,
    _process: Child,
    _directory: tempfile::TempDir,
}

impl Litestream {
    pub(crate) async fn start(binary: PathBuf) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("terse-ls-")
            .tempdir_in("/tmp")?;
        let socket = directory.path().join("control.sock");
        let config = directory.path().join("litestream.yml");
        tokio::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({
                "socket": {"enabled": true, "path": socket, "permissions": 384},
                "retention": {"enabled": true},
                "levels": [],
                "l0-retention": "1m",
                "snapshot": {"interval": "1m", "retention": "1h"}
            }))?,
        )
        .await?;
        let process = Command::new(&binary)
            .args(["replicate", "-config"])
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("start Litestream")?;
        let mut daemon = Self {
            client: reqwest::Client::builder()
                .unix_socket(socket.clone())
                .timeout(Duration::from_secs(35))
                .build()?,
            socket,
            binary,
            _process: process,
            _directory: directory,
        };
        daemon.wait_ready().await?;
        Ok(daemon)
    }

    async fn wait_ready(&mut self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                ensure!(
                    self._process.try_wait()?.is_none(),
                    "Litestream exited before becoming ready"
                );
                if let Ok(response) = self.client.get("http://localhost/info").send().await {
                    let info: Value = response.error_for_status()?.json().await?;
                    ensure!(info["version"] == VERSION, "unsupported Litestream version");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("Litestream IPC did not become ready")?
    }

    async fn request(&self, endpoint: &str, body: Value) -> Result<Value> {
        self.client
            .post(format!("http://localhost/{endpoint}"))
            .json(&body)
            .send()
            .await
            .context("connect to Litestream IPC")?
            .error_for_status()
            .context("Litestream IPC request failed")?
            .json()
            .await
            .context("decode Litestream IPC response")
    }
}

#[async_trait]
impl Replicator for Litestream {
    fn socket(&self) -> &Path {
        &self.socket
    }

    async fn register(&self, path: &Path, replica: &Path) -> Result<()> {
        let reply = self
            .request(
                "register",
                serde_json::json!({
                    "path": path, "replica_url": file_url(replica)?.as_str()
                }),
            )
            .await?;
        ensure!(
            reply["status"] == "registered",
            "Litestream database was already registered"
        );
        ensure!(
            reply["path"].as_str() == path.to_str(),
            "Litestream registered another database"
        );
        Ok(())
    }

    async fn sync(&self, path: &Path) -> Result<u64> {
        let reply = self
            .request(
                "sync",
                serde_json::json!({
                    "path": path, "wait": true, "timeout": 30
                }),
            )
            .await?;
        Ok(SyncPosition::decode(&reply, path.to_str().context("invalid SQLite path")?)?.txid)
    }

    async fn unregister(&self, path: &Path) -> Result<()> {
        self.request(
            "unregister",
            serde_json::json!({"path": path, "timeout": 30}),
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl DatabaseRestore for Litestream {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        RestoreCommand(self.binary.clone())
            .restore(replica, path, txid)
            .await
    }
}

#[async_trait]
impl DatabaseRestore for RestoreCommand {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        ensure!(txid > 0, "cannot restore an empty Litestream position");
        let mut command = Command::new(&self.0);
        command
            .args(["restore", "-txid", &format!("{txid:016x}"), "-o"])
            .arg(path)
            .arg(file_url(replica)?.as_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(120), command.output())
            .await
            .context("Litestream restore timed out")??;
        ensure!(
            output.status.success(),
            "Litestream restore failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}

fn file_url(path: &Path) -> Result<reqwest::Url> {
    reqwest::Url::from_directory_path(path)
        .map_err(|_| anyhow::anyhow!("invalid replica directory"))
}

struct SyncPosition {
    txid: u64,
}

impl SyncPosition {
    fn decode(reply: &Value, path: &str) -> Result<Self> {
        ensure!(
            reply["path"].as_str() == Some(path),
            "Litestream synced another database"
        );
        let txid = reply["txid"]
            .as_u64()
            .context("Litestream sync position missing")?;
        ensure!(txid > 0, "Litestream sync position is empty");
        let replicated = reply["replicated_txid"]
            .as_u64()
            .context("Litestream replica position missing")?;
        ensure!(
            replicated >= txid,
            "Litestream replica has not reached the committed transaction"
        );
        Ok(Self { txid })
    }
}

#[cfg(test)]
#[path = "../tests/unit/litestream.rs"]
mod tests;

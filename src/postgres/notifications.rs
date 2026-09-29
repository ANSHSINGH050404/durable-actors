use super::{MakeTlsConnector, PostgresDatabase, TlsConnector};
use anyhow::{Context, Result};
use std::{str::FromStr, time::Duration};
use tokio::sync::{oneshot, watch};
use tokio_postgres::{AsyncMessage, Config, Connection, NoTls, Socket, config::SslMode};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

#[derive(Clone)]
pub(crate) struct ChangeFeed {
    changes: watch::Sender<()>,
    database: Option<PostgresDatabase>,
}

impl Default for ChangeFeed {
    fn default() -> Self {
        Self {
            changes: watch::channel(()).0,
            database: None,
        }
    }
}

impl ChangeFeed {
    pub async fn postgres(
        database: PostgresDatabase,
        url: &str,
        stop: CancellationToken,
    ) -> Result<Self> {
        let config = Config::from_str(url)?;
        let stop = stop.child_token();
        let startup = stop.clone().drop_guard();
        let changes = watch::channel(()).0;
        let feed = Self {
            changes: changes.clone(),
            database: Some(database),
        };
        let (ready, listening) = oneshot::channel();
        tokio::spawn(async move {
            let mut ready = Some(ready);
            loop {
                let result = tokio::select! {
                    _ = stop.cancelled() => return,
                    result = connect(&config, changes.clone(), &mut ready, stop.clone()) => result,
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "PostgreSQL change listener disconnected; polling remains active");
                }
                tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        });
        tokio::time::timeout(Duration::from_secs(10), listening)
            .await
            .context("PostgreSQL change listener startup timed out")??;
        startup.disarm();
        Ok(feed)
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    pub async fn notify(&self) {
        self.changes.send_replace(());
        if let Some(database) = &self.database {
            if let Err(error) = database
                .execute("SELECT pg_notify('durable_actors_changes','')", &[])
                .await
            {
                tracing::warn!(%error, "change notification failed; polling remains active");
            }
        }
    }
}

async fn connect(
    config: &Config,
    changes: watch::Sender<()>,
    ready: &mut Option<oneshot::Sender<()>>,
    stop: CancellationToken,
) -> Result<()> {
    match config.get_ssl_mode() {
        SslMode::Disable => {
            let (client, connection) = config.connect(NoTls).await?;
            listen(client, connection, changes, ready, stop).await
        }
        _ => {
            let tls = MakeTlsConnector::new(TlsConnector::builder().build()?);
            let (client, connection) = config.connect(tls).await?;
            listen(client, connection, changes, ready, stop).await
        }
    }
}

async fn listen<S>(
    client: tokio_postgres::Client,
    mut connection: Connection<Socket, S>,
    changes: watch::Sender<()>,
    ready: &mut Option<oneshot::Sender<()>>,
    stop: CancellationToken,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let signals = changes.clone();
    let mut driver = AbortOnDropHandle::new(tokio::spawn(async move {
        while let Some(message) =
            futures_util::future::poll_fn(|cx| connection.poll_message(cx)).await
        {
            if matches!(message?, AsyncMessage::Notification(_)) {
                signals.send_replace(());
            }
        }
        anyhow::bail!("PostgreSQL change connection closed")
    }));
    client
        .batch_execute("LISTEN durable_actors_changes")
        .await?;
    changes.send_replace(());
    if let Some(ready) = ready.take() {
        let _ = ready.send(());
    }
    tokio::select! { _ = stop.cancelled() => Ok(()), result = &mut driver => result? }
}

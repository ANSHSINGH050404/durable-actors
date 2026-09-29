use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, NoTls, Row, config::SslMode, types::ToSql};

pub(crate) mod notifications;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[cfg(test)]
#[path = "../tests/support/postgres.rs"]
pub(crate) mod testing;

#[derive(Clone)]
pub(crate) struct PostgresDatabase {
    pool: Pool,
    initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl PostgresDatabase {
    pub(crate) async fn connection(&self) -> Result<deadpool_postgres::Object> {
        self.initialize().await?;
        self.pool
            .get()
            .await
            .context("acquire PostgreSQL connection")
    }

    #[cfg(test)]
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let database = Self::lazy(url)?;
        database.initialize().await?;
        Ok(database)
    }

    pub(crate) fn lazy(url: &str) -> Result<Self> {
        Ok(Self {
            pool: connection_pool(url)?,
            initialized: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    async fn initialize(&self) -> Result<()> {
        self.initialized
            .get_or_try_init(|| migrate(&self.pool))
            .await?;
        Ok(())
    }

    pub(crate) async fn query_opt(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_opt(&statement, params).await?)
    }

    #[cfg(test)]
    pub(crate) async fn query_one(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_one(&statement, params).await?)
    }

    pub(crate) async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.execute(&statement, params).await?)
    }
}

async fn migrate(pool: &Pool) -> Result<()> {
    let client = pool.get().await.context("connect to PostgreSQL")?;
    // Closing this dedicated session releases the lock even on errors or cancellation.
    let mut client = deadpool_postgres::Object::take(client);
    client
        .query_one(
            "SELECT pg_advisory_lock(hashtext('durable-actors'), hashtext('schema-migrations'))",
            &[],
        )
        .await
        .context("lock durable-actors PostgreSQL migrations")?;
    embedded::migrations::runner()
        .run_async(&mut *client)
        .await
        .context("run durable-actors PostgreSQL migrations")?;
    Ok(())
}

fn connection_pool(url: &str) -> Result<Pool> {
    let config = Config::from_str(url).context("parse PostgreSQL connection URL")?;
    let manager = match config.get_ssl_mode() {
        SslMode::Disable => Manager::new(config, NoTls),
        _ => {
            let connector = TlsConnector::builder()
                .build()
                .context("build PostgreSQL TLS connector")?;
            Manager::new(config, MakeTlsConnector::new(connector))
        }
    };
    Ok(Pool::builder(manager)
        .max_size(8)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .build()?)
}

#[cfg(test)]
#[path = "../tests/unit/postgres.rs"]
mod tests;

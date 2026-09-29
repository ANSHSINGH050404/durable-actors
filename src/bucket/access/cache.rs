use super::{StorageToken, StorageTokenSource};
use crate::postgres::PostgresDatabase;
use anyhow::{Context, Result};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct SharedTokens {
    database: PostgresDatabase,
    issuer: String,
    source: Arc<dyn StorageTokenSource>,
}

impl SharedTokens {
    pub fn new(
        database: PostgresDatabase,
        issuer: String,
        source: Arc<dyn StorageTokenSource>,
    ) -> Self {
        Self {
            database,
            issuer,
            source,
        }
    }

    pub async fn issue(&self, boundary: &Value) -> Result<StorageToken> {
        tokio::time::timeout(Duration::from_secs(65), async {
            loop {
                if let Some(token) = self.obtain(boundary, true).await? {
                    return Ok(token);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("scoped credential refresh timed out")?
    }

    pub fn start(&self, stop: CancellationToken) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = stop.cancelled() => return, _ = tick.tick() => {} }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    result = cache.maintain() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "credential cache maintenance deferred");
                        }
                    }
                }
            }
        });
    }

    async fn obtain(&self, boundary: &Value, touch: bool) -> Result<Option<StorageToken>> {
        let key = key(&self.issuer, boundary)?;
        let claim = uuid::Uuid::new_v4().to_string();
        let row = self
            .database
            .connection()
            .await?
            .query_one(
                include_str!("claim.sql"),
                &[&key, &self.issuer, &boundary, &claim, &touch],
            )
            .await?;
        let token = row
            .get::<_, Option<String>>(0)
            .map(|access_token| StorageToken {
                access_token,
                expires_at_ms: row.get::<_, i64>(1) as u64,
            });
        let usable: bool = row.get(2);
        let claimed = row.get::<_, Option<String>>(3).as_deref() == Some(&claim);
        if usable {
            if claimed {
                let cache = self.clone();
                let boundary = boundary.clone();
                tokio::spawn(async move {
                    if let Err(error) = cache.refresh(&key, &claim, &boundary).await {
                        tracing::warn!(%error, "scoped credential refresh failed");
                    }
                });
            }
            return Ok(token);
        }
        if claimed {
            return self.refresh(&key, &claim, boundary).await;
        }
        Ok(None)
    }

    async fn refresh(
        &self,
        key: &str,
        claim: &str,
        boundary: &Value,
    ) -> Result<Option<StorageToken>> {
        let exchanged =
            tokio::time::timeout(Duration::from_secs(25), self.source.exchange(boundary))
                .await
                .context("GCS credential exchange timed out")
                .and_then(|result| result);
        let token = match exchanged {
            Ok(token) => token,
            Err(error) => {
                self.database.execute("UPDATE durable_actors_storage_tokens SET refresh_owner=NULL,refresh_until=clock_timestamp()+interval '10 seconds' WHERE scope_key=$1 AND refresh_owner=$2", &[&key, &claim]).await?;
                return Err(error);
            }
        };
        // A late response cannot replace the token issued by a newer refresh claimant.
        let changed = self.database.execute(
            "UPDATE durable_actors_storage_tokens SET token=$3,expires_at=to_timestamp($4::double precision/1000),refresh_owner=NULL,refresh_until=to_timestamp(0) WHERE scope_key=$1 AND refresh_owner=$2 AND refresh_until>clock_timestamp()",
            &[&key, &claim, &token.access_token, &(token.expires_at_ms as f64)]
        ).await?;
        Ok((changed == 1).then_some(token))
    }

    async fn maintain(&self) -> Result<()> {
        self.database.execute("DELETE FROM durable_actors_storage_tokens WHERE expires_at<=clock_timestamp() AND refresh_until<=clock_timestamp()", &[]).await?;
        let rows = self.database.connection().await?.query(
            "SELECT boundary FROM durable_actors_storage_tokens WHERE issuer=$1 AND expires_at<clock_timestamp()+interval '5 minutes' AND refresh_until<=clock_timestamp() AND last_used_at>clock_timestamp()-interval '1 hour'", &[&self.issuer]
        ).await?;
        for row in rows {
            self.obtain(&row.get::<_, Value>(0), false).await?;
        }
        Ok(())
    }
}

fn key(issuer: &str, boundary: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(&(issuer, boundary))?;
    Ok(
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/credential_cache.rs"]
mod tests;

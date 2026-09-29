use super::*;
use crate::{clock::Clock, postgres::testing::with_postgres};

struct Source(
    tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<Result<StorageToken>>>,
);
#[async_trait::async_trait]
impl StorageTokenSource for Source {
    async fn exchange(&self, _: &Value) -> Result<StorageToken> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(tx)?;
        rx.await?
    }
}
fn token(value: &str) -> StorageToken {
    StorageToken {
        access_token: value.into(),
        expires_at_ms: crate::clock::SystemClock.now_ms().unwrap() + 600_000,
    }
}

#[tokio::test]
async fn independent_instances_share_tokens_without_crossing_permission_scopes() -> Result<()> {
    with_postgres(async |fixture| {
        let (tx, mut exchanges) = tokio::sync::mpsc::unbounded_channel();
        let source = Arc::new(Source(tx));
        let first = SharedTokens::new(
            PostgresDatabase::connect(&fixture.url).await?,
            "issuer".into(),
            source.clone(),
        );
        let second = SharedTokens::new(
            PostgresDatabase::connect(&fixture.url).await?,
            "issuer".into(),
            source.clone(),
        );
        let other_issuer = SharedTokens::new(
            PostgresDatabase::connect(&fixture.url).await?,
            "other-issuer".into(),
            source,
        );
        let scope = serde_json::json!({"actor":"one"});
        let a = tokio::spawn({
            let first = first.clone();
            let scope = scope.clone();
            async move { first.issue(&scope).await }
        });
        let exchange = exchanges.recv().await.context("exchange")?;
        let b = tokio::spawn({
            let second = second.clone();
            let scope = scope.clone();
            async move { second.issue(&scope).await }
        });
        exchange.send(Ok(token("shared"))).unwrap();
        assert_eq!(a.await??.access_token, "shared");
        assert_eq!(b.await??.access_token, "shared");
        assert!(exchanges.try_recv().is_err());
        let different =
            tokio::spawn(async move { second.issue(&serde_json::json!({"actor":"two"})).await });
        exchanges
            .recv()
            .await
            .context("other scope")?
            .send(Ok(token("different")))
            .unwrap();
        assert_eq!(different.await??.access_token, "different");
        let different_issuer = tokio::spawn(async move { other_issuer.issue(&scope).await });
        exchanges
            .recv()
            .await
            .context("other issuer")?
            .send(Ok(token("other-issuer")))
            .unwrap();
        assert_eq!(different_issuer.await??.access_token, "other-issuer");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn refresh_keeps_valid_tokens_available_and_fences_an_expired_claim() -> Result<()> {
    with_postgres(async |fixture| {
        let (tx, mut exchanges) = tokio::sync::mpsc::unbounded_channel();
        let cache = SharedTokens::new(
            PostgresDatabase::connect(&fixture.url).await?,
            "issuer".into(),
            Arc::new(Source(tx)),
        );
        let scope = serde_json::json!({"actor":"one"});
        let initial = tokio::spawn({
            let cache = cache.clone();
            let scope = scope.clone();
            async move { cache.issue(&scope).await }
        });
        exchanges.recv().await.context("initial")?
            .send(Ok(token("first"))).unwrap();
        initial.await??;
        let client = fixture.pool.get().await?;
        client.execute(
            "UPDATE durable_actors_storage_tokens SET expires_at=clock_timestamp()+interval '2 minutes', refresh_owner='old-claim', refresh_until=clock_timestamp()+interval '30 seconds'",
            &[],
        ).await?;
        let stale_refresh = tokio::spawn({
            let cache = cache.clone();
            let scope = scope.clone();
            async move { cache.refresh(&key("issuer", &scope)?, "old-claim", &scope).await }
        });
        assert_eq!(cache.issue(&scope).await?.access_token, "first");
        let old_refresh = exchanges.recv().await.context("refresh")?;
        client.execute(
            "UPDATE durable_actors_storage_tokens SET refresh_until=clock_timestamp()-interval '1 second', expires_at=clock_timestamp()-interval '1 second'",
            &[],
        ).await?;
        let replacement = tokio::spawn({
            let cache = cache.clone();
            let scope = scope.clone();
            async move { cache.issue(&scope).await }
        });
        exchanges.recv().await.context("replacement refresh")?
            .send(Ok(token("replacement"))).unwrap();
        assert_eq!(replacement.await??.access_token, "replacement");
        old_refresh.send(Ok(token("stale"))).unwrap();
        assert!(stale_refresh.await??.is_none());
        assert_eq!(cache.issue(&scope).await?.access_token, "replacement");
        Ok(())
    }).await
}

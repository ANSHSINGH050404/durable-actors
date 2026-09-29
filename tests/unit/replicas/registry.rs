use super::*;
use crate::postgres::testing::with_postgres;

#[tokio::test]
async fn concurrent_claims_are_exclusive_and_closing_is_permanent() -> Result<()> {
    with_postgres(async |db| {
        let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
        registry
            .reserve_spares(&["us-west4-a".into()], 6, 6)
            .await?;
        for (i, pending) in registry.unassigned().await?.into_iter().enumerate() {
            let pod = PodRecord {
                name: pending.name,
                zone: "us-west4-a".into(),
                uid: Some(format!("uid-{i}")),
                node: Some(format!("node-{}", i % 3)),
                placement: Some(ReplicaPlacement {
                    id: format!("disk-{i}"),
                    address: format!("http://replica-{i}:7200"),
                    zone: "us-west4-a".into(),
                }),
            };
            registry.update_pod(&pod).await?;
        }
        let zones = vec!["us-west4-a".to_owned(); 3];
        let (first, second) = tokio::join!(
            registry.claim("first/", &zones),
            registry.claim("second/", &zones)
        );
        let first = first?;
        let second = second?;
        assert_eq!(first.pods.len(), 3);
        assert!(
            first
                .pods
                .iter()
                .chain(&second.pods)
                .all(|pod| pod.placement.is_some()),
            "parallel claims must use available warm replicas"
        );
        assert_eq!(
            first
                .pods
                .iter()
                .filter_map(|p| p.node.as_ref())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
        assert!(
            first
                .pods
                .iter()
                .all(|p| second.pods.iter().all(|q| p.name != q.name))
        );
        registry.ready("first/").await?;
        assert!(registry.close("first/").await?.ever_ready);
        assert!(registry.ready("first/").await.is_err());
        assert!(registry.claim("first/", &zones).await.is_err());
        let restarted = Registry::new(PostgresDatabase::lazy(&db.url)?);
        assert_eq!(restarted.lookup("first/").await?.state, "closing");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn closing_before_claim_fences_delayed_creation() -> Result<()> {
    with_postgres(async |db| {
        let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
        let group = registry.close("late/").await?;
        assert!(!group.ever_ready);
        registry.archived("late/", None).await?;
        assert!(
            registry
                .claim("late/", &["us-west4-a".into()])
                .await
                .is_err()
        );
        assert_eq!(registry.lookup("late/").await?.state, "archived");
        Ok(())
    })
    .await
}

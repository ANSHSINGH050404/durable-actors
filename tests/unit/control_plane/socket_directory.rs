use super::*;

#[tokio::test(start_paused = true)]
async fn room_ownership_is_stable_until_the_gateway_lease_expires() -> Result<()> {
    let directory = MemorySocketDirectory::default();
    let first = GatewayOwner {
        id: "first".into(),
        route: "http://first:7100".into(),
    };
    let second = GatewayOwner {
        id: "second".into(),
        route: "http://second:7100".into(),
    };
    directory.register(&first, true).await?;
    directory.register(&second, true).await?;
    let actor = crate::actor::ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    assert_eq!(directory.claim(&actor, &first).await?, first);
    assert_eq!(directory.claim(&actor, &second).await?, first);
    tokio::time::advance(Duration::from_secs(20)).await;
    directory.renew(&second).await?;
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(directory.renew(&first).await.is_err());
    assert_eq!(directory.claim(&actor, &second).await?, second);
    assert_eq!(directory.lookup(&actor).await?, Some(second));
    Ok(())
}

#[tokio::test]
async fn postgres_gateways_agree_on_room_ownership() -> Result<()> {
    crate::postgres::testing::with_postgres(async |db| {
        let directory = PostgresSocketDirectory::new(crate::postgres::PostgresDatabase::lazy(&db.url)?);
        let first = GatewayOwner { id: "first".into(), route: "http://first:7100".into() };
        let second = GatewayOwner { id: "second".into(), route: "http://second:7100".into() };
        directory.register(&first, true).await?;
        directory.register(&second, true).await?;
        let actor = crate::actor::ActorKey { project_id: "p".into(), actor_name: "Room".into(), actor_id: "one".into() };
        let (a, b) = tokio::join!(directory.claim(&actor, &first), directory.claim(&actor, &second));
        assert_eq!(a?, b?);
        let owner = directory.lookup(&actor).await?.unwrap();
        assert_eq!(directory.owners("p").await?, vec![owner.clone()]);
        assert!(directory.owners("other").await?.is_empty());
        directory.database.execute("UPDATE socket_gateways SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1", &[&owner.id]).await?;
        assert!(directory.renew(&owner).await.is_err());
        let replacement = if owner == first { second } else { first };
        assert_eq!(directory.claim(&actor, &replacement).await?, replacement);
        Ok(())
    }).await
}

#[tokio::test]
async fn control_plane_nodes_route_rooms_to_the_dedicated_gateway_pool() -> Result<()> {
    let directory = MemorySocketDirectory::default();
    let control = GatewayOwner {
        id: "control".into(),
        route: "http://control:7100".into(),
    };
    let gateway = GatewayOwner {
        id: "gateway".into(),
        route: "http://gateway:7100".into(),
    };
    directory.register(&control, false).await?;
    directory.register(&gateway, true).await?;
    let actor = crate::actor::ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    assert_eq!(directory.claim(&actor, &control).await?, gateway);
    Ok(())
}

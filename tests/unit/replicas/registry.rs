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

#[tokio::test]
async fn unchanged_registration_avoids_a_write_without_accepting_replaced_or_retiring_disks()
-> Result<()> {
    with_postgres(async |db| {
        let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
        let group = registry
            .claim("registration/", &["us-west4-a".into()])
            .await?;
        let pod = PodRecord {
            uid: Some("original-uid".into()),
            node: Some("node".into()),
            placement: Some(ReplicaPlacement {
                id: "disk".into(),
                address: "http://replica:7200".into(),
                zone: "us-west4-a".into(),
            }),
            ..group.pods[0].clone()
        };
        registry.update_pod(&pod).await?;
        let client = db.pool.get().await?;
        let before: String = client
            .query_one(
                "SELECT xmin::text FROM durable_actors_replication_pods WHERE name=$1",
                &[&pod.name],
            )
            .await?
            .get(0);
        registry.update_pod(&pod).await?;
        let after: String = client
            .query_one(
                "SELECT xmin::text FROM durable_actors_replication_pods WHERE name=$1",
                &[&pod.name],
            )
            .await?
            .get(0);
        assert_eq!(
            before, after,
            "an unchanged registration must not generate another row version"
        );
        let replaced = PodRecord {
            uid: Some("new-uid".into()),
            ..pod.clone()
        };
        assert!(registry.update_pod(&replaced).await.is_err());
        let mut replaced_disk = pod.clone();
        replaced_disk.placement.as_mut().unwrap().id = "new-disk".into();
        assert!(registry.update_pod(&replaced_disk).await.is_err());
        client
            .execute(
                "UPDATE durable_actors_replication_pods SET state='retiring' WHERE name=$1",
                &[&pod.name],
            )
            .await?;
        assert!(registry.update_pod(&pod).await.is_err());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn registration_racing_a_claim_preserves_the_binding_and_racing_retirement_fails()
-> Result<()> {
    for retiring in [false, true] {
        with_postgres(async |db| {
            let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
            registry.reserve_spares(&["us-west4-a".into()], 1, 1).await?;
            let pending = registry.unassigned().await?.remove(0);
            let pod = PodRecord {
                uid: Some("uid".into()), node: Some("node".into()),
                placement: Some(ReplicaPlacement { id: "disk".into(), address: "http://replica:7200".into(), zone: pending.zone.clone() }),
                ..pending
            };
            let mut blocker = db.pool.get().await?;
            let pid: i32 = blocker.query_one("SELECT pg_backend_pid()", &[]).await?.get(0);
            let tx = blocker.transaction().await?;
            tx.query_one("SELECT name FROM durable_actors_replication_pods WHERE name=$1 FOR UPDATE", &[&pod.name]).await?;
            let work = registry.clone();
            let observed = pod.clone();
            let registration = tokio::spawn(async move { work.update_pod(&observed).await });
            wait_for_blocked_query(db, pid).await?;
            if retiring {
                tx.execute("UPDATE durable_actors_replication_pods SET state='retiring' WHERE name=$1", &[&pod.name]).await?;
            } else {
                tx.execute("INSERT INTO durable_actors_replication_groups(prefix,state) VALUES('claimed/','creating')", &[]).await?;
                tx.execute("UPDATE durable_actors_replication_pods SET group_prefix='claimed/',slot=0,state='bound' WHERE name=$1", &[&pod.name]).await?;
            }
            tx.commit().await?;
            assert_eq!(registration.await?.is_err(), retiring);
            let row = blocker.query_one("SELECT state,group_prefix FROM durable_actors_replication_pods WHERE name=$1", &[&pod.name]).await?;
            assert_eq!(row.get::<_, &str>(0), if retiring { "retiring" } else { "bound" });
            assert_eq!(row.get::<_, Option<&str>>(1), if retiring { None } else { Some("claimed/") });
            Ok(())
        }).await?;
    }
    Ok(())
}

#[tokio::test]
async fn ready_waiting_on_a_closing_group_cannot_return_a_ready_record() -> Result<()> {
    with_postgres(async |db| {
        let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
        registry.claim("closing/", &["us-west4-a".into()]).await?;
        let mut blocker = db.pool.get().await?;
        let pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        tx.execute(
            "UPDATE durable_actors_replication_groups SET state='closing' WHERE prefix='closing/'",
            &[],
        )
        .await?;
        let work = registry.clone();
        let ready = tokio::spawn(async move { work.ready("closing/").await });
        wait_for_blocked_query(db, pid).await?;
        tx.commit().await?;
        assert!(ready.await?.is_err());
        assert!(!registry.lookup("closing/").await?.ever_ready);
        Ok(())
    })
    .await
}

async fn wait_for_blocked_query(
    db: &crate::postgres::testing::TestDatabase,
    blocker: i32,
) -> Result<()> {
    let client = db.pool.get().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", &[&blocker]).await?.get(0);
            if blocked { return anyhow::Ok(()); }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await??;
    Ok(())
}

#[tokio::test]
async fn claims_bind_all_slots_together_preserving_zone_order_and_skipping_locked_nodes()
-> Result<()> {
    with_postgres(async |db| {
        let registry = Registry::new(PostgresDatabase::connect(&db.url).await?);
        let client = db.pool.get().await?;
        for (name, zone, node) in [("a1", "a", "one"), ("a2", "a", "one"), ("a3", "a", "two"), ("b1", "b", "three")] {
            let pod = PodRecord { name: name.into(), zone: zone.into(), uid: Some(name.into()), node: Some(node.into()), placement: Some(ReplicaPlacement { id: name.into(), address: format!("http://{name}:7200"), zone: zone.into() }) };
            client.execute("INSERT INTO durable_actors_replication_pods(name,zone,state,config) VALUES($1,$2,'ready',$3)", &[&name,&zone,&serde_json::to_string(&pod)?]).await?;
        }
        client.batch_execute("CREATE TABLE claim_statements (id integer); CREATE FUNCTION count_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO claim_statements VALUES (1); RETURN NULL; END $$; CREATE TRIGGER count_claim BEFORE INSERT ON durable_actors_replication_pods FOR EACH STATEMENT EXECUTE FUNCTION count_claim()").await?;
        let mut blocker = db.pool.get().await?;
        let lock = blocker.transaction().await?;
        lock.query_one("SELECT name FROM durable_actors_replication_pods WHERE name='a3' FOR UPDATE", &[]).await?;
        let zones = vec!["a".into(), "b".into(), "a".into()];
        let group = tokio::time::timeout(std::time::Duration::from_secs(2), registry.claim("batch/", &zones)).await??;
        assert_eq!(group.pods.iter().map(|p| p.zone.clone()).collect::<Vec<_>>(), zones);
        assert_eq!(group.pods[0].name, "a1");
        assert_eq!(group.pods[1].name, "b1");
        assert!(group.pods[2].placement.is_none());
        assert_eq!(registry.lookup("batch/").await?.pods, group.pods);
        let statements: i64 = client.query_one("SELECT count(*) FROM claim_statements", &[]).await?.get(0);
        assert_eq!(statements, 1, "all slots must bind in one statement");
        let repeated = registry.claim("batch/", &zones).await?;
        assert_eq!(repeated.pods, group.pods);
        lock.rollback().await?;
        Ok(())
    }).await
}

use super::*;
use crate::replicas::{
    directory::DedicatedSnapshots,
    server::testing::{SECRET, TestReplica},
};

struct NetworkNodes {
    archive: Arc<dyn Bucket>,
    servers: Mutex<HashMap<String, TestReplica>>,
}

#[async_trait]
impl ReplicaPods for NetworkNodes {
    async fn ensure(
        &self,
        expected: &PodRecord,
        _: Option<&str>,
        _: &[String],
    ) -> Result<PodRecord> {
        let server = TestReplica::start(&expected.zone, self.archive.clone()).await?;
        let mut pod = expected.clone();
        pod.uid = Some(server.placement.id.clone());
        pod.node = Some(format!("node-{}", pod.name));
        pod.placement = Some(server.placement.clone());
        self.servers
            .lock()
            .unwrap()
            .insert(pod.name.clone(), server);
        Ok(pod)
    }
    async fn health(&self, pod: &PodRecord, _: &PodInventory) -> Result<PodHealth> {
        Ok(PodHealth {
            live: self.servers.lock().unwrap().contains_key(&pod.name),
            draining: false,
            current_image: true,
        })
    }
    async fn observed(&self) -> Result<PodInventory> {
        Ok(HashMap::new())
    }
    async fn protect(&self, _: &PodRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, pod: &PodRecord) -> Result<()> {
        self.servers.lock().unwrap().remove(&pod.name);
        Ok(())
    }
}

#[tokio::test]
async fn network_zone_loss_falls_back_repairs_and_preserves_every_snapshot() -> Result<()> {
    with_postgres(async |db| {
        let mut fixture = Fixture::new(&db.url).await?;
        let nodes = Arc::new(NetworkNodes {
            archive: fixture.bucket.clone(),
            servers: Mutex::new(HashMap::new()),
        });
        fixture.fleet.pods = nodes.clone();
        fixture.fleet.peers = Arc::new(HttpPeers::new(SECRET.into())?);
        fixture
            .warm(&["us-west4-a", "us-west4-b", "us-west4-c"])
            .await?;
        let fleet = Arc::new(fixture.fleet);
        let snapshots = DedicatedSnapshots::new(fleet.clone(), SECRET.into())?;
        snapshots.prepare(&fixture.prefix).await?;
        let original = fleet.group(&fixture.prefix).await?;
        let object = |version| format!("{}{version}.json", fixture.prefix);
        let states = (1..=7).map(Fixture::snapshot).collect::<Result<Vec<_>>>()?;
        let state = |version: usize| states[version - 1].clone();
        snapshots.put(&object(1), state(1)).await?;
        assert!(fleet.archive.get(&fixture.prefix, 1).await?.is_none());

        nodes
            .servers
            .lock()
            .unwrap()
            .retain(|_, server| server.placement.zone != "us-west4-a");
        tokio::task::yield_now().await;
        snapshots.put(&object(2), state(2)).await?;
        snapshots.put(&object(3), state(3)).await?;
        assert!(fleet.group(&fixture.prefix).await?.replicas.is_empty());

        fleet
            .registry
            .reserve_spares(&["us-west4-b".into(), "us-west4-c".into()], 2, 2)
            .await?;
        for pod in fleet.registry.unassigned().await? {
            fleet
                .registry
                .update_pod(&nodes.ensure(&pod, None, &[]).await?)
                .await?;
        }
        snapshots.put(&object(4), state(4)).await?;
        let repaired = fleet.group(&fixture.prefix).await?;
        assert_eq!(repaired.replicas.len(), 2);
        snapshots.put(&object(5), state(5)).await?;
        assert!(fleet.archive.get(&fixture.prefix, 5).await?.is_none());

        fleet.registry.reserve_spares(&fleet.zones, 3, 3).await?;
        for pod in fleet.registry.unassigned().await? {
            fleet
                .registry
                .update_pod(&nodes.ensure(&pod, None, &[]).await?)
                .await?;
        }
        fleet
            .reconcile_group(&fixture.prefix, &PodInventory::new())
            .await?;
        snapshots.put(&object(6), state(6)).await?;
        assert_eq!(fleet.group(&fixture.prefix).await?.replicas.len(), 3);

        let stale = crate::bucket::ReplicaSet::from_replicas(
            &original.replicas,
            SECRET.into(),
            ReplicaClient::http()?,
        )?;
        assert!(stale.put(&object(7), state(7)).await.is_err());
        snapshots.seal(&fixture.prefix).await?;
        for version in 1..=6 {
            let bytes = snapshots.get(&object(version)).await?.unwrap();
            assert_eq!(bytes, state(version));
        }
        assert_eq!(
            snapshots.latest(&fixture.prefix).await?.unwrap().0,
            object(6)
        );
        assert!(snapshots.put(&object(7), state(7)).await.is_err());
        Ok(())
    })
    .await
}

#[tokio::test(start_paused = true)]
async fn one_actor_preparing_does_not_block_other_control_plane_activations() -> Result<()> {
    use crate::replicas::directory::ReplicaDirectory;
    struct Directory {
        preparing: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl ReplicaDirectory for Directory {
        async fn execute(&self, command: DirectoryCommand) -> Result<DirectoryReply> {
            let DirectoryCommand::Prepare { prefix } = command else {
                anyhow::bail!("expected prepare");
            };
            if prefix == "blocked" {
                self.preparing.notify_one();
                self.release.notified().await;
            }
            Ok(DirectoryReply {
                groups: vec![Group {
                    prefix,
                    ..Default::default()
                }],
                ..Default::default()
            })
        }
    }
    let directory = Arc::new(Directory {
        preparing: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let snapshots = Arc::new(DedicatedSnapshots::for_control_plane(
        directory.clone(),
        SECRET.into(),
    )?);
    let first = snapshots.clone();
    let blocked = tokio::spawn(async move { first.prepare("blocked").await });
    directory.preparing.notified().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        snapshots.prepare("unrelated"),
    )
    .await??;
    directory.release.notify_one();
    blocked.await??;
    Ok(())
}

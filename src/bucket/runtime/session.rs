use super::*;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Session {
    pub(super) id: String,
    pub(super) region: String,
    pub(super) replicas: Vec<ReplicaTarget>,
    pub(super) state: RecoveryState,
}

impl Session {
    pub(super) fn is_open(&self) -> bool {
        self.state == RecoveryState::Open
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum RecoveryState {
    Open,
    Recovering,
    Sealed,
}

impl RuntimeStorage {
    pub(crate) async fn finish_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
        self.uploads.close();
        self.uploads.wait().await;
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.lease.id == *host && record.lease.session_id == session,
            "actor ownership changed"
        );
        let checkpoint = self.upload_checkpoint(&record)?;
        self.seal_owned_session(&record.scope()).await?;
        self.release_with_checkpoint(actor, host, session, Some(checkpoint))
            .await
    }

    pub async fn retire_replication(&self, scope: &ReplicaScope) -> Result<()> {
        if let Some(owner) = self.get_owner(&scope.actor.storage_key()).await? {
            ensure!(
                owner.owner != scope.host
                    || owner.lease.session_id != scope.session
                    || owner.lease.expires_at_ms <= self.clock.now_ms()?,
                "actor activation is still active"
            );
        }
        let session = self.start_recovery(scope).await?;
        if session.state == RecoveryState::Sealed {
            return Ok(());
        }
        for snapshot in self.seal_replicas(&session).await? {
            self.recover_snapshot(&session.replicas, &snapshot).await?;
        }
        self.finish_recovery(scope, session).await
    }

    pub async fn replica_members(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        let key = key(&scope.host, &scope.session);
        let Some(object) = self.authority.get(&key).await? else {
            return Ok(Vec::new());
        };
        let session: Session = serde_json::from_slice(&object.bytes)?;
        ensure!(
            session.id == scope.identity() && session.region == scope.region,
            "session identity mismatch"
        );
        Ok(if session.state == RecoveryState::Sealed {
            Vec::new()
        } else {
            session.replicas
        })
    }

    fn upload_checkpoint(&self, record: &Ownership) -> Result<SessionCheckpoint> {
        let mut snapshot = record.base.clone();
        if let Some(uploaded) = self
            .uploaded
            .lock()
            .unwrap()
            .get(&record.scope().identity())
        {
            ensure!(
                uploaded.started == uploaded.completed,
                "snapshot uploads did not complete successfully"
            );
            advance(&mut snapshot, uploaded.latest.clone())?;
        }
        Ok(SessionCheckpoint { snapshot })
    }

    async fn seal_owned_session(&self, scope: &ReplicaScope) -> Result<()> {
        let key = key(&scope.host, &scope.session);
        for _ in 0..3 {
            let object = self.authority.get(&key).await?;
            let mut session = match &object {
                Some(object) => serde_json::from_slice::<Session>(&object.bytes)?,
                None => Session {
                    id: scope.identity(),
                    region: scope.region.clone(),
                    replicas: vec![],
                    state: RecoveryState::Open,
                },
            };
            ensure!(
                session.id == scope.identity() && session.region == scope.region,
                "session identity mismatch"
            );
            ensure!(
                session.state != RecoveryState::Recovering,
                "session recovery has already started"
            );
            if session.state == RecoveryState::Sealed {
                return Ok(());
            }
            session.state = RecoveryState::Sealed;
            if self
                .save_session(&key, object.map(|object| object.generation), &session)
                .await?
            {
                return Ok(());
            }
        }
        anyhow::bail!("session kept changing during clean shutdown")
    }

    pub(super) async fn recover_session(
        &self,
        owner: &Ownership,
    ) -> Result<Option<LoadedSnapshot>> {
        let session = self.start_recovery(&owner.scope()).await?;
        if session.state == RecoveryState::Sealed {
            if session.replicas.is_empty() {
                return Ok(None);
            }
            // Another recovery may have uploaded snapshots after our parallel LIST.
            let newest = self.latest_snapshot_key(owner).await?;
            return self.load_latest(owner, None, newest, &[]).await;
        }
        let snapshots = self.seal_replicas(&session).await?;
        let recovered = self.restore_session(owner, &session, snapshots).await?;
        self.finish_recovery(&owner.scope(), session).await?;
        Ok(recovered)
    }

    async fn start_recovery(&self, scope: &ReplicaScope) -> Result<Session> {
        let key = key(&scope.host, &scope.session);
        let id = scope.identity();
        loop {
            let object = self.authority.get(&key).await?;
            let Some(object) = object else {
                // A tombstone also fences initialization delayed past lease expiry.
                let sealed = Session {
                    id: id.clone(),
                    region: scope.region.clone(),
                    replicas: Vec::new(),
                    state: RecoveryState::Sealed,
                };
                if self.save_session(&key, None, &sealed).await? {
                    return Ok(sealed);
                }
                continue;
            };
            let mut session: Session = serde_json::from_slice(&object.bytes)?;
            ensure!(
                session.id == id && session.region == scope.region,
                "session identity mismatch"
            );
            match session.state {
                RecoveryState::Sealed => return Ok(session),
                RecoveryState::Recovering => return Ok(session),
                RecoveryState::Open => {
                    session.state = RecoveryState::Recovering;
                    if self
                        .save_session(&key, Some(object.generation), &session)
                        .await?
                    {
                        return Ok(session);
                    }
                }
            }
        }
    }

    async fn finish_recovery(&self, scope: &ReplicaScope, mut session: Session) -> Result<()> {
        let key = key(&scope.host, &scope.session);
        session.state = RecoveryState::Sealed;
        loop {
            let current = self
                .authority
                .get(&key)
                .await?
                .context("recovery record disappeared")?;
            let record: Session = serde_json::from_slice(&current.bytes)?;
            if record.state == RecoveryState::Sealed
                || self
                    .save_session(&key, Some(current.generation), &session)
                    .await?
            {
                return Ok(());
            }
        }
    }

    async fn save_session(
        &self,
        key: &str,
        generation: Option<i64>,
        session: &Session,
    ) -> Result<bool> {
        replace(
            self.authority.as_ref(),
            key,
            generation,
            serde_json::to_vec(session)?,
        )
        .await
    }

    async fn seal_replicas(&self, session: &Session) -> Result<Vec<SnapshotRef>> {
        let mut pending = JoinSet::new();
        for target in &session.replicas {
            let (peers, target, id) = (self.peers.clone(), target.clone(), session.id.clone());
            pending.spawn(async move { peers.seal(&target, &id).await });
        }
        let mut witnesses = 0;
        let mut snapshots = HashMap::new();
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(head)) = result {
                ensure!(
                    head.session == session.id,
                    "replica returned another session"
                );
                if !head.initialized || !head.sealed {
                    continue;
                }
                witnesses += 1;
                for head in head.streams {
                    ensure!(
                        head.stream.session == session.id,
                        "replica returned another session's stream"
                    );
                    if let Some(snapshot) = head.latest {
                        ensure!(
                            snapshot.object == head.stream.object(snapshot.state_version),
                            "replica snapshot identity mismatch"
                        );
                        advance(
                            snapshots.entry(head.stream.prefix).or_insert(None),
                            Some(snapshot),
                        )?;
                    }
                }
            }
        }
        ensure!(
            session.replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        Ok(snapshots.into_values().flatten().collect())
    }

    async fn restore_session(
        &self,
        owner: &Ownership,
        session: &Session,
        snapshots: Vec<SnapshotRef>,
    ) -> Result<Option<LoadedSnapshot>> {
        let prefix = snapshot_prefix(&owner.actor)?;
        let mut loaded: Option<LoadedSnapshot> = None;
        for snapshot in snapshots {
            let bytes = self.recover_snapshot(&session.replicas, &snapshot).await?;
            if snapshot.object.starts_with(&prefix)
                && loaded.as_ref().is_none_or(|s| {
                    snapshot_position(&snapshot.object) > snapshot_position(&s.reference.object)
                })
            {
                loaded = Some(LoadedSnapshot {
                    reference: snapshot,
                    bytes: bytes.into(),
                });
            }
        }
        Ok(loaded)
    }
}

pub(super) fn identity(host: &HostId, session: &str) -> String {
    format!("{}/", crate::storage_paths::session(host, session))
}

fn key(host: &HostId, session: &str) -> String {
    format!("{}.json", crate::storage_paths::session(host, session))
}

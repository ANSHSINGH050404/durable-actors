use super::*;

impl RuntimeStorage {
    pub(crate) async fn finish_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
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
        self.release_with_checkpoint(actor, host, session, Some(checkpoint))
            .await
    }

    fn upload_checkpoint(&self, record: &Ownership) -> Result<SessionCheckpoint> {
        let mut snapshot = record.base.clone();
        if let Some(uploaded) = self.uploaded.lock().unwrap().get(&record.lease.session_id) {
            ensure!(
                uploaded.started == uploaded.completed,
                "snapshot uploads did not complete successfully"
            );
            advance(&mut snapshot, uploaded.latest.clone())?;
        }
        Ok(SessionCheckpoint { snapshot })
    }
}

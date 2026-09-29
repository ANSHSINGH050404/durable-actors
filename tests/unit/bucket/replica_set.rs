use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct CopyStore {
    bytes: Mutex<Option<Bytes>>,
    entered: Semaphore,
    release: Semaphore,
    gated: bool,
    failed: AtomicBool,
    writes: AtomicUsize,
}

impl CopyStore {
    fn new(gated: bool) -> Arc<Self> {
        Arc::new(Self {
            bytes: Mutex::new(None),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            gated,
            failed: AtomicBool::new(false),
            writes: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SnapshotStore for CopyStore {
    async fn prepare(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn seal(&self, _: &str) -> Result<()> {
        ensure!(!self.failed.load(Ordering::SeqCst), "bucket unavailable");
        Ok(())
    }
    async fn get(&self, _: &str) -> Result<Option<Bytes>> {
        ensure!(!self.failed.load(Ordering::SeqCst), "bucket unavailable");
        Ok(self.bytes.lock().await.clone())
    }
    async fn latest(&self, _: &str) -> Result<Option<(String, Bytes)>> {
        Ok(self
            .get("epoch/1.json")
            .await?
            .map(|bytes| ("epoch/1.json".into(), bytes)))
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        ensure!(!self.failed.load(Ordering::SeqCst), "bucket unavailable");
        Ok(self
            .bytes
            .lock()
            .await
            .as_ref()
            .map(|_| vec!["epoch/1.json".into()])
            .unwrap_or_default())
    }
    async fn put(&self, _: &str, bytes: Bytes) -> Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        *self.bytes.lock().await = Some(bytes);
        self.entered.add_permits(1);
        if self.gated {
            self.release.acquire().await?.forget();
        }
        ensure!(
            !self.failed.load(Ordering::SeqCst),
            "reply lost after persisting"
        );
        Ok(())
    }
}

#[tokio::test]
async fn acknowledgement_waits_for_every_required_copy_and_dispatches_concurrently() -> Result<()> {
    let a = CopyStore::new(true);
    let b = CopyStore::new(true);
    let store = Arc::new(ReplicaSet::new(vec![a.clone(), b.clone()])?);
    store.prepare("epoch/").await?;
    let pending = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .put("epoch/1.json", Bytes::from_static(b"state"))
                .await
        }
    });
    a.entered.acquire().await?.forget();
    b.entered.acquire().await?.forget();
    a.release.add_permits(1);
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    b.release.add_permits(1);
    pending.await??;
    Ok(())
}

#[tokio::test]
async fn ambiguous_write_fences_the_entire_epoch_without_downgrading() -> Result<()> {
    let a = CopyStore::new(false);
    let b = CopyStore::new(false);
    b.failed.store(true, Ordering::SeqCst);
    let store = ReplicaSet::new(vec![a.clone(), b.clone()])?;
    store.prepare("epoch/").await?;
    assert!(
        store
            .put("epoch/1.json", Bytes::from_static(b"state"))
            .await
            .is_err()
    );
    b.failed.store(false, Ordering::SeqCst);
    assert!(
        store
            .put("epoch/2.json", Bytes::from_static(b"next"))
            .await
            .is_err()
    );
    assert!(store.prepare("epoch/").await.is_err());
    assert_eq!(a.writes.load(Ordering::SeqCst), 1);
    assert_eq!(b.writes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancellation_fences_the_epoch_before_any_subsequent_append() -> Result<()> {
    let a = CopyStore::new(true);
    let store = Arc::new(ReplicaSet::new(vec![a.clone()])?);
    store.prepare("epoch/").await?;
    let pending = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .put("epoch/1.json", Bytes::from_static(b"state"))
                .await
        }
    });
    a.entered.acquire().await?.forget();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(
        store
            .put("epoch/2.json", Bytes::from_static(b"next"))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn recovery_uses_a_surviving_copy_but_rejects_conflicting_state() -> Result<()> {
    let a = CopyStore::new(false);
    let b = CopyStore::new(false);
    let store = ReplicaSet::new(vec![a.clone(), b.clone()])?;
    store.prepare("epoch/").await?;
    store
        .put("epoch/1.json", Bytes::from_static(b"state"))
        .await?;
    a.failed.store(true, Ordering::SeqCst);
    assert_eq!(
        store.get("epoch/1.json").await?,
        Some(Bytes::from_static(b"state"))
    );
    assert_eq!(store.list("epoch/").await?, vec!["epoch/1.json"]);
    a.failed.store(false, Ordering::SeqCst);
    *a.bytes.lock().await = Some(Bytes::from_static(b"conflict"));
    assert!(store.get("epoch/1.json").await.is_err());
    a.failed.store(true, Ordering::SeqCst);
    b.failed.store(true, Ordering::SeqCst);
    assert!(store.get("epoch/1.json").await.is_err());
    assert!(store.list("epoch/").await.is_err());
    Ok(())
}

#[tokio::test]
async fn sealing_a_surviving_copy_prevents_old_epoch_writes() -> Result<()> {
    let a = CopyStore::new(false);
    let b = CopyStore::new(false);
    let store = ReplicaSet::new(vec![a.clone(), b.clone()])?;
    store.prepare("epoch/").await?;
    store
        .put("epoch/1.json", Bytes::from_static(b"state"))
        .await?;
    a.failed.store(true, Ordering::SeqCst);
    store.seal("epoch/").await?;
    assert_eq!(
        store.get("epoch/1.json").await?,
        Some(Bytes::from_static(b"state"))
    );
    a.failed.store(false, Ordering::SeqCst);
    assert!(
        store
            .put("epoch/2.json", Bytes::from_static(b"next"))
            .await
            .is_err()
    );
    assert!(store.prepare("epoch/").await.is_err());
    Ok(())
}

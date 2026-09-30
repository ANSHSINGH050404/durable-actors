use super::*;

type Opened = Vec<(Replica, Box<dyn LogWriter>)>;

pub(super) struct Prepared {
    pub id: String,
    pub prefix: String,
    opened: AbortOnDropHandle<Result<Option<Opened>>>,
}

impl Prepared {
    pub fn new(prefix: String, zones: Vec<Arc<dyn LogZone>>) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let object = format!("{prefix}{id}.segment");
        let opened =
            AbortOnDropHandle::new(tokio::spawn(async move { open(&zones, &object).await }));
        Self { id, prefix, opened }
    }

    pub async fn take(self) -> Result<Option<Opened>> {
        self.opened
            .await
            .context("Rapid connection preparation failed")?
    }
}

async fn open(zones: &[Arc<dyn LogZone>], object: &str) -> Result<Option<Opened>> {
    let results =
        futures_util::future::join_all(zones.iter().map(|zone| bounded(zone.open(object)))).await;
    let mut opened = Vec::new();
    for result in results {
        match result {
            Ok(copy) => opened.push(copy),
            Err(error) => {
                tracing::warn!(%error, "Rapid log unavailable; activation will use Standard")
            }
        }
    }
    if opened.len() == zones.len() {
        return Ok(Some(opened));
    }
    for (replica, writer) in opened {
        drop(writer);
        if let Some(zone) = zones.iter().find(|zone| zone.bucket() == replica.bucket) {
            let _ = bounded(zone.delete(&replica)).await;
        }
    }
    Ok(None)
}

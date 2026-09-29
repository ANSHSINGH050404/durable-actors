use super::{FileBucket, RuntimeStorage};
use crate::clock::SystemClock;
use anyhow::Result;
use std::sync::Arc;

pub(crate) struct RuntimeFixture {
    pub directory: tempfile::TempDir,
    pub runtime: Arc<RuntimeStorage>,
}
impl RuntimeFixture {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(directory.path().into())?);
        let runtime = Arc::new(RuntimeStorage::new(bucket, Arc::new(SystemClock))?);
        Ok(Self { directory, runtime })
    }
}

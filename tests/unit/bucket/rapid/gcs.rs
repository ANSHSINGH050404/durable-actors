use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

#[tokio::test]
async fn rapid_uploads_bound_chunks_and_only_succeed_after_flush() -> Result<()> {
    let bytes = Bytes::from(
        (0..5 * 1024 * 1024 + 17)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>(),
    );
    for (fail_append, fail_flush) in [(false, false), (true, false), (false, true)] {
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicU64::new(0));
        let mut writer = GcsWriter(AppendableObjectWriter::new(UploadStub {
            chunks: chunks.clone(),
            flushes: flushes.clone(),
            fail_append,
            fail_flush,
            persisted: 0,
        }));
        let result = writer.append_and_flush(bytes.clone()).await;
        if fail_append || fail_flush {
            assert!(result.is_err());
        } else {
            assert_eq!(result?, bytes.len() as u64);
        }
        assert_eq!(flushes.load(Ordering::SeqCst), u64::from(!fail_append));
        let chunks = chunks.lock().unwrap();
        assert!(chunks.iter().all(|chunk| chunk.len() <= 256 * 1024));
        let received = chunks.concat();
        assert_eq!(received, bytes[..received.len()]);
        assert_eq!(
            received.len(),
            if fail_append { 256 * 1024 } else { bytes.len() }
        );
    }
    Ok(())
}

#[derive(Debug)]
struct UploadStub {
    chunks: Arc<Mutex<Vec<Bytes>>>,
    flushes: Arc<AtomicU64>,
    fail_append: bool,
    fail_flush: bool,
    persisted: i64,
}

impl google_cloud_storage::stub::AppendableObjectWriter for UploadStub {
    async fn append(&mut self, chunk: Bytes) -> google_cloud_storage::Result<()> {
        let mut chunks = self.chunks.lock().unwrap();
        if self.fail_append && !chunks.is_empty() {
            return Err(google_cloud_storage::Error::io("injected append failure"));
        }
        chunks.push(chunk);
        Ok(())
    }

    async fn flush(&mut self) -> google_cloud_storage::Result<i64> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        if self.fail_flush {
            return Err(google_cloud_storage::Error::io("injected flush failure"));
        }
        self.persisted = self
            .chunks
            .lock()
            .unwrap()
            .iter()
            .map(|chunk| chunk.len() as i64)
            .sum();
        Ok(self.persisted)
    }

    async fn finalize(
        mut self,
    ) -> google_cloud_storage::Result<google_cloud_storage::model::Object> {
        Ok(google_cloud_storage::model::Object::default().set_size(self.flush().await?))
    }

    async fn close(mut self) -> google_cloud_storage::Result<i64> {
        self.flush().await
    }

    fn generation(&self) -> i64 {
        1
    }

    fn persisted_size(&self) -> i64 {
        self.persisted
    }
}

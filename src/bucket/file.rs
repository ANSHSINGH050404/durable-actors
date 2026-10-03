use super::{Bucket, BucketObject};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use atomic_write_file::AtomicWriteFile;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct FileBucket {
    directory: PathBuf,
}

impl FileBucket {
    pub fn new(directory: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&directory)?;
        Ok(Self {
            directory: directory.canonicalize()?,
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Path) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || operation(&directory)).await?
    }
}

#[async_trait]
impl Bucket for FileBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        let key = key.to_owned();
        self.run(move |directory| read(&object_path(directory, &key)?))
            .await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        use std::io::{Seek, SeekFrom};
        let key = key.to_owned();
        self.run(move |directory| {
            let Some(mut file) = open(&object_path(directory, &key)?)? else {
                return Ok(None);
            };
            let generation = read_generation(&mut file)?;
            file.seek(SeekFrom::Start(
                start.checked_add(8).context("range overflow")?,
            ))?;
            let bytes = crate::payload::copy(file.take(length))?;
            ensure!(bytes.len() as u64 == length, "incomplete object range");
            Ok(Some(BucketObject { generation, bytes }))
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<i64>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let key = key.to_owned();
        self.run(move |directory| {
            let path = object_path(directory, &key)?;
            let _lock = lock_object(&path)?;
            let generation = open(&path)?
                .map(|mut file| read_generation(&mut file))
                .transpose()?;
            if generation != expected {
                return Ok(false);
            }
            let next = generation
                .unwrap_or(0)
                .checked_add(1)
                .context("bucket generation overflow")?;
            let mut replacement = AtomicWriteFile::open(&path)?;
            replacement.write_all(&next.to_be_bytes())?;
            replacement.write_all(&bytes)?;
            replacement.commit()?;
            Ok(true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.run(move |directory| {
            let mut objects = Vec::new();
            list(directory, directory, &prefix, &mut objects)?;
            objects.sort();
            Ok(objects)
        })
        .await
    }
}

fn object_path(directory: &Path, key: &str) -> Result<PathBuf> {
    ensure!(
        !key.is_empty()
            && key
                .split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.') && !part.contains('\\')),
        "invalid bucket object path"
    );
    Ok(directory.join(key))
}

fn read(path: &Path) -> Result<Option<BucketObject>> {
    let Some(mut file) = open(path)? else {
        return Ok(None);
    };
    let generation = read_generation(&mut file)?;
    let bytes = crate::payload::copy(file)?;
    Ok(Some(BucketObject { generation, bytes }))
}

fn open(path: &Path) -> Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_generation(file: &mut File) -> Result<i64> {
    let mut generation = [0; 8];
    file.read_exact(&mut generation)
        .context("incomplete bucket object")?;
    Ok(i64::from_be_bytes(generation))
}

fn lock_object(path: &Path) -> Result<File> {
    let parent = path.parent().context("object parent missing")?;
    let name = path
        .file_name()
        .context("object filename missing")?
        .to_string_lossy();
    std::fs::create_dir_all(parent)?;
    // Keep a stable lock file: replacing or unlinking it would split waiting writers.
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(parent.join(format!(".{name}.lock")))?;
    lock.lock()?;
    Ok(lock)
}

fn list(root: &Path, directory: &Path, prefix: &str, objects: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        if entry.file_type()?.is_dir() {
            list(root, &entry.path(), prefix, objects)?;
        } else if entry.file_type()?.is_file() {
            let key = entry
                .path()
                .strip_prefix(root)?
                .to_string_lossy()
                .into_owned();
            if key.starts_with(prefix) {
                objects.push(key);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/file.rs"]
mod tests;

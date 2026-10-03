use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{
    fmt,
    fs::File,
    io::{self, Read, Write},
    ops::Deref,
};

pub(crate) const BUFFER_BYTES: usize = 64 * 1024;
pub(crate) const IO_BUFFER_BYTES: usize = 256 * 1024;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

pub(crate) struct Spool {
    memory: Vec<u8>,
    file: Option<tempfile::NamedTempFile>,
    length: u64,
}

impl Spool {
    pub fn new() -> Self {
        Self {
            memory: Vec::new(),
            file: None,
            length: 0,
        }
    }

    pub fn finish(mut self) -> Result<Bytes> {
        self.flush()?;
        match self.file {
            None => Ok(self.memory.into()),
            Some(file) => {
                let path = file.into_temp_path();
                let file = File::open(&path)?;
                // This privately owned file is immutable until the last mapping is dropped.
                let map = unsafe { memmap2::Mmap::map(&file)? };
                Ok(Bytes::from_owner(Mapped { map, _path: path }))
            }
        }
    }
}

impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .filter(|length| *length <= MAX_FILE_BYTES)
            .ok_or_else(|| io::Error::other("temporary storage object exceeds 2 GiB"))?;
        if self.file.is_none() && length > BUFFER_BYTES as u64 {
            let mut file = tempfile::NamedTempFile::new()?;
            file.write_all(&self.memory)?;
            self.memory = Vec::new();
            self.file = Some(file);
        }
        match self.file.as_mut() {
            Some(file) => file.write_all(bytes)?,
            None => self.memory.extend_from_slice(bytes),
        }
        self.length = length;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

struct Mapped {
    map: memmap2::Mmap,
    _path: tempfile::TempPath,
}
impl AsRef<[u8]> for Mapped {
    fn as_ref(&self) -> &[u8] {
        &self.map
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Bytes> {
    let mut spool = Spool::new();
    serde_json::to_writer(&mut spool, value)?;
    spool.finish()
}

pub(crate) fn copy(mut reader: impl Read) -> Result<Bytes> {
    let mut spool = Spool::new();
    io::copy(&mut reader, &mut spool)?;
    spool.finish()
}

#[derive(Clone, Debug)]
pub struct Text(Bytes);

impl Text {
    pub(crate) fn from_bytes(bytes: Bytes) -> Result<Self> {
        std::str::from_utf8(&bytes)?;
        Ok(Self(bytes))
    }
}
impl Deref for Text {
    type Target = str;
    fn deref(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated text")
    }
}
impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl From<String> for Text {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}
impl Serialize for Text {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}
impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Text;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("base64 text")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Text, E> {
                copy(value.as_bytes()).map(Text).map_err(E::custom)
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Text, E> {
                self.visit_str(&value)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

pub(crate) struct Upload {
    bytes: Bytes,
    offset: usize,
}
impl Upload {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, offset: 0 }
    }
}
impl google_cloud_storage::streaming_source::StreamingSource for Upload {
    type Error = io::Error;
    async fn next(&mut self) -> Option<io::Result<Bytes>> {
        if self.offset == self.bytes.len() {
            return None;
        }
        let end = self.bytes.len().min(self.offset + IO_BUFFER_BYTES);
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Some(Ok(bytes))
    }
    async fn size_hint(&self) -> io::Result<google_cloud_storage::streaming_source::SizeHint> {
        Ok(google_cloud_storage::streaming_source::SizeHint::with_exact(self.bytes.len() as u64))
    }
}
impl google_cloud_storage::streaming_source::Seek for Upload {
    type Error = io::Error;
    async fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.offset = usize::try_from(offset).map_err(io::Error::other)?;
        if self.offset > self.bytes.len() {
            return Err(io::Error::other("upload seek exceeds object"));
        }
        Ok(())
    }
}

pub(crate) struct Download {
    spool: Spool,
    buffer: Vec<u8>,
}

impl Download {
    pub fn new() -> Self {
        Self {
            spool: Spool::new(),
            buffer: Vec::new(),
        }
    }

    pub async fn append(mut self, bytes: Bytes) -> Result<Self> {
        let mut remaining = bytes.as_ref();
        while !remaining.is_empty() {
            let length = remaining.len().min(IO_BUFFER_BYTES - self.buffer.len());
            let capacity = (self.buffer.len() + length).next_power_of_two();
            self.buffer.reserve_exact(capacity - self.buffer.len());
            self.buffer.extend_from_slice(&remaining[..length]);
            remaining = &remaining[length..];
            if self.buffer.len() == IO_BUFFER_BYTES {
                self = tokio::task::spawn_blocking(move || -> Result<Self> {
                    self.flush()?;
                    Ok(self)
                })
                .await??;
            }
        }
        Ok(self)
    }

    pub async fn finish(mut self) -> Result<Bytes> {
        tokio::task::spawn_blocking(move || {
            self.flush()?;
            self.spool.finish()
        })
        .await?
    }

    fn flush(&mut self) -> io::Result<()> {
        self.spool.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage_payload.rs"]
mod tests;

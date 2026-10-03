use super::*;
use std::{
    io::{BufRead, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::Duration,
};

#[tokio::test]
async fn independent_local_clients_share_atomic_generations_across_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let first = FileBucket::new(directory.path().into())?;
    let second = FileBucket::new(directory.path().into())?;
    let key = "runtime/owners/actor.json";
    assert!(
        first
            .compare_and_swap(key, None, bytes::Bytes::from_static(b"initial"))
            .await?
    );
    let generation = first.get(key).await?.unwrap().generation;
    let (one, two) = tokio::join!(
        first.compare_and_swap(key, Some(generation), bytes::Bytes::from_static(b"one")),
        second.compare_and_swap(key, Some(generation), bytes::Bytes::from_static(b"two"))
    );
    assert_ne!(one?, two?);
    drop(first);
    drop(second);
    let reopened = FileBucket::new(directory.path().into())?;
    let object = reopened.get(key).await?.unwrap();
    assert_eq!(object.generation, generation + 1);
    assert!(object.bytes.as_ref() == b"one" || object.bytes.as_ref() == b"two");
    assert_eq!(reopened.list("runtime/owners/").await?, [key]);
    assert!(reopened.list("other/").await?.is_empty());
    assert!(
        reopened
            .compare_and_swap("../escape", None, bytes::Bytes::new())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn an_object_lock_blocks_only_its_own_writes_and_survives_replacement() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = FileBucket::new(directory.path().into())?;
    let key = "owners/actor.json";
    assert!(
        bucket
            .compare_and_swap(key, None, bytes::Bytes::from_static(b"initial"))
            .await?
    );
    let mut holder = Client::start(directory.path(), key, "lock", "1")?;
    let stable_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("owners/.actor.json.lock"))?;
    let writer = FileBucket::new(directory.path().into())?;
    let mut pending = tokio::spawn(async move {
        writer
            .compare_and_swap(key, Some(1), bytes::Bytes::from_static(b"next"))
            .await
    });
    let blocked = tokio::time::timeout(Duration::from_millis(100), &mut pending).await;
    if blocked.is_ok() {
        anyhow::bail!("a write bypassed another process's object lock");
    }
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            bucket.compare_and_swap(
                "owners/other.json",
                None,
                bytes::Bytes::from_static(b"independent")
            ),
        )
        .await??
    );
    assert_eq!(bucket.get(key).await?.unwrap().bytes.as_ref(), b"initial");
    holder.process.kill()?;
    holder.process.wait()?;
    assert!(tokio::time::timeout(Duration::from_secs(2), pending).await???);
    stable_lock.lock()?;
    let writer = FileBucket::new(directory.path().into())?;
    let mut pending = tokio::spawn(async move {
        writer
            .compare_and_swap(key, Some(2), bytes::Bytes::from_static(b"last"))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    drop(stable_lock);
    assert!(tokio::time::timeout(Duration::from_secs(2), pending).await???);
    assert_eq!(bucket.list("owners/").await?, [key, "owners/other.json"]);
    Ok(())
}

#[tokio::test]
async fn competing_processes_have_one_winner_for_create_and_update() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let key = "owners/actor.json";
    let bucket = FileBucket::new(directory.path().into())?;
    for expected in ["absent", "1", "2"] {
        let mut first = Client::start(directory.path(), key, "cas", expected)?;
        let mut second = Client::start(directory.path(), key, "cas", expected)?;
        first.start_write()?;
        second.start_write()?;
        assert_ne!(first.result()?, second.result()?);
    }
    assert_eq!(bucket.get(key).await?.unwrap().generation, 3);
    Ok(())
}

#[tokio::test]
async fn readers_observe_complete_versions_during_replacement() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let writer = FileBucket::new(directory.path().into())?;
    let reader = FileBucket::new(directory.path().into())?;
    let key = "snapshots/state.json";
    let size = 200 * 1024;
    assert!(
        writer
            .compare_and_swap(key, None, vec![1; size].into())
            .await?
    );
    let writes = tokio::spawn(async move {
        for generation in 1..=32 {
            assert!(
                writer
                    .compare_and_swap(
                        key,
                        Some(generation),
                        vec![(generation + 1) as u8; size].into()
                    )
                    .await?
            );
        }
        Ok::<_, anyhow::Error>(())
    });
    loop {
        let object = reader.get(key).await?.unwrap();
        assert_eq!(object.bytes.len(), size);
        assert!(
            object
                .bytes
                .iter()
                .all(|byte| *byte == object.generation as u8)
        );
        if writes.is_finished() {
            break;
        }
    }
    writes.await??;
    assert_eq!(reader.get(key).await?.unwrap().generation, 33);
    Ok(())
}

struct Client {
    process: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Client {
    fn start(directory: &Path, key: &str, action: &str, expected: &str) -> Result<Self> {
        let mut process = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "bucket::file::tests::subprocess_client",
                "--nocapture",
            ])
            .env("BUCKET_TEST_DIRECTORY", directory)
            .env("BUCKET_TEST_KEY", key)
            .env("BUCKET_TEST_ACTION", action)
            .env("BUCKET_TEST_EXPECTED", expected)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = process.stdin.take().unwrap();
        let output = BufReader::new(process.stdout.take().unwrap());
        let mut client = Self {
            process,
            input,
            output,
        };
        client.read_marker("ready")?;
        Ok(client)
    }

    fn start_write(&mut self) -> Result<()> {
        writeln!(self.input, "go")?;
        Ok(())
    }

    fn result(&mut self) -> Result<String> {
        let result = self.read_marker("result=")?;
        ensure!(self.process.wait()?.success(), "bucket subprocess failed");
        Ok(result)
    }

    fn read_marker(&mut self, marker: &str) -> Result<String> {
        loop {
            let mut line = String::new();
            ensure!(
                self.output.read_line(&mut line)? > 0,
                "bucket subprocess exited before {marker}"
            );
            if let Some(start) = line.find(marker) {
                return Ok(line[start..].trim().to_owned());
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[test]
fn subprocess_client() -> Result<()> {
    let Some(directory) = std::env::var_os("BUCKET_TEST_DIRECTORY") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    let key = std::env::var("BUCKET_TEST_KEY")?;
    let action = std::env::var("BUCKET_TEST_ACTION")?;
    let path = directory.join(&key);
    let _lock = if action == "lock" {
        let name = path.file_name().unwrap().to_str().unwrap();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_file_name(format!(".{name}.lock")))?;
        lock.lock()?;
        Some(lock)
    } else {
        None
    };
    println!("ready");
    std::io::stdout().flush()?;
    std::io::stdin().read_line(&mut String::new())?;
    if action == "cas" {
        let expected = std::env::var("BUCKET_TEST_EXPECTED")?;
        let expected = if expected == "absent" {
            None
        } else {
            Some(expected.parse()?)
        };
        let bucket = FileBucket::new(directory)?;
        let result = tokio::runtime::Runtime::new()?.block_on(bucket.compare_and_swap(
            &key,
            expected,
            bytes::Bytes::from_static(b"child"),
        ))?;
        println!("result={result}");
    }
    Ok(())
}

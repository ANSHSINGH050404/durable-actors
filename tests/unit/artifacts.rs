use super::*;
use futures_util::stream;

fn artifact(path: &str, bytes: &[u8]) -> ArtifactFile {
    ArtifactFile {
        path: path.into(),
        object: "compiled/actors.mjs".into(),
        generation: 123,
        sha256: checksum(bytes),
    }
}

#[tokio::test]
async fn streamed_code_is_published_only_after_its_digest_matches() -> Result<()> {
    let root = tempfile::tempdir()?;
    tokio::fs::write(root.path().join("actors.mjs"), b"old").await?;
    let file = artifact("actors.mjs", b"new code");
    assert!(
        install_file(
            root.path(),
            &file,
            stream::iter([Ok(Bytes::from_static(b"wrong"))])
        )
        .await
        .is_err()
    );
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"old"
    );
    install_file(
        root.path(),
        &file,
        stream::iter([
            Ok(Bytes::from_static(b"new ")),
            Ok(Bytes::from_static(b"code")),
        ]),
    )
    .await?;
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"new code"
    );
    Ok(())
}

#[tokio::test]
async fn artifact_paths_cannot_escape_the_sandbox_code_directory() -> Result<()> {
    let root = tempfile::tempdir()?;
    for path in ["../outside.mjs", "/outside.mjs", "nested/../../outside.mjs"] {
        assert!(
            install_file(
                root.path(),
                &artifact(path, b"code"),
                stream::iter([Ok(Bytes::from_static(b"code"))])
            )
            .await
            .is_err()
        );
    }
    Ok(())
}

fn checksum(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&SHA256, bytes).as_ref())
}

#[test]
fn python_code_manifests_include_the_entrypoint_and_installed_dependencies() -> Result<()> {
    let manifest = ArtifactManifest {
        bucket: "code-bucket".into(),
        files: vec![
            artifact("actors.pyz", b"code"),
            artifact("python/dependency/module.so", b"native"),
        ],
    };
    let decoded = ArtifactManifest::decode(&manifest.encode()?)?;
    assert_eq!(decoded.entrypoint()?, "actors.pyz");
    assert_eq!(decoded.files.len(), 2);
    Ok(())
}

#[test]
fn compiled_artifact_collection_preserves_dependency_paths() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("python/dependency"))?;
    std::fs::write(root.path().join("actors.pyz"), b"code")?;
    std::fs::write(root.path().join("python/dependency/module.so"), b"native")?;
    assert_eq!(
        compiled_files(root.path())?,
        vec!["actors.pyz", "python/dependency/module.so"]
    );
    Ok(())
}

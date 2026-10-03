use super::*;

pub(crate) struct UnusedSandboxProvider;
#[async_trait]
impl SandboxProvider for UnusedSandboxProvider {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected host assignment")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

pub(crate) fn code_artifact(generation: i64) -> String {
    use base64::Engine;
    crate::artifacts::ArtifactManifest {
        bucket: "test-artifacts".into(),
        files: vec![crate::artifacts::ArtifactFile {
            path: "actors.mjs".into(),
            object: "durable-actors/v3/artifacts/00000000-0000-4000-8000-000000000001/actors.mjs"
                .into(),
            generation,
            sha256: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0; 32]),
        }],
    }
    .encode()
    .unwrap()
}

#[test]
fn customer_process_cannot_inspect_host_credentials() -> anyhow::Result<()> {
    super::protect_runtime_credentials()?;
    let status = std::process::Command::new("/bin/sh")
        .args(["-c", "cat /proc/$PPID/environ >/dev/null"])
        .stderr(std::process::Stdio::null())
        .status()?;
    assert!(!status.success());
    Ok(())
}

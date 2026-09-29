use anyhow::{Context, Result, ensure};
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub(crate) struct Access {
    secret: String,
}
impl Access {
    pub fn new(secret: String) -> Result<Self> {
        ensure!(
            secret.len() >= 32,
            "replica secret must contain at least 32 bytes"
        );
        Ok(Self { secret })
    }
    pub fn admin(&self) -> &str {
        &self.secret
    }
    pub fn scoped(&self, actor: &crate::actor::ActorKey) -> Result<String> {
        let prefix = crate::storage_paths::snapshots(actor)?;
        let payload = URL_SAFE_NO_PAD.encode(prefix);
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, self.secret.as_bytes()),
            payload.as_bytes(),
        );
        Ok(format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }
    pub fn authorize(&self, token: &str, scope: Option<&str>) -> Result<()> {
        if bool::from(token.as_bytes().ct_eq(self.secret.as_bytes())) {
            return Ok(());
        }
        let scope = scope.context("replica administration requires the server credential")?;
        let (payload, signature) = token
            .split_once('.')
            .context("invalid replica capability")?;
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, self.secret.as_bytes()),
            payload.as_bytes(),
            &URL_SAFE_NO_PAD.decode(signature)?,
        )
        .map_err(|_| anyhow::anyhow!("invalid replica capability"))?;
        let prefix = String::from_utf8(URL_SAFE_NO_PAD.decode(payload)?)?;
        ensure!(
            scope.starts_with(&prefix),
            "replica capability belongs to another actor"
        );
        Ok(())
    }
}

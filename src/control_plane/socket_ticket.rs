use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::{ActorKey, validate_socket_metadata},
    placement::validate_region,
};

pub(crate) struct SocketGrant {
    pub actor: ActorKey,
    pub region: String,
    pub home_region: Option<String>,
    pub metadata: Value,
}

impl SocketGrant {
    pub(crate) fn validate(&self) -> Result<()> {
        self.actor.validate()?;
        validate_region(&self.region)?;
        if let Some(region) = &self.home_region {
            validate_region(region)?;
        }
        validate_socket_metadata(&self.metadata)?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SocketTicket {
    pub iss: String,
    pub aud: String,
    pub scope: String,
    pub iat: i64,
    pub nbf: i64,
    pub exp: i64,
    pub actor: ActorKey,
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_region: Option<String>,
    pub metadata: Value,
    pub connect_by_ms: i64,
}

impl SocketTicket {
    pub(crate) fn validate(&self, now_ms: i64) -> Result<()> {
        self.actor.validate()?;
        validate_region(&self.region)?;
        if let Some(region) = &self.home_region {
            validate_region(region)?;
        }
        validate_socket_metadata(&self.metadata)?;
        ensure!(self.scope == "actor:socket", "invalid socket ticket scope");
        ensure!(
            self.iat * 1000 <= now_ms && self.nbf * 1000 <= now_ms,
            "socket ticket is not active"
        );
        ensure!(now_ms < self.connect_by_ms, "socket ticket has expired");
        ensure!(
            self.connect_by_ms - self.iat * 1000 <= 61_000,
            "invalid socket ticket admission lifetime"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct SocketTicketVerifier {
    keys: jsonwebtoken::jwk::JwkSet,
    issuer: String,
    audience: String,
}

impl SocketTicketVerifier {
    pub(crate) fn new(keys: &str, issuer: String, audience: String) -> Result<Self> {
        Ok(Self {
            keys: serde_json::from_str(keys)?,
            issuer,
            audience,
        })
    }

    pub(crate) fn verify(&self, token: &str) -> Result<SocketTicket> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;
        self.verify_at(token, now)
    }

    pub(crate) fn verify_at(&self, token: &str, now_ms: i64) -> Result<SocketTicket> {
        use anyhow::Context;
        use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
        ensure!(token.len() <= 128 * 1024, "socket ticket is too large");
        let header = decode_header(token)?;
        ensure!(
            header.alg == Algorithm::EdDSA,
            "invalid socket ticket algorithm"
        );
        let key = self
            .keys
            .find(
                header
                    .kid
                    .as_deref()
                    .context("socket signing key missing")?,
            )
            .context("unknown socket signing key")?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        let ticket =
            decode::<SocketTicket>(token, &DecodingKey::from_jwk(key)?, &validation)?.claims;
        ticket.validate(now_ms)?;
        Ok(ticket)
    }
}

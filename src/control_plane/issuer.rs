use std::time::Duration;

use super::socket_ticket::{SocketGrant, SocketTicket};
use anyhow::{Context, Result, ensure};
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use jsonwebtoken::{
    Algorithm, EncodingKey, Header, encode,
    jwk::{Jwk, JwkSet, PublicKeyUse},
};
use serde::Serialize;

use crate::{
    actor::ActorKey, control_plane::auth::ActorInvocationCapability, host::HostId,
    placement::validate_region,
};

const HOST_TOKEN_TTL: Duration = Duration::from_secs(1_800);
const INVOCATION_TARGET_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct ActorJwtIssuer {
    encoding_key: EncodingKey,
    public_key: Jwk,
    key_id: String,
    issuer: String,
    authority_audience: String,
    invocation_audience: String,
    max_lifetime: Duration,
}

pub(crate) struct IssuedActorToken {
    pub token: String,
    pub expires_at_ms: i64,
}

impl ActorJwtIssuer {
    pub(crate) fn from_base64_pkcs8(
        encoded_key: &str,
        key_id: impl Into<String>,
        issuer: impl Into<String>,
        authority_audience: impl Into<String>,
        invocation_audience: impl Into<String>,
        max_lifetime: Duration,
    ) -> Result<Self> {
        let key_id = key_id.into();
        ensure!(
            !key_id.is_empty(),
            "DURABLE_ACTORS_JWT_KEY_ID must not be empty"
        );
        ensure!(
            !max_lifetime.is_zero(),
            "actor JWT lifetime must be positive"
        );
        let issuer = issuer.into();
        let authority_audience = authority_audience.into();
        let invocation_audience = invocation_audience.into();
        ensure!(!issuer.is_empty(), "actor JWT issuer must not be empty");
        ensure!(
            !authority_audience.is_empty(),
            "actor authority JWT audience must not be empty"
        );
        ensure!(
            !invocation_audience.is_empty(),
            "actor invocation JWT audience must not be empty"
        );
        let pkcs8 = STANDARD
            .decode(encoded_key)
            .context("DURABLE_ACTORS_JWT_SIGNING_KEY must be base64-encoded PKCS#8")?;
        let key_pair = Ed25519KeyPair::from_pkcs8(&pkcs8)
            .context("DURABLE_ACTORS_JWT_SIGNING_KEY is not an Ed25519 PKCS#8 key")?;
        let encoding_key = EncodingKey::from_ed_der(&pkcs8);
        let mut public_key: Jwk = serde_json::from_value(serde_json::json!({
            "alg": "EdDSA",
            "crv": "Ed25519",
            "kty": "OKP",
            "x": URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref())
        }))
        .context("derive the actor JWT public key")?;
        public_key.common.key_id = Some(key_id.clone());
        public_key.common.public_key_use = Some(PublicKeyUse::Signature);
        Ok(Self {
            encoding_key,
            public_key,
            key_id,
            issuer,
            authority_audience,
            invocation_audience,
            max_lifetime,
        })
    }

    pub(crate) fn verifier_keys_json(&self) -> Result<String> {
        Ok(String::from_utf8(self.jwks_json()?)?)
    }

    pub(super) fn session_verifier(&self) -> Result<super::session::SessionVerifier> {
        super::session::SessionVerifier::new(
            &self.verifier_keys_json()?,
            &self.issuer,
            &self.session_audience(),
        )
    }

    pub(super) fn issue_session(
        &self,
        project_id: String,
        subject: String,
        deadline_ms: i64,
    ) -> Result<IssuedActorToken> {
        let now = unix_millis()? / 1000;
        let expires_at = (deadline_ms / 1000).min(
            now.saturating_add(i64::try_from(
                self.max_lifetime
                    .as_secs()
                    .min(super::session::MAX_SESSION_LIFETIME_SECONDS as u64),
            )?),
        );
        let session = super::session::ActorSession {
            iss: self.issuer.clone(),
            aud: self.session_audience(),
            subject,
            jti: uuid::Uuid::new_v4().to_string(),
            project_id,
            scope: "actor:session".into(),
            iat: now,
            nbf: now,
            expires_at,
        };
        session.validate(now)?;
        let token = self.sign(&session)?;
        ensure!(token.len() <= 8192, "actor session is too large");
        Ok(IssuedActorToken {
            token,
            expires_at_ms: expires_at * 1000,
        })
    }

    pub(super) fn issue_socket(&self, grant: SocketGrant) -> Result<(String, i64)> {
        self.issue_socket_at(grant, unix_millis()?)
    }

    fn issue_socket_at(&self, grant: SocketGrant, now_ms: i64) -> Result<(String, i64)> {
        grant.validate()?;
        let lifetime = duration_millis(self.max_lifetime)?.min(60_000);
        ensure!(
            lifetime >= 1000,
            "configured token lifetime is too short for a socket"
        );
        let connect_by_ms = now_ms
            .checked_add(lifetime)
            .context("socket admission time overflow")?;
        let claims = SocketTicket {
            iss: self.issuer.clone(),
            aud: format!("{}:websocket", self.authority_audience),
            scope: "actor:socket".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: (connect_by_ms + 999) / 1000,
            actor: grant.actor,
            region: grant.region,
            home_region: grant.home_region,
            metadata: grant.metadata,
            connect_by_ms,
        };
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.key_id.clone());
        Ok((encode(&header, &claims, &self.encoding_key)?, connect_by_ms))
    }

    #[cfg(test)]
    pub(super) fn verify_socket(&self, token: &str) -> Result<SocketTicket> {
        self.verify_socket_at(token, unix_millis()?)
    }

    #[cfg(test)]
    fn verify_socket_at(&self, token: &str, now_ms: i64) -> Result<SocketTicket> {
        self.socket_verifier()?.verify_at(token, now_ms)
    }

    pub(crate) fn invocation_verifier(&self) -> Result<super::ActorJwtVerifier> {
        super::ActorJwtVerifier::for_scope(
            self.verifier_keys_json()?,
            self.issuer.clone(),
            self.invocation_audience.clone(),
            super::ActorTokenPurpose::Invocation,
            self.max_lifetime,
        )
    }

    pub(crate) fn socket_audience(&self) -> String {
        format!("{}:websocket", self.authority_audience)
    }

    pub(crate) fn socket_verifier(&self) -> Result<super::socket_ticket::SocketTicketVerifier> {
        super::socket_ticket::SocketTicketVerifier::new(
            &self.verifier_keys_json()?,
            self.issuer.clone(),
            self.socket_audience(),
        )
    }

    pub(crate) fn jwks_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&JwkSet {
            keys: vec![self.public_key.clone()],
        })?)
    }

    pub(crate) fn issue_host(
        &self,
        host_id: &HostId,
        session_id: &str,
        host_config_key: &str,
        region: &str,
        actor: &ActorKey,
    ) -> Result<IssuedActorToken> {
        actor.validate()?;
        let now_ms = unix_millis()?;
        let expires_at_ms =
            now_ms.saturating_add(duration_millis(self.max_lifetime.min(HOST_TOKEN_TTL))?);
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![
                self.authority_audience.clone(),
                self.invocation_audience.clone(),
            ],
            sub: host_id.as_str().to_owned(),
            process_id: host_id.as_str().to_owned(),
            session_id: session_id.to_owned(),
            region: region.to_owned(),
            host_config_key: Some(host_config_key.to_owned()),
            scope: "actor:authority actor:invoke".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: expires_at_ms / 1000,
            invocation: None,
            actor: actor.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn issue_invocation_target(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        session_id: &str,
        host_config_key: &str,
        region: &str,
        owner_epoch: u64,
        grant: Option<super::session::InvocationGrant>,
        route: &str,
    ) -> Result<IssuedActorToken> {
        actor.validate()?;
        validate_region(region)?;
        ensure!(owner_epoch > 0, "actor owner epoch must be positive");
        let now_ms = unix_millis()?;
        let now = now_ms / 1_000;
        let target_expires_at = now.saturating_add(i64::try_from(INVOCATION_TARGET_TTL.as_secs())?);
        let issuer_expires_at = now.saturating_add(i64::try_from(self.max_lifetime.as_secs())?);
        let expires_at = target_expires_at
            .min(issuer_expires_at)
            .min(grant.as_ref().map_or(i64::MAX, |grant| grant.expires_at));
        ensure!(expires_at > now, "invocation credential expires too soon");
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![self.invocation_audience.clone()],
            sub: host_id.as_str().to_owned(),
            process_id: host_id.as_str().to_owned(),
            session_id: session_id.to_owned(),
            region: region.to_owned(),
            host_config_key: Some(host_config_key.to_owned()),
            scope: if grant.is_some() {
                "actor:delegated-invoke"
            } else {
                "actor:invoke"
            }
            .into(),
            iat: now,
            nbf: now,
            exp: expires_at,
            actor: actor.clone(),
            invocation: Some(ActorInvocationCapability {
                route: super::gateway::backend_origin(route)?.to_string(),
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
                grant,
            }),
        })
    }

    fn session_audience(&self) -> String {
        format!("{}:session", self.invocation_audience)
    }

    fn issue(&self, claims: ActorJwtClaims) -> Result<IssuedActorToken> {
        let expires_at_ms = claims
            .exp
            .checked_mul(1_000)
            .context("issued actor token expiration overflow")?;
        Ok(IssuedActorToken {
            token: self.sign(&claims)?,
            expires_at_ms,
        })
    }

    fn sign(&self, claims: &impl Serialize) -> Result<String> {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.key_id.clone());
        encode(&header, claims, &self.encoding_key).context("sign actor JWT")
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    actor: ActorKey,
    iss: String,
    aud: Vec<String>,
    sub: String,
    #[serde(rename = "processId")]
    process_id: String,
    session_id: String,
    #[serde(rename = "storageRegion")]
    region: String,
    host_config_key: Option<String>,
    scope: String,
    iat: i64,
    nbf: i64,
    exp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation: Option<ActorInvocationCapability>,
}

fn unix_millis() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported JWT range")
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).context("duration exceeds supported JWT range")
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/issuer.rs"]
mod tests;

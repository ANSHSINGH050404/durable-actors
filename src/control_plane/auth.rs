use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm},
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

use crate::host::HostId;

const AUTHORIZATION: &str = "authorization";
const CLOCK_SKEW: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorTokenPurpose {
    ControlPlane,
    Invocation,
}

impl ActorTokenPurpose {
    fn claim(self) -> &'static str {
        match self {
            Self::ControlPlane => "actor:authority",
            Self::Invocation => "actor:invoke",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ActorPrincipal {
    pub actor: crate::actor::ActorKey,
    pub host_id: HostId,
    pub session_id: String,
    pub region: String,
    pub host_config_key: Option<String>,
    pub invocation: Option<ActorInvocationCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActorInvocationCapability {
    pub actor: crate::actor::ActorKey,
    pub host_id: HostId,
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<super::session::InvocationGrant>,
}

#[derive(Clone)]
pub(crate) struct ActorJwtVerifier {
    public_keys: Arc<HashMap<String, DecodingKey>>,
    validation: Validation,
    purpose: ActorTokenPurpose,
    max_lifetime: Duration,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    actor: crate::actor::ActorKey,
    sub: String,
    #[serde(rename = "processId")]
    host_id: String,
    session_id: String,
    #[serde(rename = "storageRegion")]
    region: String,
    host_config_key: Option<String>,
    scope: String,
    iat: i64,
    #[serde(rename = "nbf")]
    _not_before: i64,
    exp: i64,
    #[serde(default)]
    invocation: Option<ActorInvocationCapability>,
}

impl ActorJwtVerifier {
    #[cfg(test)]
    pub(crate) fn new(
        public_keys_json: impl AsRef<str>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        max_lifetime: Duration,
    ) -> Result<Self> {
        Self::for_scope(
            public_keys_json,
            issuer,
            audience,
            ActorTokenPurpose::ControlPlane,
            max_lifetime,
        )
    }

    pub(crate) fn for_scope(
        public_keys_json: impl AsRef<str>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        purpose: ActorTokenPurpose,
        max_lifetime: Duration,
    ) -> Result<Self> {
        let public_keys = decode_public_keys(public_keys_json.as_ref())?;
        Self::from_decoded_public_keys(public_keys, issuer, audience, purpose, max_lifetime)
    }

    pub(crate) async fn authenticate<T>(
        &self,
        request: &Request<T>,
    ) -> Result<ActorPrincipal, Status> {
        let authorization = request
            .metadata()
            .get(AUTHORIZATION)
            .ok_or_else(|| Status::unauthenticated("actor token is required"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("actor token is not valid metadata"))?;
        self.authenticate_authorization(authorization)
            .map_err(|error| Status::unauthenticated(format!("{error:#}")))
    }

    pub(crate) fn authenticate_authorization(&self, authorization: &str) -> Result<ActorPrincipal> {
        let token = bearer_token(authorization)?;
        let header = decode_header(token).context("actor token header is invalid")?;
        self.verify_with_header(token, header)
    }

    fn from_decoded_public_keys(
        public_keys: HashMap<String, DecodingKey>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        purpose: ActorTokenPurpose,
        max_lifetime: Duration,
    ) -> Result<Self> {
        ensure!(
            !max_lifetime.is_zero(),
            "actor JWT maximum lifetime must be positive"
        );
        ensure!(
            !public_keys.is_empty(),
            "actor JWT public-key set must not be empty"
        );
        let issuer = issuer.into();
        let audience = audience.into();
        ensure!(!issuer.is_empty(), "actor JWT issuer must not be empty");
        ensure!(!audience.is_empty(), "actor JWT audience must not be empty");
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = CLOCK_SKEW.as_secs();
        Ok(Self {
            public_keys: Arc::new(public_keys),
            validation,
            purpose,
            max_lifetime,
        })
    }

    #[cfg(test)]
    fn verify(&self, token: &str) -> Result<ActorPrincipal> {
        self.verify_with_header(token, decode_header(token)?)
    }

    fn verify_with_header(
        &self,
        token: &str,
        header: jsonwebtoken::Header,
    ) -> Result<ActorPrincipal> {
        ensure!(
            header.typ.as_deref().is_none_or(|value| value == "JWT"),
            "actor token has an invalid type"
        );
        let key_id = header.kid.context("actor token key ID is empty")?;
        let public_key = self
            .public_keys
            .get(&key_id)
            .with_context(|| format!("actor token uses unknown key ID {key_id:?}"))?;
        let claims = decode::<ActorJwtClaims>(token, public_key, &self.validation)
            .context("verify actor JWT")?
            .claims;
        self.validate_claims(claims)
    }

    fn validate_claims(&self, claims: ActorJwtClaims) -> Result<ActorPrincipal> {
        let delegated = claims
            .invocation
            .as_ref()
            .is_some_and(|capability| capability.grant.is_some());
        let valid_scope = if delegated {
            self.purpose == ActorTokenPurpose::Invocation
                && claims.scope == "actor:delegated-invoke"
        } else {
            claims
                .scope
                .split_ascii_whitespace()
                .any(|scope| scope == self.purpose.claim())
                && !claims
                    .scope
                    .split_ascii_whitespace()
                    .any(|scope| scope == "actor:delegated-invoke")
        };
        ensure!(valid_scope, "actor token credential scope is invalid");
        ensure!(!claims.sub.is_empty(), "actor token subject is empty");
        ensure!(
            claims.sub == claims.host_id,
            "host token subject does not match its process identity"
        );
        ensure!(
            uuid::Uuid::parse_str(&claims.session_id).is_ok(),
            "actor token session ID is invalid"
        );
        ensure!(claims.exp > claims.iat, "actor token lifetime is invalid");
        let lifetime_seconds = u64::try_from(
            claims
                .exp
                .checked_sub(claims.iat)
                .context("actor token lifetime is invalid")?,
        )
        .context("actor token lifetime is invalid")?;
        ensure!(
            Duration::from_secs(lifetime_seconds) <= self.max_lifetime,
            "actor token lifetime exceeds the configured maximum"
        );

        let now = unix_seconds()?;
        ensure!(claims.exp > now, "actor token has expired");
        let skew_seconds = i64::try_from(CLOCK_SKEW.as_secs()).unwrap_or(i64::MAX);
        ensure!(
            claims.iat <= now.saturating_add(skew_seconds),
            "actor token was issued in the future"
        );
        claims.actor.validate()?;
        let principal = ActorPrincipal {
            actor: claims.actor,
            host_id: HostId::new(claims.host_id),
            session_id: claims.session_id,
            region: claims.region,
            host_config_key: claims.host_config_key,
            invocation: claims.invocation,
        };
        ensure!(
            principal.host_id.as_str().starts_with("host.v3."),
            "invalid host identity"
        );
        ensure!(
            !principal.region.is_empty()
                && principal.region.len() <= 64
                && principal
                    .region
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')),
            "actor token storage region is invalid"
        );
        if let Some(capability) = &principal.invocation {
            if let Some(grant) = &capability.grant {
                ensure!(
                    claims.exp <= grant.expires_at && grant.expires_at > now,
                    "delegated authorization has expired"
                );
                ensure!(
                    !grant.subject.is_empty()
                        && grant.subject.len() <= 128
                        && !grant.grant_id.is_empty()
                        && grant.grant_id.len() <= 128,
                    "delegated identity is invalid"
                );
                ensure!(grant.methods.len() <= 256, "too many delegated methods");
                for method in &grant.methods {
                    super::admin::validate_component("delegated method", method, 128)?;
                }
            }
            ensure!(
                capability.host_id == principal.host_id,
                "actor invocation capability targets another host"
            );
            capability.actor.validate()?;
            ensure!(
                capability.owner_epoch > 0,
                "actor invocation capability owner epoch is invalid"
            );
        }
        Ok(principal)
    }
}

pub(super) fn decode_public_keys(public_keys_json: &str) -> Result<HashMap<String, DecodingKey>> {
    let keys: JwkSet = serde_json::from_str(public_keys_json)
        .context("parse durable-actors JWT public keys as a JWK set")?;
    ensure!(
        !keys.keys.is_empty(),
        "durable-actors JWT public keys must contain at least one key"
    );
    keys.keys
        .into_iter()
        .map(|key| {
            let key_id = key
                .common
                .key_id
                .clone()
                .context("actor JWT key ID must not be empty")?;
            ensure!(
                key.common.key_algorithm == Some(KeyAlgorithm::EdDSA),
                "actor JWT public key {key_id:?} must use EdDSA"
            );
            let decoding_key = DecodingKey::from_jwk(&key)
                .with_context(|| format!("decode actor JWT public key {key_id:?}"))?;
            Ok((key_id, decoding_key))
        })
        .collect()
}

pub(super) fn bearer_token(authorization: &str) -> Result<&str> {
    let token = authorization
        .strip_prefix("Bearer ")
        .context("actor token must use Bearer authentication")?;
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token is invalid"
    );
    Ok(token)
}

pub(super) fn unix_seconds() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_secs()).context("system clock exceeds supported JWT range")
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/auth.rs"]
mod tests;

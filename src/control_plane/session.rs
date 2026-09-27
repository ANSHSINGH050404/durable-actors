use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};

use super::{
    admin::validate_component,
    auth::{bearer_token, decode_public_keys, unix_seconds},
};
use crate::actor::ActorKey;

#[derive(Clone)]
pub(crate) struct SessionVerifier {
    keys: Arc<HashMap<String, DecodingKey>>,
    validation: Validation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActorSession {
    pub iss: String,
    pub aud: String,
    #[serde(rename = "sub")]
    pub subject: String,
    pub jti: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    pub scope: String,
    pub iat: i64,
    pub nbf: i64,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InvocationGrant {
    pub subject: String,
    pub grant_id: String,
    pub expires_at: i64,
    pub methods: Vec<String>,
}

impl SessionVerifier {
    pub(super) fn new(keys: &str, issuer: &str, audience: &str) -> Result<Self> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp", "nbf"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        Ok(Self {
            keys: Arc::new(decode_public_keys(keys)?),
            validation,
        })
    }

    pub(crate) fn authenticate(
        &self,
        authorization: &str,
        project_id: &str,
    ) -> Result<ActorSession> {
        let token = bearer_token(authorization)?;
        ensure!(token.len() <= 8192, "actor session is too large");
        let header = decode_header(token)?;
        ensure!(
            header.typ.as_deref() == Some("JWT"),
            "invalid actor session type"
        );
        let key = self
            .keys
            .get(
                header
                    .kid
                    .as_deref()
                    .context("actor session key ID is required")?,
            )
            .context("unknown actor session key")?;
        let session = decode::<ActorSession>(token, key, &self.validation)?.claims;
        session.validate(unix_seconds()?)?;
        ensure!(
            session.project_id == project_id,
            "actor session project mismatch"
        );
        Ok(session)
    }
}

impl ActorSession {
    pub(super) fn validate(&self, now: i64) -> Result<()> {
        ensure!(
            self.scope == "actor:session",
            "actor session purpose mismatch"
        );
        validate_component("project ID", &self.project_id, 64)?;
        ensure!(
            !self.subject.is_empty()
                && self.subject.len() <= 128
                && !self.jti.is_empty()
                && self.jti.len() <= 128,
            "invalid actor session identity"
        );
        ensure!(
            self.iat <= now + 5
                && self.expires_at > now
                && self.expires_at > self.iat
                && self.expires_at.saturating_sub(self.iat) <= 60,
            "invalid actor session lifetime"
        );
        Ok(())
    }

    pub(crate) fn invocation(
        self,
        actor: &ActorKey,
        published_methods: Vec<String>,
    ) -> Result<InvocationGrant> {
        ensure!(
            self.project_id == actor.project_id,
            "actor session project mismatch"
        );
        ensure!(
            !published_methods.is_empty(),
            "actor has no published RPC methods"
        );
        Ok(InvocationGrant {
            subject: self.subject,
            grant_id: self.jti,
            expires_at: self.expires_at,
            methods: published_methods,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/session.rs"]
mod tests;

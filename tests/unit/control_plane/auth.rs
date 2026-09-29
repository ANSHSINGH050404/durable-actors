use aws_lc_rs::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::json;

use super::*;

#[test]
fn rejects_tokens_without_actor_scope() -> Result<()> {
    let (verifier, key_pair) = verifier_and_key_pair()?;
    let mut claims = valid_claims(unix_seconds()?);
    claims.as_object_mut().unwrap().remove("actor");
    let token = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
        claims,
    )?;
    assert!(verifier.verify(&token).is_err());
    Ok(())
}

#[test]
fn verifies_a_signed_actor_token() -> Result<()> {
    let (verifier, key_pair) = verifier_and_key_pair()?;
    let now = unix_seconds()?;
    let principal = verifier.verify(&token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
        valid_claims(now),
    )?)?;

    assert_eq!(
        principal.host_id,
        HostId::new("host.v3.00000000-0000-4000-8000-000000000001")
    );
    assert_eq!(principal.session_id, "00000000-0000-4000-8000-000000000002");
    Ok(())
}

#[test]
fn invocation_credentials_are_distinct_from_control_plane_credentials() -> Result<()> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())?;
    let keys = public_key_set(&key_pair)?;
    let invocation = ActorJwtVerifier::for_scope(
        &keys,
        "durable-actors-control-plane",
        "durable-actors-invoke",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let control_plane = ActorJwtVerifier::for_scope(
        keys,
        "durable-actors-control-plane",
        "durable-actors-authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let mut claims = valid_claims(unix_seconds()?);
    claims["aud"] = json!("durable-actors-invoke");
    claims["scope"] = json!("actor:invoke");
    claims["hostConfigKey"] = json!("revision-1");
    let token = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
        claims,
    )?;

    assert!(invocation.verify(&token).is_ok());
    assert!(control_plane.verify(&token).is_err());
    Ok(())
}

#[test]
fn delegated_scope_requires_a_restricted_capability() -> Result<()> {
    let (_, key_pair) = verifier_and_key_pair()?;
    let verifier = ActorJwtVerifier::for_scope(
        public_key_set(&key_pair)?,
        "durable-actors-control-plane",
        "durable-actors-invoke",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let now = unix_seconds()?;
    let mut claims = valid_claims(now);
    claims["aud"] = json!("durable-actors-invoke");
    claims["scope"] = json!("actor:delegated-invoke");
    let sign = |claims| {
        token(
            &key_pair,
            json!({"alg": "EdDSA", "kid": "test-key", "typ": "JWT"}),
            claims,
        )
    };
    assert!(verifier.verify(&sign(claims.clone())?).is_err());
    claims["invocation"] = json!({
        "actor": claims["actor"], "hostId": claims["processId"], "ownerEpoch": 1, "route":"http://host",
        "grant": { "subject": "credential-a", "grantId": "grant-a", "expiresAt": now + 60, "methods": ["increment"] }
    });
    assert!(verifier.verify(&sign(claims.clone())?).is_ok());
    claims["scope"] = json!("actor:invoke");
    assert!(verifier.verify(&sign(claims.clone())?).is_err());
    claims["scope"] = json!("actor:delegated-invoke");
    claims["invocation"]["grant"]["expiresAt"] = json!(now + 30);
    assert!(verifier.verify(&sign(claims)?).is_err());
    Ok(())
}

#[test]
fn rejects_an_expired_token_during_clock_skew_leeway() -> Result<()> {
    let (verifier, key_pair) = verifier_and_key_pair()?;
    let now = unix_seconds()?;
    let expired = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
        json!({
            "iss": "durable-actors-control-plane",
            "aud": "durable-actors-authority",
            "sub": "host.v3.00000000-0000-4000-8000-000000000001",
            "processId": "host.v3.00000000-0000-4000-8000-000000000001",
            "sessionId": "00000000-0000-4000-8000-000000000002",
            "storageRegion": "us-east",
            "scope": "actor:authority",
            "iat": now - 60,
            "nbf": now - 60,
            "exp": now - 1
        }),
    )?;

    assert!(verifier.verify(&expired).is_err());
    Ok(())
}

#[test]
fn rejects_tampering_and_invalid_constraints() -> Result<()> {
    let (verifier, key_pair) = verifier_and_key_pair()?;
    let now = unix_seconds()?;
    let valid = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
        valid_claims(now),
    )?;
    let mut tampered = valid.into_bytes();
    let last = tampered.len() - 1;
    tampered[last] = if tampered[last] == b'a' { b'b' } else { b'a' };
    assert!(verifier.verify(std::str::from_utf8(&tampered)?).is_err());

    let wrong_audience = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key" }),
        json!({
            "iss": "durable-actors-control-plane",
            "aud": "somewhere-else",
            "sub": "host.v3.00000000-0000-4000-8000-000000000001",
            "processId": "host.v3.00000000-0000-4000-8000-000000000001",
            "sessionId": "00000000-0000-4000-8000-000000000002",
            "scope": "actor:authority",
            "iat": now,
            "nbf": now,
            "exp": now + 60
        }),
    )?;
    assert!(verifier.verify(&wrong_audience).is_err());

    let expired = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key" }),
        json!({
            "iss": "durable-actors-control-plane",
            "aud": "durable-actors-authority",
            "sub": "host.v3.00000000-0000-4000-8000-000000000001",
            "processId": "host.v3.00000000-0000-4000-8000-000000000001",
            "sessionId": "00000000-0000-4000-8000-000000000002",
            "scope": "actor:authority",
            "iat": now - 60,
            "nbf": now - 60,
            "exp": now - 10
        }),
    )?;
    assert!(verifier.verify(&expired).is_err());

    let unknown_key = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "retired-key" }),
        valid_claims(now),
    )?;
    assert!(verifier.verify(&unknown_key).is_err());

    let too_long = token(
        &key_pair,
        json!({ "alg": "EdDSA", "kid": "test-key" }),
        json!({
            "iss": "durable-actors-control-plane",
            "aud": "durable-actors-authority",
            "sub": "host.v3.00000000-0000-4000-8000-000000000001",
            "processId": "host.v3.00000000-0000-4000-8000-000000000001",
            "sessionId": "00000000-0000-4000-8000-000000000002",
            "scope": "actor:authority",
            "iat": now,
            "nbf": now,
            "exp": now + 61
        }),
    )?;
    assert!(verifier.verify(&too_long).is_err());
    Ok(())
}

fn verifier_and_key_pair() -> Result<(ActorJwtVerifier, Ed25519KeyPair)> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())?;
    let keys = public_key_set(&key_pair)?;
    Ok((
        ActorJwtVerifier::new(
            keys,
            "durable-actors-control-plane",
            "durable-actors-authority",
            Duration::from_secs(60),
        )?,
        key_pair,
    ))
}

fn token(
    key_pair: &Ed25519KeyPair,
    header: serde_json::Value,
    claims: serde_json::Value,
) -> Result<String> {
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    let input = format!("{header}.{claims}");
    let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(input.as_bytes()).as_ref());
    Ok(format!("{input}.{signature}"))
}

fn public_key_set(key_pair: &Ed25519KeyPair) -> Result<String> {
    Ok(serde_json::to_string(&json!({
        "keys": [{
            "alg": "EdDSA",
            "crv": "Ed25519",
            "kid": "test-key",
            "kty": "OKP",
            "use": "sig",
            "x": URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref())
        }]
    }))?)
}

fn valid_claims(now: i64) -> serde_json::Value {
    json!({
        "actor": {"project_id":"default","actor_name": "Counter", "actor_id": "one"},
        "iss": "durable-actors-control-plane",
        "aud": "durable-actors-authority",
        "sub": "host.v3.00000000-0000-4000-8000-000000000001",
        "processId": "host.v3.00000000-0000-4000-8000-000000000001",
        "sessionId": "00000000-0000-4000-8000-000000000002",
        "storageRegion": "default",
        "scope": "actor:authority",
        "iat": now,
        "nbf": now,
        "exp": now + 60
    })
}

use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};

use super::*;
use crate::control_plane::{ActorJwtVerifier, ActorTokenPurpose};

#[test]
fn socket_tickets_bind_actor_metadata_with_short_admission() -> Result<()> {
    let issuer = socket_issuer()?;
    let now = 1_700_000_000_000;
    let grant = || SocketGrant {
        actor: ActorKey {
            project_id: "default".into(),
            actor_name: "Room".into(),
            actor_id: "lobby".into(),
        },
        region: "us-east".into(),
        home_region: Some("us-east".into()),
        metadata: serde_json::json!({"userId":"alice"}),
        authorization_lifetime_ms: 900_000,
    };
    let (token, connect_by, authorized_until) = issuer.issue_socket_at(grant(), now)?;
    assert_eq!(connect_by, now + 60_000);
    assert_eq!(authorized_until, now + 900_000);
    let claims = issuer.verify_socket_at(&token, now + 1)?;
    assert_eq!(claims.actor, grant().actor);
    assert_eq!(claims.metadata, grant().metadata);
    assert_eq!(claims.authorized_until_ms, now + 900_000);
    assert!(issuer.verify_socket_at(&token, now + 60_000).is_err());

    assert!(issuer.verify_socket_at(&token, now - 1_000).is_err());
    assert!(socket_issuer()?.verify_socket_at(&token, now).is_err());
    let host = issuer.issue_host(
        &HostId::new("host.v3.r1.one"),
        &uuid::Uuid::new_v4().to_string(),
        "r1",
        "us-east",
        &ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
    )?;
    assert!(issuer.verify_socket(&host.token).is_err());
    Ok(())
}

fn socket_issuer() -> Result<ActorJwtIssuer> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(86_400),
    )
}

#[test]
fn host_tokens_round_trip_with_a_bounded_lifetime() -> Result<()> {
    let issuer = socket_issuer()?;
    let before = unix_millis()?;
    let host = HostId::new("host.v3.r1.one");
    let issued = issuer.issue_host(
        &host,
        &uuid::Uuid::new_v4().to_string(),
        "r1",
        "us-east",
        &ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
    )?;
    let verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(1800),
    )?;
    let principal = verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;
    assert_eq!(principal.host_id, host);
    assert_eq!(principal.region, "us-east");
    assert!(issued.expires_at_ms <= before + 1_800_000);
    Ok(())
}

#[test]
fn direct_invocation_tokens_are_bound_to_one_actor_target_without_host_authority() -> Result<()> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    let issuer = ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "test-key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(60),
    )?;
    let actor = crate::actor::ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let host_id = HostId::new("host.v3.revision-1.host-1");
    let issued = issuer.issue_invocation_target(
        &actor,
        &host_id,
        "00000000-0000-4000-8000-000000000001",
        "revision-1",
        "north-america-east",
        3,
        None,
        "http://10.1.2.3:7101",
    )?;
    let invocation_verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let principal =
        invocation_verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;

    assert_eq!(principal.host_id, host_id);
    assert_eq!(
        principal.invocation.as_ref().unwrap().route,
        "http://10.1.2.3:7101/"
    );
    assert_eq!(
        principal.invocation.expect("invocation capability").actor,
        actor
    );
    assert!(issued.expires_at_ms <= (unix_millis()? + 60_000));

    let authority_verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    assert!(
        authority_verifier
            .authenticate_authorization(&format!("Bearer {}", issued.token))
            .is_err()
    );
    Ok(())
}

#[test]
fn delegated_tickets_have_restricted_scope_and_cannot_outlive_authorization() -> Result<()> {
    let issuer = socket_issuer()?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let now = unix_millis()? / 1000;
    let grant = super::super::session::InvocationGrant {
        subject: "credential-fingerprint".into(),
        grant_id: "grant-id".into(),
        expires_at: now + 20,
        methods: vec!["increment".into()],
    };
    let issue = |grant| {
        issuer.issue_invocation_target(
            &actor,
            &HostId::new("host.v3.revision.one"),
            "00000000-0000-4000-8000-000000000001",
            "revision",
            "us-west",
            1,
            Some(grant),
            "http://10.1.2.3:7101",
        )
    };
    let issued = issue(grant.clone())?;
    assert_eq!(issued.expires_at_ms, grant.expires_at * 1000);
    let payload: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(issued.token.split('.').nth(1).unwrap())?)?;
    assert_eq!(payload["scope"], "actor:delegated-invoke");
    let verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let principal = verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;
    assert_eq!(principal.invocation.unwrap().grant, Some(grant.clone()));
    assert!(
        issue(super::super::session::InvocationGrant {
            expires_at: now,
            ..grant
        })
        .is_err()
    );
    Ok(())
}

#[test]
fn sessions_are_runtime_signed_and_never_extend_the_authorization_deadline() -> Result<()> {
    let issuer = socket_issuer()?;
    let now = unix_millis()?;
    for deadline in [now + 25_000, now + 300_000, now + 600_000] {
        let issued = issuer.issue_session("project".into(), "credential".into(), deadline)?;
        assert!(issued.expires_at_ms <= deadline.min(unix_millis()? + 300_000));
        assert!(issued.expires_at_ms >= deadline.min(now + 300_000) - 1000);
        let authorization = format!("Bearer {}", issued.token);
        let verifier = issuer.session_verifier()?;
        let session = verifier.authenticate(&authorization, "project")?;
        assert_eq!(session.subject, "credential");
        assert_eq!(session.expires_at * 1000, issued.expires_at_ms);
        assert_eq!(session.project_id, "project");
        assert!(verifier.authenticate(&authorization, "other").is_err());
        assert!(
            socket_issuer()?
                .session_verifier()?
                .authenticate(&authorization, "project")
                .is_err()
        );
        for (audience, purpose) in [
            ("authority", ActorTokenPurpose::ControlPlane),
            ("invocation", ActorTokenPurpose::Invocation),
        ] {
            let host_verifier = ActorJwtVerifier::for_scope(
                issuer.verifier_keys_json()?,
                "issuer",
                audience,
                purpose,
                Duration::from_secs(60),
            )?;
            assert!(
                host_verifier
                    .authenticate_authorization(&authorization)
                    .is_err()
            );
        }
    }
    assert!(
        issuer
            .issue_session("project".into(), "credential".into(), now)
            .is_err()
    );
    Ok(())
}

#[test]
fn sessions_respect_a_shorter_configured_jwt_lifetime() -> Result<()> {
    let mut issuer = socket_issuer()?;
    issuer.max_lifetime = Duration::from_secs(30);
    let now = unix_millis()?;
    let issued = issuer.issue_session("project".into(), "credential".into(), now + 300_000)?;
    assert!(issued.expires_at_ms <= unix_millis()? + 30_000);
    assert!(issued.expires_at_ms >= now + 29_000);
    issuer
        .session_verifier()?
        .authenticate(&format!("Bearer {}", issued.token), "project")?;
    Ok(())
}

#[test]
fn session_verification_rejects_invalid_claims_and_tampering() -> Result<()> {
    let issuer = socket_issuer()?;
    let issued = issuer.issue_session(
        "project".into(),
        "credential".into(),
        unix_millis()? + 60_000,
    )?;
    let verifier = issuer.session_verifier()?;
    let session = verifier.authenticate(&format!("Bearer {}", issued.token), "project")?;
    let original = serde_json::to_value(&session)?;
    for (field, value) in [
        ("iss", serde_json::json!("other")),
        ("aud", serde_json::json!("invocation")),
        ("scope", serde_json::json!("actor:invoke")),
        ("sub", serde_json::json!("")),
        ("jti", serde_json::json!("")),
        ("nbf", serde_json::json!(session.iat + 60)),
        ("iat", serde_json::json!(session.iat + 60)),
        ("exp", serde_json::json!(session.iat)),
        ("exp", serde_json::json!(session.iat + 301)),
        ("projectId", serde_json::json!("")),
        ("projectId", serde_json::json!("../other")),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut claims = original.clone();
        claims[field] = value;
        assert!(
            verifier
                .authenticate(&format!("Bearer {}", issuer.sign(&claims)?), "project")
                .is_err(),
            "{field}"
        );
    }
    let mut parts: Vec<String> = issued.token.split('.').map(str::to_owned).collect();
    let mut altered = original;
    altered["projectId"] = serde_json::json!("other");
    parts[1] = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&altered)?);
    assert!(
        verifier
            .authenticate(&format!("Bearer {}", parts.join(".")), "other")
            .is_err()
    );
    Ok(())
}

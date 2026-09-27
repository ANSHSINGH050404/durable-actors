use super::*;
use crate::{
    actor::ActorKey,
    control_plane::{ActorInvocationCapability, ActorPrincipal},
};

#[test]
fn direct_capability_is_bound_to_the_actor_host_session_and_epoch() {
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let host_id = HostId::new("host.v3.revision-1.host-1");
    let principal = ActorPrincipal {
        actor: actor.clone(),
        host_id: host_id.clone(),
        session_id: "00000000-0000-4000-8000-000000000001".into(),
        region: "north-america-east".into(),
        host_config_key: Some("revision-1".into()),
        invocation: Some(ActorInvocationCapability {
            actor: actor.clone(),
            host_id: host_id.clone(),
            owner_epoch: 3,
            grant: None,
        }),
    };

    assert!(validate_host_request(&principal, &host_id, "another-session", &actor, 3).is_err());
    assert!(validate_host_request(&principal, &host_id, &principal.session_id, &actor, 3).is_ok());
    assert!(
        validate_host_request(
            &principal,
            &host_id,
            &principal.session_id,
            &ActorKey {
                project_id: "another-project".into(),
                ..actor.clone()
            },
            3
        )
        .is_err()
    );
    assert!(validate_host_request(&principal, &host_id, &principal.session_id, &actor, 4).is_err());
    assert!(
        validate_host_request(
            &principal,
            &host_id,
            &principal.session_id,
            &ActorKey {
                actor_id: "other".into(),
                ..actor
            },
            3
        )
        .is_err()
    );
}

#[test]
fn delegated_tickets_only_allow_published_rpc_methods() {
    let grant = crate::control_plane::session::InvocationGrant {
        subject: "credential-1".into(),
        grant_id: "grant-1".into(),
        expires_at: i64::MAX,
        methods: vec!["increment".into()],
    };
    assert!(authorize_grant(Some(&grant), Some("increment")).is_ok());
    assert!(authorize_grant(Some(&grant), Some("privateMethod")).is_err());
    assert!(authorize_grant(Some(&grant), Some("onConnect")).is_err());
    assert!(authorize_grant(Some(&grant), None).is_err());
    assert!(authorize_grant(None, None).is_ok());
}
